package k8s

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

type (
	podName       string
	namespace     string
	containerName string
	containerID   string

	podMeta struct {
		*corev1.Pod
		updateTime time.Time
	}

	metaItem struct {
		namespace     namespace
		podName       podName
		containerName containerName
		containerID   containerID
	}

	meta map[namespace]map[podName]map[containerID]*podMeta
)

var (
	client       *kubernetes.Clientset
	metaData     = make(meta)
	metaDataMu   = &sync.RWMutex{}
	podBlackList = make(map[podName]bool) // to mark pods for which we are miss k8s meta and don't wanna wait for timeout for each event
	controller   cache.Controller

	expiredItems = make([]*metaItem, 0, 16) // temporary list of expired items

	informerStop    = make(chan struct{}, 1)
	maintenanceStop = make(chan struct{}, 1)
	stopped         = make(chan struct{}, 1)

	MaintenanceInterval = 15 * time.Second
	metaExpireDuration  = 5 * time.Minute

	metaRecheckInterval = 250 * time.Millisecond
	metaWaitWarn        = 5 * time.Second
	MetaWaitTimeout     = 60 * time.Second

	stopWg = &sync.WaitGroup{}

	// some debugging shit
	DisableMetaUpdates  = false
	metaAddedCounter    atomic.Int64
	expiredItemsCounter atomic.Int64

	criType    = "docker"
	nodeLabels = make(map[string]string)

	selfNodeName string

	localLogger *zap.SugaredLogger
)

func enableGatherer(l *zap.SugaredLogger) {
	localLogger = l
	localLogger.Info("enabling k8s meta gatherer")

	if !DisableMetaUpdates {
		initGatherer()

		go controller.Run(informerStop)
	}

	go maintenance()
}

func disableGatherer() {
	localLogger.Info("disabling k8s meta gatherer")
	if !DisableMetaUpdates {
		informerStop <- struct{}{}
	}
	maintenanceStop <- struct{}{}
	<-stopped
	stopWg.Wait()
}

func initGatherer() {
	apiConfig, err := rest.InClusterConfig()
	if err != nil {
		kubeConfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
		apiConfig, err = clientcmd.BuildConfigFromFlags("", kubeConfig)
		if err != nil {
			localLogger.Fatalf("can't get k8s client config: %s", err.Error())
		}
	}

	client, err = kubernetes.NewForConfig(apiConfig)
	if err != nil {
		localLogger.Fatalf("can't create k8s client: %s", err.Error())
		panic("")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	initNodeInfo(ctx)
	initInformer()
	initRuntime(ctx)
}

func initNodeInfo(ctx context.Context) {
	podName, err := os.Hostname()
	if err != nil {
		localLogger.Fatalf("can't get host name for k8s plugin: %s", err.Error())
		panic("")
	}
	pod, err := client.CoreV1().Pods(getNamespace()).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		localLogger.Fatalf("can't detect node name for k8s plugin using pod %q: %s", podName, err.Error())
		panic("")
	}
	selfNodeName = pod.Spec.NodeName
}

func initInformer() {
	selector, err := fields.ParseSelector("spec.nodeName=" + selfNodeName)
	if err != nil {
		localLogger.Fatalf("can't create k8s field selector: %s", err.Error())
	}
	podListWatcher := cache.NewListWatchFromClient(client.CoreV1().RESTClient(), "pods", "", selector)
	_, c := cache.NewIndexerInformer(podListWatcher, &corev1.Pod{}, metaExpireDuration/4, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			putMeta(obj.(*corev1.Pod))
		},
		UpdateFunc: func(old any, obj any) {
			putMeta(obj.(*corev1.Pod))
		},
		DeleteFunc: func(obj any) {
		},
	}, cache.Indexers{})
	controller = c
}

func initRuntime(ctx context.Context) {
	node, err := client.CoreV1().Nodes().Get(ctx, selfNodeName, metav1.GetOptions{})
	if err != nil || node == nil {
		localLogger.Fatalf("can't detect CRI runtime for node %s, api call is unsuccessful: %s", node, err.Error())
		panic("_")
	}
	runtimeVer := node.Status.NodeInfo.ContainerRuntimeVersion
	pos := strings.IndexByte(runtimeVer, ':')
	if pos < 0 {
		localLogger.Fatalf("can't detect CRI runtime for node %s, wrong runtime version: %s", node, runtimeVer)
	}

	nodeLabels = node.Labels
	criType = runtimeVer[:pos]
}

func removeExpired() {
	expiredItems = getExpiredItems(expiredItems)
	cleanUpItems(expiredItems)

	if MaintenanceInterval > time.Second {
		localLogger.Infof("k8s meta stat for last %d seconds: total=%d, updated=%d, expired=%d", MaintenanceInterval/time.Second, getTotalItems(), metaAddedCounter.Load(), expiredItemsCounter.Load())
	}

	metaAddedCounter.Swap(0)
	expiredItemsCounter.Swap(0)
}

func maintenance() {
	stopWg.Add(1)
	for {
		select {
		case <-maintenanceStop:
			stopped <- struct{}{}
			stopWg.Done()
			return
		default:
			time.Sleep(MaintenanceInterval)
			removeExpired()
		}
	}
}

func getTotalItems() int {
	metaDataMu.RLock()
	defer metaDataMu.RUnlock()

	totalItems := 0
	for _, podNames := range metaData {
		for _, containerIDs := range podNames {
			totalItems += len(containerIDs)
		}
	}
	return totalItems
}

func getExpiredItems(out []*metaItem) []*metaItem {
	out = out[:0]
	now := time.Now()

	metaDataMu.RLock()
	defer metaDataMu.RUnlock()

	// find pods which aren't in k8s pod list for some time and add them to the expiration list
	for ns, podNames := range metaData {
		for pod, containerIDs := range podNames {
			for cid, podData := range containerIDs {
				if now.Sub(podData.updateTime) > metaExpireDuration {
					out = append(out, &metaItem{
						namespace:   ns,
						podName:     pod,
						containerID: cid,
					})
				}
			}
		}
	}

	return out
}

func cleanUpItems(items []*metaItem) {
	metaDataMu.Lock()
	defer metaDataMu.Unlock()

	for _, item := range items {
		expiredItemsCounter.Inc()
		delete(metaData[item.namespace][item.podName], item.containerID)

		if len(metaData[item.namespace][item.podName]) == 0 {
			delete(metaData[item.namespace], item.podName)
		}

		if len(metaData[item.namespace]) == 0 {
			delete(metaData, item.namespace)
		}
	}
}

func getPodMeta(ns namespace, pod podName, cid containerID) (bool, *podMeta) {
	var podMeta *podMeta
	var success bool

	i := time.Nanosecond
	for {
		metaDataMu.RLock()
		pm, has := metaData[ns][pod][cid]
		isInBlackList := podBlackList[pod]
		metaDataMu.RUnlock()

		if has {
			if i-metaWaitWarn >= 0 {
				localLogger.Warnf("meta retrieved with delay time=%dms pod=%s container=%s", i/time.Millisecond, string(pod), string(cid))
			}

			success = true
			podMeta = pm
			return success, podMeta
		}

		// fast skip blacklisted pods
		if isInBlackList {
			return success, podMeta
		}

		time.Sleep(metaRecheckInterval)
		i += metaRecheckInterval

		if i-MetaWaitTimeout >= 0 {
			metaDataMu.Lock()
			if len(podBlackList) > 32 {
				podBlackList = make(map[podName]bool)
			}
			podBlackList[pod] = true
			metaDataMu.Unlock()
			localLogger.Errorf("pod %q have blacklisted, cause k8s meta retrieve timeout ns=%s", string(pod), string(ns))

			return success, podMeta
		}
	}
}

func putMeta(podData *corev1.Pod) {
	if len(podData.Status.ContainerStatuses) == 0 {
		return
	}

	podCopy := podData

	pod := podName(podCopy.Name)
	ns := namespace(podCopy.Namespace)

	metaDataMu.Lock()
	if metaData[ns] == nil {
		metaData[ns] = make(map[podName]map[containerID]*podMeta)
	}
	if metaData[ns][pod] == nil {
		metaData[ns][pod] = make(map[containerID]*podMeta)
	}
	metaDataMu.Unlock()

	// normal containers
	for i := range podCopy.Status.ContainerStatuses {
		status := &podCopy.Status.ContainerStatuses[i]
		putContainerMeta(ns, pod, status.ContainerID, podCopy)

		if status.LastTerminationState.Terminated != nil {
			putContainerMeta(ns, pod, status.LastTerminationState.Terminated.ContainerID, podCopy)
		}
	}

	// init containers
	for i := range podCopy.Status.InitContainerStatuses {
		status := &podCopy.Status.InitContainerStatuses[i]
		putContainerMeta(ns, pod, status.ContainerID, podCopy)

		if status.LastTerminationState.Terminated != nil {
			putContainerMeta(ns, pod, status.LastTerminationState.Terminated.ContainerID, podCopy)
		}
	}

	metaAddedCounter.Inc()
}

// putContainerMeta fullContainerID must be in format XXX://ID, eg docker://4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0
func putContainerMeta(ns namespace, pod podName, fullContainerID string, podInfo *corev1.Pod) {
	l := len(fullContainerID)
	if l == 0 {
		return
	}

	pos := strings.IndexByte(fullContainerID, ':')
	if pos <= 0 {
		localLogger.Fatalf("container id should have format XXXX://ID: %s", fullContainerID)
	}

	if pos+3 >= l {
		localLogger.Fatalf("container id should have format XXXX://ID: %s", fullContainerID)
	}

	if fullContainerID[pos:pos+3] != "://" {
		localLogger.Fatalf("container id should have format XXXX://ID: %s", fullContainerID)
	}

	containerID := containerID(fullContainerID[pos+3:])
	if len(containerID) != 64 {
		localLogger.Fatalf("wrong container id: %s", fullContainerID)
	}

	meta := &podMeta{
		updateTime: time.Now(),
		Pod:        podInfo,
	}

	metaDataMu.Lock()
	metaData[ns][pod][containerID] = meta
	metaDataMu.Unlock()
}

func getNamespace() string {
	data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}
