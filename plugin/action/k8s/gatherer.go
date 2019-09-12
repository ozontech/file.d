package k8s

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	formatInfo = "make sure log has name in format [pod-name]_[namespace]_[container-name]-[container id].log"
)

type metaItem struct {
	nodeName      nodeName
	namespace     namespace
	podName       podName
	containerName containerName
	containerID   containerID
}

type meta map[namespace]map[podName]map[containerID]*corev1.Pod

type nodeName string
type podName string
type namespace string
type containerName string
type containerID string

var (
	client       *kubernetes.Clientset
	metaData     = make(meta)
	metaDataMu   = &sync.Mutex{}
	podListOps   metav1.ListOptions       // to select only pods for node on which we are running
	podBlackList = make(map[podName]bool) // to mark pods for which we are miss k8s meta and don't wanna wait for timeout for each event

	expiredItems = make([]*metaItem, 0, 16) // temporary list of expired items

	gathererStop    = make(chan bool, 1)
	maintenanceStop = make(chan bool, 1)

	maintenanceInterval = 15 * time.Second
	metaExpireDuration  = 5 * time.Minute

	metaRecheckInterval = 250 * time.Millisecond
	metaWaitWarn        = 5 * time.Second
	metaWaitTimeout     = 60 * time.Second

	stopWg = &sync.WaitGroup{}

	// some debugging shit
	disableMetaUpdates  = false
	metaAddedCounter    atomic.Int64
	expiredItemsCounter atomic.Int64

)

func enableGatherer() {
	logger.Info("enabling k8s meta gatherer")

	initGatherer()

	stopWg.Add(1)
	go gather()

	stopWg.Add(1)
	go maintenance()
}

func disableGatherer() {
	logger.Info("disabling k8s meta gatherer")
	gathererStop <- true
	maintenanceStop <- true
	stopWg.Wait()
}

func initGatherer() {
	apiConfig, err := rest.InClusterConfig()
	if err != nil {
		kubeConfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
		apiConfig, err = clientcmd.BuildConfigFromFlags("", kubeConfig)
		if err != nil {
			logger.Fatalf("can't get k8s client config: %s", err.Error())
		}
	}
	client, err = kubernetes.NewForConfig(apiConfig)
	if err != nil {
		logger.Fatalf("can't create k8s client: %s", err.Error())
		panic("")
	}

	podListOps.IncludeUninitialized = true

	if !disableMetaUpdates {
		podName, err := os.Hostname()
		if err != nil {
			logger.Fatalf("can't get host name for k8s plugin: %s", err.Error())
			panic("")
		}

		pod, err := client.CoreV1().Pods(getNamespace()).Get(podName, metav1.GetOptions{})
		if err != nil {
			logger.Fatalf("can't detect node name for k8s plugin using pod %q: %s", podName, err.Error())
			panic("")
		}

		podListOps.FieldSelector = "spec.nodeName=" + pod.Spec.NodeName
	}
}

func gather() {
	if disableMetaUpdates {
		<-gathererStop
		stopWg.Done()
		return
	}

	watcher := getWatcher()
	for {
		select {
		case <-gathererStop:
			watcher.Stop()
			stopWg.Done()
			return
		case event, ok := <-watcher.ResultChan():
			// watcher stopped by api server, recreate it
			if !ok {
				logger.Infof("k8s watcher will be recreated")
				watcher.Stop()
				watcher = getWatcher()
			}

			if event.Type == watch.Error {
				logger.Errorf("k8s watch error: %s", event.Object)
				continue
			}

			if event.Object == nil {
				continue
			}

			putMeta(event.Object.(*corev1.Pod))
		}
	}
}

func refresh() {
	if !disableMetaUpdates {
		podList, err := client.CoreV1().Pods("").List(podListOps)
		if err != nil {
			logger.Errorf("can't get pod list from k8s")
			return
		}

		// update time for pods which is in the k8s pod list
		for _, podMeta := range podList.Items {
			putMeta(&podMeta)
		}
	}

	expiredItems = getExpiredItems(expiredItems)
	cleanUpItems(expiredItems)

	logger.Infof("k8s meta stat for last %d seconds: total=%d, updated=%d, expired=%d", maintenanceInterval/time.Second, getTotalItems(), metaAddedCounter.Load(), expiredItemsCounter.Load())

	metaAddedCounter.Swap(0)
	expiredItemsCounter.Swap(0)
}

func getWatcher() watch.Interface {
	watcher, err := client.CoreV1().Pods("").Watch(podListOps)
	if err != nil {
		logger.Fatalf("can't watch k8s pod events: %s", err.Error())
		panic("")
	}

	return watcher
}

func maintenance() {
	for {
		select {
		case <-maintenanceStop:
			stopWg.Done()
			return
		default:
			// retrieve all pods info first, then sleep
			refresh()
			time.Sleep(maintenanceInterval)
		}
	}
}

func getTotalItems() int {
	metaDataMu.Lock()
	defer metaDataMu.Unlock()

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

	metaDataMu.Lock()
	defer metaDataMu.Unlock()

	// find pods which isn't in k8s pod list for some time and add them to the expiration list
	for ns, podNames := range metaData {
		for pod, containerIDs := range podNames {
			for cid, podData := range containerIDs {
				podUpdateTime := time.Unix(0, podData.Generation)
				if now.Sub(podUpdateTime) > metaExpireDuration {
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

	for _, i := range items {
		expiredItemsCounter.Inc()
		delete(metaData[i.namespace][i.podName], i.containerID)

		if len(metaData[i.namespace][i.podName]) == 0 {
			delete(metaData[i.namespace], i.podName)
		}

		if len(metaData[i.namespace]) == 0 {
			delete(metaData, i.namespace)
		}
	}
}

func getMeta(fullFilename string) (ns namespace, pod podName, container containerName, cid containerID, success bool, podMeta *corev1.Pod) {
	podMeta = nil
	success = false
	ns, pod, container, cid = parseDockerFilename(fullFilename)

	i := time.Nanosecond
	for {
		metaDataMu.Lock()
		pm, has := metaData[ns][pod][cid]
		isInBlackList := podBlackList[pod]
		metaDataMu.Unlock()

		// fast skip blacklisted pods
		if isInBlackList {
			return
		}

		if has {
			if i-metaWaitWarn >= 0 {
				logger.Warnf("meta retrieved with delay time=%dms pod=%s container=%s", i/time.Millisecond, string(pod), string(cid))
			}

			success = true
			podMeta = pm
			return
		}

		time.Sleep(metaRecheckInterval)
		i += metaRecheckInterval

		if i-metaWaitTimeout >= 0 {
			metaDataMu.Lock()
			if len(podBlackList) > 32 {
				podBlackList = make(map[podName]bool)
			}
			podBlackList[pod] = true
			metaDataMu.Unlock()
			logger.Errorf("pod %q have blacklisted, cause k8s meta retrieve timeout ns=%s container=%s cid=%s", string(pod), string(ns), string(container), string(cid))

			return
		}
	}
}

func putMeta(podMeta *corev1.Pod) {
	if len(podMeta.Status.ContainerStatuses) == 0 {
		return
	}

	pod := podName(podMeta.Name)
	ns := namespace(podMeta.Namespace)
	// use generation field of k8d pod as hack to avoid creation of special struct to store time
	podMeta.Generation = time.Now().UnixNano()

	metaDataMu.Lock()
	if metaData[ns] == nil {
		metaData[ns] = make(map[podName]map[containerID]*corev1.Pod)
	}
	if metaData[ns][pod] == nil {
		metaData[ns][pod] = make(map[containerID]*corev1.Pod)
	}
	metaDataMu.Unlock()

	// normal containers
	for _, status := range podMeta.Status.ContainerStatuses {
		putContainerMeta(ns, pod, status.ContainerID, podMeta)

		if status.LastTerminationState.Terminated != nil {
			putContainerMeta(ns, pod, status.LastTerminationState.Terminated.ContainerID, podMeta)
		}
	}

	// init containers
	for _, status := range podMeta.Status.InitContainerStatuses {
		putContainerMeta(ns, pod, status.ContainerID, podMeta)

		if status.LastTerminationState.Terminated != nil {
			putContainerMeta(ns, pod, status.LastTerminationState.Terminated.ContainerID, podMeta)
		}
	}

	metaAddedCounter.Inc()
}

func putContainerMeta(ns namespace, pod podName, fullContainerID string, podMeta *corev1.Pod) {
	if len(fullContainerID) == 0 {
		return
	}

	if len(fullContainerID) < 9 || fullContainerID[:9] != "docker://" {
		logger.Fatalf("wrong container id: %s", fullContainerID)
	}

	containerID := containerID(fullContainerID[9:])
	if len(containerID) != 64 {
		logger.Fatalf("wrong container id: %s", fullContainerID)
	}

	metaDataMu.Lock()
	metaData[ns][pod][containerID] = podMeta
	metaDataMu.Unlock()
}

func parseDockerFilename(fullFilename string) (namespace, podName, containerName, containerID) {
	if fullFilename[len(fullFilename)-4:] != ".log" {
		logger.Infof(formatInfo)
		logger.Fatalf("wrong docker log file name, no .log at ending %s", fullFilename)
	}
	lastSlash := strings.LastIndexByte(fullFilename, '/')
	if lastSlash < 0 {
		logger.Infof(formatInfo)
		logger.Fatalf("wrong docker log file name %s, no slashes", fullFilename)
	}
	filename := fullFilename[lastSlash+1 : len(fullFilename)-4]
	if filename == "" {
		logger.Infof(formatInfo)
		logger.Fatalf("wrong docker log file name, empty", filename)
	}

	underscore := strings.IndexByte(filename, '_')
	if underscore < 0 {
		logger.Infof(formatInfo)
		logger.Fatalf("wrong docker log file name, no underscore for pod: %s", filename)
	}

	pod := filename[:underscore]
	filename = filename[underscore+1:]

	underscore = strings.IndexByte(filename, '_')
	if underscore < 0 {
		logger.Infof(formatInfo)
		logger.Fatalf("wrong docker log file name, no underscore for ns: %s", filename)
	}
	ns := filename[:underscore]
	filename = filename[underscore+1:]

	if len(filename) < 65 {
		logger.Infof(formatInfo)
		logger.Fatalf("wrong docker log file name, not enough chars: %s", filename)
	}

	container := filename[:len(filename)-65]
	cid := filename[len(filename)-64:]

	return namespace(ns), podName(pod), containerName(container), containerID(cid)
}

func getNamespace() string {
	data, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}
