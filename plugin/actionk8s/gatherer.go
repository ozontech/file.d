package actionk8s

import (
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

type metaData map[namespace]map[podName]map[containerID]*corev1.Pod

type nodeName string
type podName string
type namespace string
type containerName string
type containerID string

var (
	meta   = make(metaData)
	client *kubernetes.Clientset
	metaMu sync.Mutex

	expiredItems = make([]*metaItem, 0, 16)

	gathererStop    = make(chan bool, 1)
	maintenanceStop = make(chan bool, 1)

	maintenanceInterval = time.Second * 15
	metaExpireDuration  = time.Minute * 5

	metaRecheckInterval = time.Millisecond * 250
	metaWaitTimeout     = time.Second * 5

	stopWg = &sync.WaitGroup{}

	// some debugging shit
	disableWatching          = false
	checkerLogs                  = make(map[podName]int)
	metaAddedCounter    atomic.Int64
	expiredItemsCounter atomic.Int64
)

func enableGatherer() {
	logger.Info("enabling k8s meta gatherer")

	createClient()

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

func gather() {
	watcher := getWatcher()
	for {
		select {
		case <-gathererStop:
			stopWg.Done()
			return
		case event := <-watcher.ResultChan():

			if event.Type == watch.Error {
				logger.Warnf("watch error: %s", event.Object)
				continue
			}

			if event.Object == nil {
				continue
			}

			putMeta(event.Object.(*corev1.Pod))
		}
	}
}

func createClient() {
	apiConfig, err := rest.InClusterConfig()
	if err != nil {
		kubeConfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
		apiConfig, err = clientcmd.BuildConfigFromFlags("", kubeConfig)
		if err != nil {
			logger.Fatalf("can't get k8s config: %s", err.Error())
		}
	}
	client, err = kubernetes.NewForConfig(apiConfig)
	if err != nil {
		logger.Fatalf("can't create k8s client: %s", err.Error())
		panic("")
	}
}

func getWatcher() watch.Interface {
	watcher, err := client.CoreV1().Pods("").Watch(metav1.ListOptions{})
	if err != nil {
		logger.Fatalf("can't watch k8s pod events: %s", err.Error())
		panic("")
	}

	if disableWatching {
		watcher.Stop()
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

func refresh() {
	podList, err := client.CoreV1().Pods("").List(metav1.ListOptions{})
	if err != nil {
		logger.Errorf("can't get pod list from k8s")
		return
	}

	// update time for pods which is in the k8s pod list
	for _, podMeta := range podList.Items {
		putMeta(&podMeta)
	}

	expiredItems = getExpiredItems(expiredItems)
	cleanUpItems(expiredItems)

	notFull := 0
	for _, c := range checkerLogs {
		if c != 20001 {
			notFull++
		}
	}

	logger.Infof("k8s meta stat for last %d seconds: updated=%d, expired=%d", maintenanceInterval/time.Second, metaAddedCounter.Load(), expiredItemsCounter.Load())
	logger.Infof("checker logs total=%d, not full=%d", len(checkerLogs), notFull)

	metaAddedCounter.Swap(0)
	expiredItemsCounter.Swap(0)
}

func getExpiredItems(out []*metaItem) []*metaItem {
	out = out[:0]
	now := time.Now()

	metaMu.Lock()
	defer metaMu.Unlock()

	// find pods which isn't in k8s pod list for some time and add them to the expiration list
	for ns, podNames := range meta {
		for pod, containerIDs := range podNames {
			for cid, podData := range containerIDs {
				if now.Sub(time.Unix(0, podData.Generation)) > metaExpireDuration {
					out = append(out, &metaItem{
						namespace:   ns,
						podName:     pod,
						containerID: cid,
					})
				}

				// don't need to check all containers, just first
				break
			}
		}
	}

	return out
}

func cleanUpItems(items []*metaItem) {
	for _, i := range items {
		expiredItemsCounter.Inc()
		delete(meta[i.namespace][i.podName], i.containerID)
		if len(meta[i.namespace][i.podName]) == 0 {
			delete(meta[i.namespace], i.podName)
		}
		if len(meta[i.namespace]) == 0 {
			delete(meta, i.namespace)
		}
	}
}

func getMeta(ns namespace, pod podName, cid containerID, container containerName) *corev1.Pod {
	i := time.Nanosecond
	for {
		metaMu.Lock()
		metaInfo := meta[ns][pod][cid]
		metaMu.Unlock()

		if metaInfo != nil {
			return metaInfo
		}

		time.Sleep(metaRecheckInterval)
		i += metaRecheckInterval
		if i-metaWaitTimeout >= 0 {
			logger.Errorf("meta retrieve timeout for ns=%s pod=%s container=%s cid=%s", string(ns), string(pod), string(container), string(cid))
			return nil
		}
	}
}

func putMeta(podMeta *corev1.Pod) {
	if len(podMeta.Status.ContainerStatuses) == 0 {
		return
	}

	pod := podName(podMeta.Name)
	ns := namespace(podMeta.Namespace)

	metaMu.Lock()
	defer metaMu.Unlock()

	if meta[ns] == nil {
		meta[ns] = make(map[podName]map[containerID]*corev1.Pod)
	}

	if meta[ns][pod] == nil {
		meta[ns][pod] = make(map[containerID]*corev1.Pod)
	}

	// normal containers
	for _, status := range podMeta.Status.ContainerStatuses {
		putContainerMeta(ns, pod, status.ContainerID, status.Name, podMeta)

		if status.LastTerminationState.Terminated != nil {
			putContainerMeta(ns, pod, status.LastTerminationState.Terminated.ContainerID, status.Name, podMeta)
		}
	}

	// init containers
	for _, status := range podMeta.Status.InitContainerStatuses {
		putContainerMeta(ns, pod, status.ContainerID, status.Name, podMeta)

		if status.LastTerminationState.Terminated != nil {
			putContainerMeta(ns, pod, status.LastTerminationState.Terminated.ContainerID, status.Name, podMeta)
		}
	}

	metaAddedCounter.Inc()
}

func putContainerMeta(ns namespace, pod podName, fullContainerID string, containerName string, podMeta *corev1.Pod) {
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

	// hack to avoid creation of special struct to store time
	// store it in generation field of k8d pod
	podMeta.Generation = time.Now().UnixNano()
	meta[ns][pod][containerID] = podMeta

	//logger.Infof("k8s meta added ns=%s pod=%s container=%s cid=%s", ns, pod, containerName, containerID)
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
