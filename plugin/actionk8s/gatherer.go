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
	meta          = make(metaData)
	client        *kubernetes.Clientset
	maintenanceMu sync.Mutex

	expiredItems = make([]*metaItem, 0, 16)

	gathererStop    = make(chan bool, 1)
	maintenanceStop = make(chan bool, 1)

	maintenanceInterval = time.Second * 15
	metaExpireDuration  = time.Minute * 5

	metaRecheckInterval = time.Millisecond * 250
	metaWaitTimeout     = time.Second * 5

	stopWg = &sync.WaitGroup{}

	// some debugging shit
	disableWatching     = false
	checkerLogsCounter  = make(map[podName]int)
	watchEventsCounter  atomic.Int64
	expiredItemsCounter atomic.Int64
)

func enableGatherer() {
	logger.Info("enabling k8s meta gatherer")

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

func getWatcher() watch.Interface {
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
			time.Sleep(maintenanceInterval)

			cleanUp()
		}
	}
}

func cleanUp() {
	maintenanceMu.Lock()
	defer maintenanceMu.Unlock()

	expiredItems = getExpiredItems(expiredItems[:0])
	cleanUpItems(expiredItems)

	notFull := 0
	for _, c := range checkerLogsCounter {
		if c != 20001 {
			notFull++
		}
	}
	logger.Infof("checker logs total=%d, not full=%d", len(checkerLogsCounter), notFull)
}

func getExpiredItems(out []*metaItem) []*metaItem {
	now := time.Now()
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
		maintenanceMu.Lock()
		if meta[ns][pod][cid] != nil {
			metaInfo := meta[ns][pod][cid]
			maintenanceMu.Unlock()
			return metaInfo
		}
		maintenanceMu.Unlock()

		time.Sleep(metaRecheckInterval)
		i += metaRecheckInterval
		if i-metaWaitTimeout >= 0 {
			logger.Errorf("meta retrieve timeout for ns=%s pod=%s container=%s", string(ns), string(pod), string(container))
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

	maintenanceMu.Lock()
	defer maintenanceMu.Unlock()

	if meta[ns] == nil {
		meta[ns] = make(map[podName]map[containerID]*corev1.Pod)
	}

	if meta[ns][pod] == nil {
		meta[ns][pod] = make(map[containerID]*corev1.Pod)
	}

	for _, containerStatus := range podMeta.Status.ContainerStatuses {
		if len(containerStatus.ContainerID) == 0 {
			continue
		}

		if len(containerStatus.ContainerID) < 9 || containerStatus.ContainerID[:9] != "docker://" {
			logger.Fatalf("wrong container id: %s", containerStatus.ContainerID)
		}

		containerID := containerID(containerStatus.ContainerID[9:])
		if len(containerID) != 64 {
			logger.Fatalf("wrong container id: %s", containerStatus.ContainerID)
		}

		// hack to avoid creation of special struct to store time
		// store it in generation field of k8d pod
		podMeta.Generation = time.Now().UnixNano()
		meta[ns][pod][containerID] = podMeta

	}

	watchEventsCounter.Inc()
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
