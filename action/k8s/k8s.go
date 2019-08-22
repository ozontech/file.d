package k8s

import (
	"os"
	"path/filepath"
	"sync"

	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"go.uber.org/atomic"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	info         = make(podInfo)
	client       *kubernetes.Clientset
	updateMu     sync.Mutex
	updateEvents atomic.Int64
	startFlag    atomic.Int32
)

type Config struct {
}

type K8SPlugin struct {
	config *Config
}

// pods by namespace, pod name, container id
type podInfo map[string]map[string]map[string]*corev1.Pod

func init() {
	filed.DefaultPluginRegistry.RegisterAction(&pipeline.PluginInfo{
		Type:    "k8s",
		Factory: factory,
	})

}

func factory() (pipeline.Plugin, pipeline.Config) {
	return &K8SPlugin{}, &Config{}
}

func (p *K8SPlugin) Start(config pipeline.Config, controller pipeline.Controller) {
	flag := startFlag.Inc()

	if flag == 1 {
		logger.Info("starting gatherer of k8s action plugin")
		start()
	} else {
		logger.Info("gatherer of k8s is already started, it should be only one instance")
	}
}

func (p *K8SPlugin) Stop() {

}

// source docker log file name format: [pod-name]_[namespace]_[container-name]-[container id].log
// example: /docker-logs/advanced-logs-checker-1566485760-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log

func start() {
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
	}

	go gatherInfo(watcher)
	go maintenance()
}

func maintenance() {
	//updateMu.Lock()
	//defer updateMu.Unlock()
	//
	//for namespace, podNames := range info {
	//for podName, containerIDs := range podNames {
	//for containerID, pod := range containerIDs {
	//	pod.Status.
	//
	//	info[namespace][name][containerID] = pod
	//}}
	//}

}

func gatherInfo(watcher watch.Interface) {
	for {
		event := <-watcher.ResultChan()
		if event.Type == watch.Error {
			logger.Warnf("watch error: %s", event.Object)
			continue
		}

		pod := event.Object.(*corev1.Pod)

		if len(pod.Status.ContainerStatuses) == 0 {
			continue
		}

		podName := pod.Name
		namespace := pod.Namespace

		updateMu.Lock()
		if info[namespace] == nil {
			info[namespace] = make(map[string]map[string]*corev1.Pod)
		}

		if info[namespace][podName] == nil {
			info[namespace][podName] = make(map[string]*corev1.Pod)
		}

		for _, containerStatus := range pod.Status.ContainerStatuses {
			if len(containerStatus.ContainerID) == 0 {
				continue
			}

			if len(containerStatus.ContainerID) < 9 || containerStatus.ContainerID[:9] != "docker://" {
				logger.Fatalf("wrong container id: %s", containerStatus.ContainerID)
			}
			containerID := containerStatus.ContainerID[9:]
			logger.Infof("namespace=%s pod=%s container=%s", namespace, podName, containerID)
			info[namespace][podName][containerID] = pod
		}
		updateMu.Unlock()

		updateEvents.Inc()
	}
}
