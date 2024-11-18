package meta

import "strings"

type K8sMetaInformation struct {
	namespace     string
	podName       string
	containerName string
	containerID   string
}

func NewK8sMetaInformation(fullFilename string) K8sMetaInformation {
	lastSlash := strings.LastIndexByte(fullFilename, '/')
	filename := fullFilename[lastSlash+1 : len(fullFilename)-4]
	underscore := strings.IndexByte(filename, '_')
	pod := filename[:underscore]
	filename = filename[underscore+1:]
	underscore = strings.IndexByte(filename, '_')
	ns := filename[:underscore]
	filename = filename[underscore+1:]
	container := filename[:len(filename)-65]
	cid := filename[len(filename)-64:]

	return K8sMetaInformation{
		ns, pod, container, cid,
	}
}

func (m K8sMetaInformation) GetData() map[string]any {
	return map[string]any{
		"pod":          m.podName,
		"namespace":    m.namespace,
		"container":    m.containerName,
		"container_id": m.containerID,
	}
}
