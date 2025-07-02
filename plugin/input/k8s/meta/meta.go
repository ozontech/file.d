package meta

import (
	"errors"
	"strings"
)

type K8sMetaInformation struct {
	Namespace     string
	PodName       string
	ContainerName string
	ContainerID   string
	Pod           *podMeta
}

func NewK8sMetaInformation(fullFilename string) (K8sMetaInformation, error) {
	lastSlash := strings.LastIndexByte(fullFilename, '/')
	if lastSlash == -1 || lastSlash+1 >= len(fullFilename) {
		return K8sMetaInformation{}, errors.New("invalid filename: no valid file name found")
	}

	// Extract the filename without the path
	filename := fullFilename[lastSlash+1 : len(fullFilename)-4]
	if len(filename) < 4 { // Ensure there's enough length for the expected format
		return K8sMetaInformation{}, errors.New("invalid filename: too short after removing extension")
	}

	// Extract pod name
	underscore := strings.IndexByte(filename, '_')
	if underscore == -1 {
		return K8sMetaInformation{}, errors.New("invalid filename: no pod found")
	}
	pod := filename[:underscore]

	// Extract namespace
	filename = filename[underscore+1:]
	underscore = strings.IndexByte(filename, '_')
	if underscore == -1 {
		return K8sMetaInformation{}, errors.New("invalid filename: no namespace found")
	}
	ns := filename[:underscore]

	// Extract container and CID
	filename = filename[underscore+1:]
	if len(filename) < 65 { // Ensure there's enough length for the container and CID
		return K8sMetaInformation{}, errors.New("invalid filename: too short for container and CID")
	}

	// Extract container name
	container := filename[:len(filename)-64]
	// Trim any trailing hyphen or unwanted characters from the container name
	container = strings.TrimRight(container, "-")

	cid := filename[len(filename)-64:]

	var podMeta *podMeta
	if !DisableMetaUpdates {
		_, podMeta = GetPodMeta(Namespace(ns), PodName(pod), ContainerID(cid))
	}

	return K8sMetaInformation{
		Namespace:     ns,
		PodName:       pod,
		ContainerName: container,
		ContainerID:   cid,
		Pod:           podMeta,
	}, nil
}

func (m K8sMetaInformation) GetData() map[string]any {
	return map[string]any{
		"pod_name":       m.PodName,
		"namespace":      m.Namespace,
		"container_name": m.ContainerName,
		"container_id":   m.ContainerID,
		"pod":            m.Pod,
	}
}
