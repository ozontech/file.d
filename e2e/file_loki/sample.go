package file_loki

type sample struct {
	Timestamp    string `json:"timestamp"`
	K8sContainer string `json:"k8s_container"`
	Message      string `json:"message"`
}

var samples = []sample{
	{
		Timestamp:    "",
		K8sContainer: "container",
		Message:      "started",
	},
	{
		Timestamp:    "",
		K8sContainer: "container",
		Message:      "message1",
	},
	{
		Timestamp:    "",
		K8sContainer: "container",
		Message:      "message2",
	},
	{
		Timestamp:    "",
		K8sContainer: "container",
		Message:      "message3",
	},
	{
		Timestamp:    "",
		K8sContainer: "container",
		Message:      "message4",
	},
	{
		Timestamp:    "",
		K8sContainer: "container",
		Message:      "stopped",
	},
	{
		Timestamp:    "",
		K8sContainer: "container2",
		Message:      "started",
	},
	{
		Timestamp:    "",
		K8sContainer: "container2",
		Message:      "message1",
	},
	{
		Timestamp:    "",
		K8sContainer: "container2",
		Message:      "message2",
	},
	{
		Timestamp:    "",
		K8sContainer: "container2",
		Message:      "message3",
	},
	{
		Timestamp:    "",
		K8sContainer: "container2",
		Message:      "message4",
	},
	{
		Timestamp:    "",
		K8sContainer: "container2",
		Message:      "stopped",
	},
}
