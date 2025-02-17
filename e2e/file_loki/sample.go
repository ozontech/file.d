package file_loki

type sample struct {
	Timestamp    string `json:"timestamp"`
	K8sContainer string `json:"k8s_container"`
	Message      string `json:"message"`
}

var samples []sample = []sample{
	{
		Timestamp:    "2024-08-01T10:31:55.665748976Z",
		K8sContainer: "container",
		Message:      "started",
	},
	{
		Timestamp:    "2024-08-01T10:31:55.665748976Z",
		K8sContainer: "container",
		Message:      "message1",
	},
	{
		Timestamp:    "2024-08-01T10:31:55.665748976Z",
		K8sContainer: "container",
		Message:      "message2",
	},
	{
		Timestamp:    "2024-08-01T10:31:55.665748976Z",
		K8sContainer: "container",
		Message:      "message3",
	},
	{
		Timestamp:    "2024-08-01T10:31:55.665748976Z",
		K8sContainer: "container",
		Message:      "message4",
	},
	{
		Timestamp:    "2024-08-01T10:31:55.665748976Z",
		K8sContainer: "container",
		Message:      "stopped",
	},
	{
		Timestamp:    "2024-08-01T10:31:55.665748976Z",
		K8sContainer: "container2",
		Message:      "started",
	},
	{
		Timestamp:    "2024-08-01T10:31:55.665748976Z",
		K8sContainer: "container2",
		Message:      "message1",
	},
	{
		Timestamp:    "2024-08-01T10:31:55.665748976Z",
		K8sContainer: "container2",
		Message:      "message2",
	},
	{
		Timestamp:    "2024-08-01T10:31:55.665748976Z",
		K8sContainer: "container2",
		Message:      "message3",
	},
	{
		Timestamp:    "2024-08-01T10:31:55.665748976Z",
		K8sContainer: "container2",
		Message:      "message4",
	},
	{
		Timestamp:    "2024-08-01T10:31:55.665748976Z",
		K8sContainer: "container2",
		Message:      "stopped",
	},
}
