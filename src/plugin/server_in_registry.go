package plugin

type ServerInRegistry struct {
	Host            string `json:"host"`
	Port            int    `json:"port"`
	ApplicationName string `json:"applicationName"`
	Version         string `json:"version"`
	Group           string `json:"group"`
	Pid             string `json:"pid"`
	Timestamp       string `json:"timestamp"`
}
