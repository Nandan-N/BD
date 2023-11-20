package dlts

type Register struct {
    NodeId string `json:"node_id"`
    NodeIp string `json:"node_ip"`
}

type Heartbeat struct {
    NodeId string `json:"node_id"`
    Heartbeat bool `json:"heartbeat"`
}

type TestConfig struct {
    TestId string `json:"test_id"`
    Type string `json:"test_type"`
    MessageDelay int `json:"test_message_delay"`
}

type Trigger struct {
    TestId string `json:"test_id"`
    Trigger string `json:"trigger"`
}

type TestResult struct {
	Mean   float64 `json:"mean_latency"`
	Median float64 `json:"median_latency"`
	Min    float64 `json:"min_latency"`
	Max    float64 `json:"max_latency"`
}

type Metrics struct {
    NodeId string `json:"node_id"` 
    TestId string `json:"test_id"`
    ReportId string `json:"report_id"`
    TestMetrics TestResult `json:"metrics"`
}
