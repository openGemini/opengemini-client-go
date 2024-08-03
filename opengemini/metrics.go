package opengemini

import "github.com/prometheus/client_golang/prometheus"

const (
	MetricsNamespace = "opengemini"
	MetricsSubsystem = "client"
)

var _ prometheus.Collector = (*metrics)(nil)

// metrics custom indicators, implementing the prometheus.Collector interface
type metrics struct {
	// queryCounter count all queries
	queryCounter prometheus.Counter
	// writeCounter count all write requests
	writeCounter prometheus.Counter
	// queryLatency calculate the average of the queries, unit milliseconds
	queryLatency prometheus.Summary
	// writeLatency calculate the average of the writes, unit milliseconds
	writeLatency prometheus.Summary
	// queryDatabaseCounter Count queries and classify using measurement
	queryDatabaseCounter *prometheus.CounterVec
	// writeDatabaseCounter count write requests and classify using measurement
	writeDatabaseCounter *prometheus.CounterVec
	// queryDatabaseLatency calculate the average of the queries for database, unit milliseconds
	queryDatabaseLatency *prometheus.SummaryVec
	// writeDatabaseLatency calculate the average of the writes for database, unit milliseconds
	writeDatabaseLatency *prometheus.SummaryVec
}

func (m *metrics) Describe(chan<- *prometheus.Desc) {}

func (m *metrics) Collect(ch chan<- prometheus.Metric) {
	ch <- m.queryCounter
	ch <- m.writeCounter
	ch <- m.queryLatency
	ch <- m.writeLatency
	m.queryDatabaseCounter.Collect(ch)
	m.writeDatabaseCounter.Collect(ch)
	m.queryDatabaseLatency.Collect(ch)
	m.writeDatabaseLatency.Collect(ch)
}

// newMetricsProvider returns metrics registered to registerer.
func newMetricsProvider(customLabels map[string]string) *metrics {
	constLabels := map[string]string{
		"client": "go", // distinguish from other language client
	}
	for k, v := range customLabels {
		constLabels[k] = v
	}

	constQuantiles := map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}
	labelNames := []string{"database"}

	m := &metrics{
		queryCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   MetricsNamespace,
			Subsystem:   MetricsSubsystem,
			Name:        "query_total",
			Help:        "Count of opengemini queries",
			ConstLabels: constLabels,
		}),
		writeCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   MetricsNamespace,
			Subsystem:   MetricsSubsystem,
			Name:        "write_total",
			Help:        "Count of opengemini writes",
			ConstLabels: constLabels,
		}),
		queryLatency: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:   MetricsNamespace,
			Subsystem:   MetricsSubsystem,
			Name:        "query_latency",
			Help:        "Calculate the average of the queries",
			ConstLabels: constLabels,
			Objectives:  constQuantiles,
		}),
		writeLatency: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:   MetricsNamespace,
			Subsystem:   MetricsSubsystem,
			Name:        "write_latency",
			Help:        "Calculate the average of the writes",
			ConstLabels: constLabels,
			Objectives:  constQuantiles,
		}),
		queryDatabaseCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   MetricsNamespace,
			Subsystem:   MetricsSubsystem,
			Name:        "query_database_total",
			Help:        "Count of opengemini queries and classify using measurement",
			ConstLabels: constLabels,
		}, labelNames),
		writeDatabaseCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   MetricsNamespace,
			Subsystem:   MetricsSubsystem,
			Name:        "write_database_total",
			Help:        "Count of opengemini writes and classify using measurement",
			ConstLabels: constLabels,
		}, labelNames),
		queryDatabaseLatency: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:   MetricsNamespace,
			Subsystem:   MetricsSubsystem,
			Name:        "query_database_latency",
			Help:        "Calculate the average of the queries for database",
			ConstLabels: constLabels,
			Objectives:  constQuantiles,
		}, labelNames),
		writeDatabaseLatency: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:   MetricsNamespace,
			Subsystem:   MetricsSubsystem,
			Name:        "write_database_latency",
			Help:        "Calculate the average of the writes for database",
			ConstLabels: constLabels,
			Objectives:  constQuantiles,
		}, labelNames),
	}

	return m
}

// ExposeMetrics expose prometheus metrics
func (c *client) ExposeMetrics() prometheus.Collector {
	return c.metrics
}
