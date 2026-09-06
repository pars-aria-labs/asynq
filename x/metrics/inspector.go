package metrics

import (
	"github.com/pars-aria-labs/asynq"
	"github.com/prometheus/client_golang/prometheus"
)

// InspectorMetricsCollector records bounded Inspector operations. Its labels
// intentionally exclude queue names, task IDs, groups, and raw errors, keeping
// cardinality stable even in installations with many dynamic queues.
type InspectorMetricsCollector struct {
	operations *prometheus.CounterVec
	items      *prometheus.CounterVec
	roundTrips *prometheus.CounterVec
	duration   *prometheus.HistogramVec
}

// NewInspectorMetricsCollector creates a Prometheus collector that also
// implements asynq.InspectorOperationObserver. Register it with a Prometheus
// registry, then attach it to an Inspector with SetOperationObserver.
func NewInspectorMetricsCollector() *InspectorMetricsCollector {
	labels := []string{"operation", "state", "action"}
	return &InspectorMetricsCollector{
		operations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "inspector",
			Name:      "operations_total",
			Help:      "Number of bounded Inspector operations by outcome.",
		}, append(labels, "outcome")),
		items: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "inspector",
			Name:      "items_processed_total",
			Help:      "Number of tasks or queue snapshots processed by bounded Inspector operations.",
		}, labels),
		roundTrips: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "inspector",
			Name:      "redis_round_trips_total",
			Help:      "Number of instrumented Redis client executions and pipeline flush attempts; internal retries and cluster fan-out are not counted.",
		}, []string{"operation"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "inspector",
			Name:      "operation_duration_seconds",
			Help:      "Duration of bounded Inspector operations in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, labels),
	}
}

// ObserveInspectorOperation implements asynq.InspectorOperationObserver.
func (collector *InspectorMetricsCollector) ObserveInspectorOperation(operation asynq.InspectorOperation) {
	outcome := "success"
	if operation.Partial() {
		outcome = "partial"
	} else if operation.Err != nil {
		outcome = "error"
	}
	labels := []string{
		inspectorOperationLabel(operation.Name),
		inspectorStateLabel(operation.State),
		inspectorActionLabel(operation.Action),
	}
	collector.operations.WithLabelValues(append(labels, outcome)...).Inc()
	collector.items.WithLabelValues(labels...).Add(float64(operation.Processed))
	collector.roundTrips.WithLabelValues(labels[0]).Add(float64(operation.RedisRoundTrips))
	collector.duration.WithLabelValues(labels...).Observe(operation.Duration.Seconds())
}

func inspectorOperationLabel(value string) string {
	switch value {
	case asynq.InspectorOperationTaskBatch,
		asynq.InspectorOperationTaskBatchSeries,
		asynq.InspectorOperationTaskIDs,
		asynq.InspectorOperationQueueInfoBatch:
		return value
	default:
		return "unknown"
	}
}

func inspectorStateLabel(value string) string {
	switch value {
	case "", "active", "pending", "scheduled", "retry", "archived", "completed", "aggregating":
		return value
	default:
		return "unknown"
	}
}

func inspectorActionLabel(value string) string {
	switch value {
	case "", "delete", "run", "archive":
		return value
	default:
		return "unknown"
	}
}

// Describe implements prometheus.Collector.
func (collector *InspectorMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	collector.operations.Describe(ch)
	collector.items.Describe(ch)
	collector.roundTrips.Describe(ch)
	collector.duration.Describe(ch)
}

// Collect implements prometheus.Collector.
func (collector *InspectorMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	collector.operations.Collect(ch)
	collector.items.Collect(ch)
	collector.roundTrips.Collect(ch)
	collector.duration.Collect(ch)
}

var _ asynq.InspectorOperationObserver = (*InspectorMetricsCollector)(nil)
var _ prometheus.Collector = (*InspectorMetricsCollector)(nil)
