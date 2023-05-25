// (C) Copyright 2023 Hewlett Packard Enterprise Development LP
// SPDX-License-Identifier: Apache-2.0

// Based on the metricsgenerationprocessor code.

package bmaasprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/bmaasprocessor"

import (
	"context"
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"math/rand"
	"strconv"

	//	"github.com/aerospike/aerospike-client-go/v6/logger"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type lastValue struct {
	timestamp pcommon.Timestamp
	value     float64
}

type BMaasProcessor struct {
	logger     *zap.Logger
	lastValues map[string]lastValue
}

func newBMaasProcessor(logger *zap.Logger) *BMaasProcessor {
	return &BMaasProcessor{
		logger:     logger,
		lastValues: make(map[string]lastValue),
	}
}

func getNameToMetricMap(rm pmetric.ResourceMetrics) map[string]pmetric.Metric {
	ilms := rm.ScopeMetrics()
	metricMap := make(map[string]pmetric.Metric)

	for i := 0; i < ilms.Len(); i++ {
		ilm := ilms.At(i)
		metricSlice := ilm.Metrics()
		for j := 0; j < metricSlice.Len(); j++ {
			metric := metricSlice.At(j)
			metricMap[metric.Name()] = metric
		}
	}
	return metricMap
}

func appendMetric(ilm pmetric.ScopeMetrics, name, unit string) pmetric.Metric {
	metric := ilm.Metrics().AppendEmpty()
	metric.SetName(name)
	metric.SetUnit(unit)

	return metric
}

// Start is invoked during service startup.
func (bp *BMaasProcessor) Start(context.Context, component.Host) error {
	return nil
}

func addMemUtilizationMetric(rm pmetric.ResourceMetrics, mMap map[string]pmetric.Metric) {
	// Generate memory utilisation percentage
	// node_memory_MemTotal_bytes and node_memory_MemFree_bytes are both gauge metrics contain
	// double values.
	memTotal, exists := mMap["node_memory_MemTotal_bytes"]
	if !exists {
		return
	}
	memFree, exists := mMap["node_memory_MemFree_bytes"]
	if !exists {
		return
	}

	// Assuming single-value metrics
	mTotDp := memTotal.Gauge().DataPoints().At(0)
	mFreeDp := memFree.Gauge().DataPoints().At(0)

	memUsed := appendMetric(rm.ScopeMetrics().At(0), "node_memory_MemUsed_percent", "percent")
	memUsed.SetEmptyGauge()
	mTotBytes := mTotDp.DoubleValue()
	mFreeBytes := mFreeDp.DoubleValue()

	mUsedDp := memUsed.Gauge().DataPoints().AppendEmpty()
	//mTotDp.CopyTo(mUsedDp)
	value := (1.0 - (mFreeBytes / mTotBytes)) * 100.0
	mUsedDp.SetDoubleValue(value)
	mUsedDp.SetTimestamp(mTotDp.Timestamp())
}

func buildDataPointMap(dps pmetric.NumberDataPointSlice, key string) map[string][]pmetric.NumberDataPoint {
	ret := make(map[string][]pmetric.NumberDataPoint)

	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		dpv, exists := dp.Attributes().Get(key)
		if !exists {
			continue
		}
		row, exists := ret[dpv.Str()]
		if !exists {
			row = []pmetric.NumberDataPoint{}
		}
		ret[dpv.Str()] = append(row, dp)
	}
	return ret
}

func sumFirstValues(values map[string][]pmetric.NumberDataPoint) float64 {
	var sum float64
	for _, v := range values {
		sum += v[0].DoubleValue()
	}
	return sum
}
func addFilesystemUtilizationMetric(rm pmetric.ResourceMetrics, mMap map[string]pmetric.Metric) {
	// The fs metrics have two dimensions: device, and mountpoint.
	// This gives a metric like the following:
	// {
	//   "name": "node_filesystem_size_bytes",
	//   "description": "Filesystem size in bytes.",
	//   "gauge": {
	//     "dataPoints": [
	//      {
	//        "attributes": [
	//     	    {"key": "device", "value": { "stringValue": "/dev/nvme0n1p2" }},
	//     	    {"key": "mountpoint", "value": { "stringValue": "/" }},
	//          ...
	//        ]
	//        "timeUnixNano": "1684922566617000000",
	//        "asDouble": 502392610816
	//      },
	//      {
	//        "attributes": [
	//     	    {"key": "device", "value": { "stringValue": "/dev/nvme0n1p2" }},
	//     	    {"key": "mountpoint", "value": { "stringValue": "/var/snap/firefox/common/host-hunspell" }},
	//          ...
	//        ]
	//        "timeUnixNano": "1684922566617000000",
	//        "asDouble": 502392610816
	//      },
	//      ...
	//   }
	// }
	//
	// From a space/usage calculation pov, it only makes sense to include each device once, and ignore the
	// various mount points.

	fsTotal, exists := mMap["node_filesystem_size_bytes"]
	if !exists {
		return
	}
	fsFree, exists := mMap["node_filesystem_avail_bytes"]
	if !exists {
		return
	}
	totalMap := buildDataPointMap(fsTotal.Gauge().DataPoints(), "device")
	freeMap := buildDataPointMap(fsFree.Gauge().DataPoints(), "device")
	totalBytes := sumFirstValues(totalMap)
	freeBytes := sumFirstValues(freeMap)

	fsUsed := appendMetric(rm.ScopeMetrics().At(0), "node_filesystem_used_percent", "percent")
	fsUsed.SetEmptyGauge()
	fsUsedDp := fsUsed.Gauge().DataPoints().AppendEmpty()

	value := (1.0 - (freeBytes / totalBytes)) * 100.0
	fsUsedDp.SetDoubleValue(value)
	fsUsedDp.SetTimestamp(fsTotal.Gauge().DataPoints().At(0).Timestamp())

}

func addRateMetric(accumulatorName, rateName, rateUnit string, rm pmetric.ResourceMetrics, mMap map[string]pmetric.Metric, last map[string]lastValue) {
	metric, exists := mMap[accumulatorName]
	if !exists {
		return
	}
	mdp := metric.Gauge().DataPoints().At(0)
	lastVal, exists := last[accumulatorName]
	last[accumulatorName] = lastValue{
		mdp.Timestamp(),
		mdp.DoubleValue(),
	}
	if !exists {
		return
	}
	dv := mdp.DoubleValue() - lastVal.value
	// dt is seconds
	dt := float64(int64(mdp.Timestamp()-lastVal.timestamp) / 1000000000)

	rateMetric := appendMetric(rm.ScopeMetrics().At(0), rateName, rateUnit)
	rateMetric.SetEmptyGauge()
	rateDp := rateMetric.Gauge().DataPoints().AppendEmpty()
	rateDp.SetDoubleValue(dv / dt)
	rateDp.SetTimestamp(mdp.Timestamp())
}

func addFakeGaugeMetric(metricName, metricUnit string, initVal float64, metricTimestamp pcommon.Timestamp, rm pmetric.ResourceMetrics, last map[string]lastValue) {

	lastVal, exists := last[metricName]
	if !exists {
		last[metricName] = lastValue{
			metricTimestamp,
			initVal,
		}
		return
	}
	delta := (rand.Float64() - 0.5) * 20.0
	newVal := lastVal.value + delta
	if newVal < 0 {
		newVal = 0
	} else if delta > 100 {
		newVal = 100
	}

	fakeMetric := appendMetric(rm.ScopeMetrics().At(0), metricName, metricUnit)
	fakeMetric.SetEmptyGauge()
	fakeDp := fakeMetric.Gauge().DataPoints().AppendEmpty()
	fakeDp.SetDoubleValue(newVal)
	fakeDp.SetTimestamp(metricTimestamp)

	last[metricName] = lastValue{
		metricTimestamp,
		newVal,
	}

}

func addLatencyMetric(durationName, countName, latencyName string, rm pmetric.ResourceMetrics, mMap map[string]pmetric.Metric, last map[string]lastValue) {
	dm, exists := mMap[durationName]
	if !exists || dm.Sum().DataPoints().Len() == 0 {
		fmt.Printf("No metric %q or zero datapoints\n", durationName)
		return
	}
	cm, exists := mMap[countName]
	if !exists || cm.Sum().DataPoints().Len() == 0 {
		fmt.Printf("No metric %q\n", countName)
		return
	}

	durations := buildDataPointMap(dm.Sum().DataPoints(), "device")
	counts := buildDataPointMap(cm.Sum().DataPoints(), "device")
	totalDuration := sumFirstValues(durations)
	totalCount := sumFirstValues(counts)

	lastDuration, exists := last[durationName]
	lastCount, exists2 := last[countName]
	last[durationName] = lastValue{
		dm.Sum().DataPoints().At(0).Timestamp(),
		totalDuration,
	}
	last[countName] = lastValue{
		cm.Sum().DataPoints().At(0).Timestamp(),
		totalCount,
	}
	if !exists || !exists2 {
		return
	}
	durationSecs := totalDuration - lastDuration.value
	count := totalCount - lastCount.value
	var latencySecs float64
	if count == 0 {
		latencySecs = 0
	} else {
		latencySecs = durationSecs / count
	}

	latencyMetric := appendMetric(rm.ScopeMetrics().At(0), latencyName, "s")
	latencyMetric.SetEmptyGauge()
	latencyDp := latencyMetric.Gauge().DataPoints().AppendEmpty()
	// Record latency in ms
	latencyDp.SetDoubleValue(latencySecs * 1000.0)
	latencyDp.SetTimestamp(dm.Sum().DataPoints().At(0).Timestamp())
}
func addCPUCount(rm pmetric.ResourceMetrics, mMap map[string]pmetric.Metric) {
	cfm, exists := mMap["node_cpu_frequency_max_hertz"]
	if !exists {
		return
	}
	cpuMaxIdx := 0
	for i := 0; i < cfm.Gauge().DataPoints().Len(); i++ {
		dp := cfm.Gauge().DataPoints().At(i)
		cpuIdxStr, exists := dp.Attributes().Get("cpu")
		if exists {
			cpuIdx, err := strconv.Atoi(cpuIdxStr.Str())
			if err == nil && cpuIdx > cpuMaxIdx {
				cpuMaxIdx = cpuIdx
			}
		}
	}
	cpuCountMetric := appendMetric(rm.ScopeMetrics().At(0), "node_cpu_count", "")
	cpuCountMetric.SetEmptyGauge()
	cpuCountDp := cpuCountMetric.Gauge().DataPoints().AppendEmpty()
	// Record latency in ms
	cpuCountDp.SetDoubleValue(float64(cpuMaxIdx) + 1)
	cpuCountDp.SetTimestamp(cfm.Gauge().DataPoints().At(0).Timestamp())
}

// processMetrics implements the ProcessMetricsFunc type.
func (mgp *BMaasProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	resourceMetricsSlice := md.ResourceMetrics()

	for i := 0; i < resourceMetricsSlice.Len(); i++ {
		rm := resourceMetricsSlice.At(i)
		nameToMetricMap := getNameToMetricMap(rm)
		// Get a representative timestamp
		firstMetric := rm.ScopeMetrics().At(0).Metrics().At(0)
		var ts pcommon.Timestamp
		if firstMetric.Type() == pmetric.MetricTypeGauge {
			ts = firstMetric.Gauge().DataPoints().At(0).Timestamp()
		} else {
			// Assume sum
			ts = firstMetric.Sum().DataPoints().At(0).Timestamp()
		}
		// The add functions assume a single scope.
		if rm.ScopeMetrics().Len() > 1 {
			mgp.logger.Warn("Metric has > 1 scope")
		}
		addMemUtilizationMetric(rm, nameToMetricMap)
		addFilesystemUtilizationMetric(rm, nameToMetricMap)
		addRateMetric("node_netstat_IpExt_OutOctets", "node_netstat_IpExt_Out_bps", "bps", rm, nameToMetricMap, mgp.lastValues)
		addRateMetric("node_netstat_IpExt_InOctets", "node_netstat_IpExt_In_bps", "bps", rm, nameToMetricMap, mgp.lastValues)
		addLatencyMetric("node_disk_write_time_seconds", "node_disk_writes_completed", "node_disk_write_latency", rm, nameToMetricMap, mgp.lastValues)
		addLatencyMetric("node_disk_read_time_seconds", "node_disk_reads_completed", "node_disk_read_latency", rm, nameToMetricMap, mgp.lastValues)
		addRateMetric("process_cpu_seconds_total", "process_cpu_rate", "s/s", rm, nameToMetricMap, mgp.lastValues)
		addFakeGaugeMetric("process_cpu_utilization", "percent", 55.0, ts, rm, mgp.lastValues)
		addCPUCount(rm, nameToMetricMap)
	}
	return md, nil
}

// Shutdown is invoked during service shutdown.
func (mgp *BMaasProcessor) Shutdown(context.Context) error {
	return nil
}
