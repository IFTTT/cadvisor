package kairosdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/google/cadvisor/client"
	info "github.com/google/cadvisor/info/v1"
	storage "github.com/google/cadvisor/storage"
)

type kairosdbMetric struct {
	Timestamp int64             `json:"timestamp"`
	Name      string            `json:"name"`
	Value     interface{}       `json:"value"`
	Tags      map[string]string `json:"tags"`
}

type kairosdbStorage struct {
	url            *url.URL
	httpClient     *http.Client
	headers        map[string]string
	machineName    string
	bufferDuration time.Duration
	lastWrite      time.Time
	points         []*kairosdbMetric
	lock           sync.Mutex
	readyToFlush   func() bool
}

// Series names
const (
	// Cumulative CPU usage
	serCpuUsageTotal  string = "cpu_usage_total"
	serCpuUsageSystem string = "cpu_usage_system"
	serCpuUsageUser   string = "cpu_usage_user"
	serCpuUsagePerCpu string = "cpu_usage_per_cpu"
	// Smoothed average of number of runnable threads x 1000.
	serLoadAverage string = "load_average"
	// Memory Usage
	serMemoryUsage string = "memory_usage"
	// Working set size
	serMemoryWorkingSet string = "memory_working_set"
	// Cumulative count of bytes received.
	serRxBytes string = "rx_bytes"
	// Cumulative count of receive errors encountered.
	serRxErrors string = "rx_errors"
	// Cumulative count of bytes transmitted.
	serTxBytes string = "tx_bytes"
	// Cumulative count of transmit errors encountered.
	serTxErrors string = "tx_errors"
	// Filesystem device.
	serFsDevice string = "fs_device"
	// Filesystem limit.
	serFsLimit string = "fs_limit"
	// Filesystem usage.
	serFsUsage string = "fs_usage"
)

// Field names
const (
	fieldValue  string = "value"
	fieldType   string = "type"
	fieldDevice string = "device"
)

// Tag names
const (
	tagMachineName   string = "machine"
	tagContainerName string = "container_name"
)

func init() {
	storage.RegisterStorageDriver("kairosdb", new)
}

// KairosDB stores timestamps in milliseconds.
func msTime(t time.Time) (ms int64) {
	ms = t.UnixNano() / 1000000

	return
}

func new() (storage.StorageDriver, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	return newStorage(
		hostname,
		*storage.ArgDbHost,
		*storage.ArgDbBufferDuration,
	)
}

func newStorage(
	machineName,
	kairosdbHost string,
	bufferDuration time.Duration,
) (*kairosdbStorage, error) {
	url := &url.URL{
		Scheme: "http",
		Host:   kairosdbHost,
		Path:   "api/v1/datapoints",
	}

	client := &http.Client{}

	ret := &kairosdbStorage{
		url:            url,
		httpClient:     client,
		machineName:    machineName,
		bufferDuration: bufferDuration,
		lastWrite:      time.Now(),
		points:         make([]*kairosdbMetric, 0),
	}

	ret.readyToFlush = ret.defaultReadyToFlush
	return ret, nil
}

func (self *kairosdbStorage) defaultReadyToFlush() bool {
	return time.Since(self.lastWrite) >= self.bufferDuration
}

func (self *kairosdbStorage) AddStats(ref info.ContainerReference, stats *info.ContainerStats) error {
	if stats == nil {
		return nil
	}
	var pointsToFlush []*kairosdbMetric
	func() {
		// AddStats will be invoked simultaneously from multiple threads and only one of them will perform a write.
		self.lock.Lock()
		defer self.lock.Unlock()

		self.points = append(self.points, self.containerStatsToPoints(ref, stats)...)
		self.points = append(self.points, self.containerFilesystemStatsToPoints(ref, stats)...)
		if self.readyToFlush() {
			pointsToFlush = self.points
			self.points = make([]*kairosdbMetric, 0)
			self.lastWrite = time.Now()
		}
	}()
	if len(pointsToFlush) > 0 {
		points := make([]kairosdbMetric, len(pointsToFlush))
		for i, p := range pointsToFlush {
			points[i] = *p
		}

		b, _ := json.Marshal(points)
		resp, err := self.httpClient.Post(self.url.String(), "application/json", bytes.NewBuffer(b))
		if err != nil || resp.StatusCode != 204 {
			return fmt.Errorf("failed to write stats to kairosDb - %s", err)
		}
		defer resp.Body.Close()
	}
	return nil
}

func (self *kairosdbStorage) Close() error {
	self.httpClient = nil
	return nil
}

func (self *kairosdbStorage) OverrideReadyToFlush(readyToFlush func() bool) {
	self.readyToFlush = readyToFlush
}

// Adds additional tags to the existing tags of a point
func addTagsToPoint(point *kairosdbMetric, tags map[string]string) {
	if point.Tags == nil {
		point.Tags = tags
	} else {
		for k, v := range tags {
			point.Tags[k] = v
		}
	}
}

func (self *kairosdbStorage) containerFilesystemStatsToPoints(
	ref info.ContainerReference,
	stats *info.ContainerStats) (points []*kairosdbMetric) {
	if len(stats.Filesystem) == 0 {
		return points
	}
	for _, fsStat := range stats.Filesystem {
		pointFsUsage := makePoint(serFsUsage, stats.Timestamp, int64(fsStat.Usage))
		tagsFsUsage := map[string]string{
			fieldDevice: fsStat.Device,
			fieldType:   "usage",
		}
		addTagsToPoint(pointFsUsage, tagsFsUsage)

		pointFsLimit := makePoint(serFsLimit, stats.Timestamp, int64(fsStat.Limit))
		tagsFsLimit := map[string]string{
			fieldDevice: fsStat.Device,
			fieldType:   "limit",
		}
		addTagsToPoint(pointFsLimit, tagsFsLimit)

		points = append(points, pointFsUsage, pointFsLimit)
	}

	self.tagPoints(ref, stats, points)

	return points
}

func (self *kairosdbStorage) containerStatsToPoints(
	ref info.ContainerReference,
	stats *info.ContainerStats,
) (points []*kairosdbMetric) {
	// CPU usage: Total usage in nanoseconds
	points = append(points, makePoint(serCpuUsageTotal, stats.Timestamp, stats.Cpu.Usage.Total))

	// CPU usage: Time spend in system space (in nanoseconds)
	points = append(points, makePoint(serCpuUsageSystem, stats.Timestamp, stats.Cpu.Usage.System))

	// CPU usage: Time spent in user space (in nanoseconds)
	points = append(points, makePoint(serCpuUsageUser, stats.Timestamp, stats.Cpu.Usage.User))

	// CPU usage per CPU
	for i := 0; i < len(stats.Cpu.Usage.PerCpu); i++ {
		point := makePoint(serCpuUsagePerCpu, stats.Timestamp, stats.Cpu.Usage.PerCpu[i])
		tags := map[string]string{"instance": fmt.Sprintf("%v", i)}
		addTagsToPoint(point, tags)

		points = append(points, point)
	}

	// Load Average
	points = append(points, makePoint(serLoadAverage, stats.Timestamp, stats.Cpu.LoadAverage))

	// Memory Usage
	points = append(points, makePoint(serMemoryUsage, stats.Timestamp, stats.Memory.Usage))

	// Working Set Size
	points = append(points, makePoint(serMemoryWorkingSet, stats.Timestamp, stats.Memory.WorkingSet))

	// Network Stats
	points = append(points, makePoint(serRxBytes, stats.Timestamp, stats.Network.RxBytes))
	points = append(points, makePoint(serRxErrors, stats.Timestamp, stats.Network.RxErrors))
	points = append(points, makePoint(serTxBytes, stats.Timestamp, stats.Network.TxBytes))
	points = append(points, makePoint(serTxErrors, stats.Timestamp, stats.Network.TxErrors))

	self.tagPoints(ref, stats, points)

	return points
}

// Creates a measurement point with a single value field
func makePoint(name string, timestamp time.Time, value interface{}) *kairosdbMetric {
	return &kairosdbMetric{
		Timestamp: msTime(timestamp),
		Name:      name,
		Value:     value,
	}
}

// Set tags and timestamp for all points of the batch.
func (self *kairosdbStorage) tagPoints(ref info.ContainerReference, stats *info.ContainerStats, points []*kairosdbMetric) {
	var containerName string
	var envs map[string]string
	var err error
	// Use container alias if possible
	if len(ref.Aliases) > 0 {
		containerName = ref.Aliases[0]
	} else {
		containerName = ref.Name
	}

	client, err := client.NewClient("http://localhost:8000/")
	if err != nil {
		glog.Errorf("tried to make client and got error %v", err)
	}

	sInfo, err := client.ContainerInfo(ref.Name, &info.ContainerInfoRequest{NumStats: 1})
	if err != nil {
		glog.Errorf("couldn't get container info for %s: %v", ref.Name, err)
	} else {
		envs = sInfo.Spec.Envs
	}

	commonTags := map[string]string{
		tagMachineName:   self.machineName,
		tagContainerName: containerName,
	}
	for i := 0; i < len(points); i++ {
		// merge with existing tags if any
		addTagsToPoint(points[i], commonTags)
		addTagsToPoint(points[i], ref.Labels)
		addTagsToPoint(points[i], envs)
	}
}
