// config/types.go
package config

import "sync"

// Config structure
type Config struct {
    Debug bool `json:"debug"`
}

// TestResult structure
type TestResult struct {
    CPU  string
    DIMM string
    HDD  string
}

// PerformanceStats tracks overall performance metrics
type PerformanceStats struct {
    CPU    CPUPerformance
    Memory MemoryPerformance
    Disk   []DiskPerformance
    RawDisk []RawDiskPerformance
    mu     sync.Mutex
}

// Lock locks the PerformanceStats mutex
func (ps *PerformanceStats) Lock() {
    ps.mu.Lock()
}

// Unlock unlocks the PerformanceStats mutex
func (ps *PerformanceStats) Unlock() {
    ps.mu.Unlock()
}

// CPUPerformance tracks CPU performance metrics
type CPUPerformance struct {
    GFLOPS     float64        // Total GFLOPS
    CoreGFLOPS map[int]float64 // Per-core GFLOPS
    IntegerOPS float64        // Integer operations performance
    FloatOPS   float64        // Floating-point operations performance
    VectorOPS  float64        // Vector operations performance
    NumCores   int            // Number of active cores
}

// MemoryPerformance tracks memory performance metrics
type MemoryPerformance struct {
    ReadSpeed        float64 // in MB/s
    WriteSpeed       float64 // in MB/s
    RandomAccessSpeed float64 // in MB/s
    UsagePercent     float64 // Percentage of memory used
}

// DiskPerformance tracks disk performance metrics
type DiskPerformance struct {
    ReadSpeed  float64 // in MB/s
    WriteSpeed float64 // in MB/s
    MountPoint string
    Mode       string
    BlockSize  int64
}

type RawDiskPerformance struct {
	DevicePath string  // Path to the raw device (e.g., "/dev/sda")
	Mode       string  // "sequential" or "random"
	BlockSize  int64   // Block size in bytes used for testing
	ReadSpeed  float64 // Read speed in MB/s
	WriteSpeed float64 // Write speed in MB/s
}
