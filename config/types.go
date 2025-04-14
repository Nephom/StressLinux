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
    CPU     CPUPerformance
    Memory  MemoryPerformance
    Disk    []DiskPerformance
    RawDisk []RawDiskPerformance
    mu      sync.Mutex
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
    GFLOPS         float64        // Total GFLOPS
    CoreGFLOPS     map[int]float64 // Per-core GFLOPS
    IntegerOPS     float64        // Integer operations performance
    FloatOPS       float64        // Floating-point operations performance
    VectorOPS      float64        // Vector operations performance
    NumCores       int            // Number of active cores
    CacheInfo      CacheInfo      // Cache information
    IntegerCount   uint64         // Integer test operation count
    FloatCount     uint64         // Float test operation count
    VectorCount    uint64         // Vector test operation count
    CacheCount     uint64         // Cache test operation count
    BranchCount    uint64         // Branch test operation count
    CryptoCount    uint64         // Crypto test operation count
}

// CacheInfo stores the sizes of L1, L2, and L3 caches
type CacheInfo struct {
    L1Size int64 // in bytes
    L2Size int64 // in bytes
    L3Size int64 // in bytes
}

// MemoryPerformance tracks memory performance metrics
type MemoryPerformance struct {
    ReadSpeed        float64 // in MB/s
    WriteSpeed       float64 // in MB/s
    RandomAccessSpeed float64 // in MB/s
    UsagePercent     float64 // Percentage of memory used
    WriteCount       uint64  // Write operation count
    ReadCount        uint64  // Read operation count
    RandomAccessCount uint64  // Random access operation count
}

// DiskPerformance tracks disk performance metrics
type DiskPerformance struct {
    ReadSpeed  float64 // in MB/s
    WriteSpeed float64 // in MB/s
    MountPoint string
    Mode       string
    BlockSize  int64
    WriteCount uint64  // Write operation count
    ReadCount  uint64  // Read operation count
}

// RawDiskPerformance tracks raw disk performance metrics
type RawDiskPerformance struct {
    DevicePath string  // Path to the raw device (e.g., "/dev/sda")
    Mode       string  // "sequential" or "random"
    BlockSize  int64   // Block size in bytes used for testing
    ReadSpeed  float64 // Read speed in MB/s
    WriteSpeed float64 // Write speed in MB/s
    WriteCount uint64  // Write operation count
    ReadCount  uint64  // Read operation count
}
