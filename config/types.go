// config/types.go
package config

import "sync"

// Config structure
type Config struct {
    Debug      bool    `json:"debug" description:"Enable debug mode"`
    CPU        bool    `json:"CPU" description:"Enable CPU stress testing"`
    Cores      int     `json:"Cores" description:"Number of CPU cores to stress (0 means all cores)"`
    Load       string  `json:"Load" description:"CPU load level: High(2), Low(1), or Default(0)"`
    Memory     bool    `json:"Memory" description:"Enable memory stress testing"`
    MEMPercent float64 `json:"MEMPercent" description:"Memory testing percentage (0.1-9.9 for 1%-99% of total memory, e.g., 1.5 for 15%)"`
    Mountpoint string  `json:"Mountpoint" description:"Comma-separated mount points to test (e.g., /mnt/disk1,/mnt/disk2)"`
    RAWDisk    string  `json:"RAWDisk" description:"Raw disk devices to test (e.g., /dev/sdb, /dev/nvme0n1)"`
    Size       string  `json:"Size" description:"Size of test files for disk tests or test size for raw disk tests (supports K, M, G units)"`
    Offset     string  `json:"Offset" description:"Start offset from the beginning of the raw device (e.g., 1G, 100M, supports K, M, G units)"`
    Block      string  `json:"Block" description:"Comma-separated block sizes for disk and raw disk operations (supports K, M, G units)"`
    Mode       string  `json:"Mode" description:"Test mode for mountpoint(sequential/random/both) and raw disk(Only sequential or random)"`
    NUMANode   int     `json:"NUMANode" description: Test NUMA node, default is -1 means test all nodes."`
}

// TestResult structure
type TestResult struct {
    CPU  string
    DIMM string
	Mountpoint    map[string]string
	RawDisk       map[string]string
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
    ReadSpeed            float64
    WriteSpeed           float64
    RandomAccessSpeed    float64
    MinReadSpeed         float64 // 新增：最小讀取速度
    MaxReadSpeed         float64 // 新增：最大讀取速度
    MinWriteSpeed        float64 // 新增：最小寫入速度
    MaxWriteSpeed        float64 // 新增：最大寫入速度
    MinRandomAccessSpeed float64 // 已有：最小隨機存取速度
    MaxRandomAccessSpeed float64 // 已有：最大隨機存取速度
    SumReadSpeed         float64 // 新增：用於計算平均讀取速度
    SumWriteSpeed        float64 // 新增：用於計算平均寫入速度
    SumRandomAccessSpeed float64 // 新增：用於計算平均隨機存取速度
    ReadSpeedCount       int     // 新增：讀取速度更新次數
    WriteSpeedCount      int     // 新增：寫入速度更新次數
    RandomAccessCount    uint64
    ReadCount            uint64
    WriteCount           uint64
    UsagePercent         float64
}

// DiskPerformance tracks disk performance metrics
type DiskPerformance struct {
    MountPoint        string
    Mode              string  // sequential or random
    IOMode            string  // direct or async
    BlockSize         int64
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
