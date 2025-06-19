package utils

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"math/rand"
	"stress/config"
	"syscall"
	"unsafe"
)

// LogMessage handles both console output and file logging
func LogMessage(message string, debug bool) {
    timestamp := time.Now().Format("2006-01-02 15:04:05")
    logEntry := fmt.Sprintf("%s | %s", timestamp, message)

    // Check if stress.log already exists
    fileInfo, err := os.Stat("stress.log")
    var creationTime string

    if os.IsNotExist(err) {
        creationTime = timestamp
        f, err := os.Create("stress.log")
        if err != nil {
            fmt.Fprintf(os.Stderr, "Failed to create stress.log: %v\n", err)
            return
        }
        defer f.Close()
        if _, err := f.WriteString(fmt.Sprintf("Log file created at: %s\n", creationTime)); err != nil {
            fmt.Fprintf(os.Stderr, "Failed to write creation time: %v\n", err)
        }
    } else {
        creationTime = fileInfo.ModTime().Format("2006-01-02 15:04:05")
    }

    f, err := os.OpenFile("stress.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed to open stress.log: %v\n", err)
        return
    }
    defer f.Close()

    logger := log.New(f, "", 0)
    logger.Println(logEntry)

    // Output to console for critical messages (debug == false) or when debug is enabled
    if !debug {
        fmt.Println(logEntry)
    }
}

// FormatSize converts bytes to human-readable string (KB, MB, GB)
func FormatSize(size int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	if size >= GB {
		return fmt.Sprintf("%.2fGB", float64(size)/float64(GB))
	}
	if size >= MB {
		return fmt.Sprintf("%.2fMB", float64(size)/float64(MB))
	}
	if size >= KB {
		return fmt.Sprintf("%.2fKB", float64(size)/float64(KB))
	}

	return fmt.Sprintf("%dB", size)
}

// 在 utils 模組中假設新增的函數
func FormatCount(count uint64) string {
    switch {
    case count >= 1_000_000_000:
        return fmt.Sprintf("%.2fG", float64(count)/1_000_000_000)
    case count >= 1_000_000:
        return fmt.Sprintf("%.2fM", float64(count)/1_000_000)
    case count >= 1_000:
        return fmt.Sprintf("%.2fK", float64(count)/1_000)
    default:
        return fmt.Sprintf("%d", count)
    }
}

// ParseSize parses size string with units (e.g., 4K, 64K, 1G)
func ParseSize(sizeStr string) (int64, error) {
	sizeStr = strings.ToUpper(sizeStr)
	var multiplier int64 = 1

	if strings.HasSuffix(sizeStr, "K") {
		multiplier = 1024
		sizeStr = sizeStr[:len(sizeStr)-1]
	} else if strings.HasSuffix(sizeStr, "KB") {
		multiplier = 1024
		sizeStr = sizeStr[:len(sizeStr)-2]
	} else if strings.HasSuffix(sizeStr, "M") {
		multiplier = 1024 * 1024
		sizeStr = sizeStr[:len(sizeStr)-1]
	} else if strings.HasSuffix(sizeStr, "MB") {
		multiplier = 1024 * 1024
		sizeStr = sizeStr[:len(sizeStr)-2]
	} else if strings.HasSuffix(sizeStr, "G") {
		multiplier = 1024 * 1024 * 1024
		sizeStr = sizeStr[:len(sizeStr)-1]
	} else if strings.HasSuffix(sizeStr, "GB") {
		multiplier = 1024 * 1024 * 1024
		sizeStr = sizeStr[:len(sizeStr)-2]
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return 0, err
	}

	return size * multiplier, nil
}

// NUMAInfo holds information about NUMA nodes
type NUMAInfo struct {
	NumNodes int
	NodeCPUs [][]int
}

// GetNUMAInfo retrieves NUMA node information (Linux-specific)
func GetNUMAInfo() (NUMAInfo, error) {
	info := NUMAInfo{
		NumNodes: 1,
		NodeCPUs: make([][]int, 0),
	}

	nodeDir := "/sys/devices/system/node"
	if _, err := os.Stat(nodeDir); os.IsNotExist(err) {
		cpus := make([]int, runtime.NumCPU())
		for i := 0; i < runtime.NumCPU(); i++ {
			cpus[i] = i
		}
		info.NodeCPUs = append(info.NodeCPUs, cpus)
		return info, nil
	}

	files, err := os.ReadDir(nodeDir)
	if err != nil {
		return info, err
	}

	for _, file := range files {
		if !file.IsDir() || !strings.HasPrefix(file.Name(), "node") {
			continue
		}

		nodeID, err := strconv.Atoi(strings.TrimPrefix(file.Name(), "node"))
		if err != nil {
			continue
		}

		if nodeID >= len(info.NodeCPUs) {
			newSize := nodeID + 1
			if len(info.NodeCPUs) < newSize {
				oldSize := len(info.NodeCPUs)
				info.NodeCPUs = append(info.NodeCPUs, make([][]int, newSize-oldSize)...)
			}
		}

		cpuList, err := os.ReadFile(filepath.Join(nodeDir, file.Name(), "cpulist"))
		if err != nil {
			continue
		}

		cpus := make([]int, 0)
		for _, segment := range strings.Split(strings.TrimSpace(string(cpuList)), ",") {
			if strings.Contains(segment, "-") {
				parts := strings.Split(segment, "-")
				if len(parts) != 2 {
					continue
				}
				start, err := strconv.Atoi(parts[0])
				if err != nil {
					continue
				}
				end, err := strconv.Atoi(parts[1])
				if err != nil {
					continue
				}
				for i := start; i <= end; i++ {
					cpus = append(cpus, i)
				}
			} else {
				cpu, err := strconv.Atoi(segment)
				if err != nil {
					continue
				}
				cpus = append(cpus, cpu)
			}
		}

		info.NodeCPUs[nodeID] = cpus
	}

	info.NumNodes = 0
	for i := range info.NodeCPUs {
		if len(info.NodeCPUs[i]) > 0 {
			info.NumNodes = i + 1
		}
	}

	return info, nil
}

// NewRand creates a new random number generator with the given seed
func NewRand(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed))
}

// GetCacheInfo retrieves L1, L2, and L3 cache sizes from the system
func GetCacheInfo() (config.CacheInfo, error) {
	cacheInfo := config.CacheInfo{}
	cacheDir := "/sys/devices/system/cpu/cpu0/cache"

	for i := 0; i <= 3; i++ {
		levelPath := filepath.Join(cacheDir, fmt.Sprintf("index%d/level", i))
		sizePath := filepath.Join(cacheDir, fmt.Sprintf("index%d/size", i))
		typePath := filepath.Join(cacheDir, fmt.Sprintf("index%d/type", i))

		levelData, err := os.ReadFile(levelPath)
		if err != nil {
			continue
		}
		level, err := strconv.Atoi(strings.TrimSpace(string(levelData)))
		if err != nil {
			continue
		}

		typeData, err := os.ReadFile(typePath)
		if err != nil {
			continue
		}
		cacheType := strings.TrimSpace(string(typeData))
		if cacheType != "Data" && cacheType != "Unified" {
			continue
		}

		sizeData, err := os.ReadFile(sizePath)
		if err != nil {
			continue
		}
		sizeStr := strings.TrimSpace(string(sizeData))
		size, err := ParseCacheSize(sizeStr)
		if err != nil {
			continue
		}

		switch level {
		case 1:
			cacheInfo.L1Size = size
		case 2:
			cacheInfo.L2Size = size
		case 3:
			cacheInfo.L3Size = size
		}
	}

	if cacheInfo.L1Size == 0 {
		cacheInfo.L1Size = 32 * 1024
	}
	if cacheInfo.L2Size == 0 {
		cacheInfo.L2Size = 256 * 1024
	}
	if cacheInfo.L3Size == 0 {
		cacheInfo.L3Size = 0
	}

	return cacheInfo, nil
}

// ParseCacheSize converts cache size string (e.g., "32K", "4M") to bytes
func ParseCacheSize(sizeStr string) (int64, error) {
	sizeStr = strings.TrimSpace(sizeStr)
	if len(sizeStr) == 0 {
		return 0, fmt.Errorf("empty cache size string")
	}

	unit := sizeStr[len(sizeStr)-1:]
	valueStr := sizeStr[:len(sizeStr)-1]
	value, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid cache size value: %v", err)
	}

	switch strings.ToUpper(unit) {
	case "K":
		return value * 1024, nil
	case "M":
		return value * 1024 * 1024, nil
	case "G":
		return value * 1024 * 1024 * 1024, nil
	default:
		return 0, fmt.Errorf("unknown cache size unit: %s", unit)
	}
}

// GetDiskSize returns the total size of the specified disk device (in bytes)
func GetDiskSize(devicePath string) (int64, error) {
	file, err := os.OpenFile(devicePath, os.O_RDONLY, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to open device %s: %v", devicePath, err)
	}
	defer file.Close()

	var size int64
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		file.Fd(),
		0x80081272, // BLKGETSIZE64
		uintptr(unsafe.Pointer(&size)),
	)
	if errno != 0 {
		return 0, fmt.Errorf("ioctl BLKGETSIZE64 failed for %s: %v", devicePath, errno)
	}

	return size, nil
}
