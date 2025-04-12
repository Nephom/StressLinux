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
)

// Logger function to handle both console output and file logging
func LogMessage(message string, debug bool) {
    timestamp := time.Now().Format("2006-01-02 15:04:05")
    logEntry := fmt.Sprintf("%s | %s", timestamp, message)

    // Always log to file
    f, err := os.OpenFile("stress.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err == nil {
        defer f.Close()
        logger := log.New(f, "", 0)
        logger.Println(logEntry)
    }

    // Print to console only in debug mode or if explicitly requested
    if debug {
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
        NumNodes: 1, // Default to 1 node if detection fails
        NodeCPUs: make([][]int, 0),
    }

    // Check if /sys/devices/system/node exists
    nodeDir := "/sys/devices/system/node"
    if _, err := os.Stat(nodeDir); os.IsNotExist(err) {
        // NUMA information not available, create default mapping
        cpus := make([]int, runtime.NumCPU())
        for i := 0; i < runtime.NumCPU(); i++ {
            cpus[i] = i
        }
        info.NodeCPUs = append(info.NodeCPUs, cpus)
        return info, nil
    }

    // Read node directories
    files, err := os.ReadDir(nodeDir)
    if err != nil {
        return info, err
    }

    for _, file := range files {
        if !file.IsDir() || !strings.HasPrefix(file.Name(), "node") {
            continue
        }

        // Parse node ID
        nodeID, err := strconv.Atoi(strings.TrimPrefix(file.Name(), "node"))
        if err != nil {
            continue
        }

        // Ensure we have enough capacity in our slice
        if nodeID >= len(info.NodeCPUs) {
            newSize := nodeID + 1
            if len(info.NodeCPUs) < newSize {
                oldSize := len(info.NodeCPUs)
                info.NodeCPUs = append(info.NodeCPUs, make([][]int, newSize-oldSize)...)
            }
        }

        // Read CPU list for this node
        cpuList, err := os.ReadFile(filepath.Join(nodeDir, file.Name(), "cpulist"))
        if err != nil {
            continue
        }

        // Parse CPU list (format can be like "0-3,7,9-11")
        cpus := make([]int, 0)
        for _, segment := range strings.Split(strings.TrimSpace(string(cpuList)), ",") {
            if strings.Contains(segment, "-") {
                // Range of CPUs
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
                // Single CPU
                cpu, err := strconv.Atoi(segment)
                if err != nil {
                    continue
                }
                cpus = append(cpus, cpu)
            }
        }

        info.NodeCPUs[nodeID] = cpus
    }

    // Set the actual number of NUMA nodes found
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
