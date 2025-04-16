package main

import (
    "crypto/md5"
    "encoding/hex"
    "flag"
    "fmt"
    "io"
    "log"
    "net"
    "os"
    "strings"
    "sync"
    "time"
    "stress/config"
    "stress/utils"
    "golang.org/x/sys/unix"
    "math/rand"
)

type Client struct {
    ServerIPs []string
    Port      string
    Debug     bool
    Stats     *config.PerformanceStats
    mu        sync.Mutex
    loggers   map[string]*log.Logger
}

func NewClient(serverIPs []string, port string, debug bool) *Client {
    return &Client{
        ServerIPs: serverIPs,
        Port:      port,
        Debug:     debug,
        Stats:     &config.PerformanceStats{},
        loggers:   make(map[string]*log.Logger),
    }
}

func (c *Client) logMessage(serverIP, message string) error {
    timestamp := time.Now().Format("2006-01-02 15:04:05")
    logEntry := fmt.Sprintf("%s | %s", timestamp, message)

    logger, exists := c.loggers[serverIP]
    if !exists {
        logFile := fmt.Sprintf("client_%s.log", strings.ReplaceAll(serverIP, ".", "_"))
        f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
        if err != nil {
            return fmt.Errorf("failed to open log file for %s: %v", serverIP, err)
        }
        logger = log.New(f, "", 0)
        c.loggers[serverIP] = logger
    }

    logger.Println(logEntry)
    if c.Debug {
        fmt.Printf("[%s] %s\n", serverIP, logEntry)
    }
    return nil
}

func (c *Client) optimizeTCP(conn *net.TCPConn) error {
    file, err := conn.File()
    if err != nil {
        return err
    }
    defer file.Close()

    // Set TCP_NODELAY
    if err := unix.SetsockoptInt(int(file.Fd()), unix.IPPROTO_TCP, unix.TCP_NODELAY, 1); err != nil {
        return fmt.Errorf("failed to set TCP_NODELAY: %v", err)
    }
    // Set large send/receive buffers
    if err := unix.SetsockoptInt(int(file.Fd()), unix.SOL_SOCKET, unix.SO_SNDBUF, 1024*1024*32); err != nil {
        return fmt.Errorf("failed to set SO_SNDBUF: %v", err)
    }
    if err := unix.SetsockoptInt(int(file.Fd()), unix.SOL_SOCKET, unix.SO_RCVBUF, 1024*1024*32); err != nil {
        return fmt.Errorf("failed to set SO_RCVBUF: %v", err)
    }
    return nil
}

func (c *Client) connectWithRetry(serverIP string) (*net.TCPConn, error) {
    addr := fmt.Sprintf("%s:%s", serverIP, c.Port)
    deadline := time.Now().Add(time.Minute)
    var lastErr error

    for time.Now().Before(deadline) {
        conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
        if err == nil {
            return conn.(*net.TCPConn), nil
        }
        lastErr = err
        c.logMessage(serverIP, fmt.Sprintf("WARN: Connection attempt failed: %v, retrying...", err))
        time.Sleep(time.Second)
    }

    c.logMessage(serverIP, fmt.Sprintf("ERROR: Failed to connect after 1 minute: %v", lastErr))
    return nil, fmt.Errorf("connection timeout")
}

func (c *Client) runTest(serverIP string) {
    conn, err := c.connectWithRetry(serverIP)
    if err != nil {
        return
    }
    defer conn.Close()

    if err := c.optimizeTCP(conn); err != nil {
        c.logMessage(serverIP, fmt.Sprintf("ERROR: Failed to optimize TCP: %v", err))
        return
    }

    c.logMessage(serverIP, "INFO: Connected to server")
    rng := utils.NewRand(time.Now().UnixNano())
    buffer := make([]byte, 1024*1024) // 1MB buffer
    var totalBytes int64
    start := time.Now()
    md5Hash := md5.New()

    // Send data for 10 seconds
    endTime := time.Now().Add(10 * time.Second)
    for time.Now().Before(endTime) {
        _, err := rng.Read(buffer)
        if err != nil {
            c.logMessage(serverIP, fmt.Sprintf("ERROR: Failed to generate random data: %v", err))
            return
        }
        n, err := conn.Write(buffer)
        if err != nil {
            c.logMessage(serverIP, fmt.Sprintf("ERROR: Write error: %v", err))
            return
        }
        totalBytes += int64(n)
        md5Hash.Write(buffer[:n])

        // Check packet stats
        if err := c.checkPacketStats(serverIP, conn); err != nil {
            c.logMessage(serverIP, fmt.Sprintf("WARN: %v", err))
        }
    }

    // Send MD5
    clientMD5 := hex.EncodeToString(md5Hash.Sum(nil))
    _, err = conn.Write([]byte(clientMD5))
    if err != nil {
        c.logMessage(serverIP, fmt.Sprintf("ERROR: Failed to send MD5: %v", err))
        return
    }

    duration := time.Since(start).Seconds()
    mbps := (float64(totalBytes) / 1024 / 1024) / duration * 8 // Convert to Mbps

    c.mu.Lock()
    c.Stats.Lock()
    c.Stats.Network = append(c.Stats.Network, config.NetworkPerformance{
        Mbps: mbps,
    })
    c.Stats.Unlock()
    c.mu.Unlock()

    c.logMessage(serverIP, fmt.Sprintf("INFO: Transfer rate: %.2f Mbps, Total: %s", mbps, utils.FormatSize(totalBytes)))
}

func (c *Client) checkPacketStats(serverIP string, conn *net.TCPConn) error {
    file, err := conn.File()
    if err != nil {
        return err
    }
    defer file.Close()

    var stats unix.TcpInfo
    statsSize := uint32(unsafe.Sizeof(stats))
    err = unix.GetsockoptTCPInfo(int(file.Fd()), &stats, &statsSize)
    if err != nil {
        return fmt.Errorf("failed to get TCP stats: %v", err)
    }

    if stats.Lost > 0 {
        return fmt.Errorf("packet loss detected: %d packets", stats.Lost)
    }
    if stats.Retrans > 0 {
        return fmt.Errorf("packet retransmissions: %d", stats.Retrans)
    }
    return nil
}

func (c *Client) getLocalIPs() ([]string, error) {
    var ips []string
    addrs, err := net.InterfaceAddrs()
    if err != nil {
        return nil, err
    }
    for _, addr := range addrs {
        ipNet, ok := addr.(*net.IPNet)
        if ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
            ips = append(ips, ipNet.IP.String())
        }
    }
    if len(ips) == 0 {
        return nil, fmt.Errorf("no suitable local IPs found")
    }
    return ips, nil
}

func (c *Client) Start() {
    // Configure network optimizations
    localIPs, err := c.getLocalIPs()
    if err != nil {
        c.logMessage("all", fmt.Sprintf("ERROR: Failed to get local IPs for network configuration: %v", err))
        return
    }
    if err := utils.ConfigureNetwork(localIPs, c.Debug); err != nil {
        c.logMessage("all", fmt.Sprintf("WARN: Network configuration issues: %v", err))
    }

    var wg sync.WaitGroup
    for _, ip := range c.ServerIPs {
        wg.Add(1)
        go func(ip string) {
            defer wg.Done()
            c.runTest(ip)
        }(ip)
    }
    wg.Wait()
}

func main() {
    ipList := flag.String("c", "", "Comma-separated list of server IPs")
    debug := flag.Bool("debug", false, "Enable debug logging")
    flag.Parse()

    if *ipList == "" {
        log.Fatal("Server IPs must be specified with -c")
    }

    serverIPs := strings.Split(*ipList, ",")
    client := NewClient(serverIPs, "5001", *debug)
    client.Start()
}
