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
)

type Server struct {
    IPs       []string
    Port      string
    Debug     bool
    Stats     *config.PerformanceStats
    mu        sync.Mutex
    loggers   map[string]*log.Logger
}

func NewServer(ips []string, port string, debug bool) *Server {
    return &Server{
        IPs:     ips,
        Port:    port,
        Debug:   debug,
        Stats:   &config.PerformanceStats{},
        loggers: make(map[string]*log.Logger),
    }
}

func (s *Server) logMessage(ip, message string) error {
    timestamp := time.Now().Format("2006-01-02 15:04:05")
    logEntry := fmt.Sprintf("%s | %s", timestamp, message)

    logger, exists := s.loggers[ip]
    if !exists {
        logFile := fmt.Sprintf("server_%s.log", strings.ReplaceAll(ip, ".", "_"))
        f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
        if err != nil {
            return fmt.Errorf("failed to open log file for %s: %v", ip, err)
        }
        logger = log.New(f, "", 0)
        s.loggers[ip] = logger
    }

    logger.Println(logEntry)
    if s.Debug {
        fmt.Printf("[%s] %s\n", ip, logEntry)
    }
    return nil
}

func (s *Server) optimizeTCP(conn *net.TCPConn) error {
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

func (s *Server) handleConnection(conn *net.TCPConn, ip string) {
    defer conn.Close()
    if err := s.optimizeTCP(conn); err != nil {
        s.logMessage(ip, fmt.Sprintf("ERROR: Failed to optimize TCP: %v", err))
        return
    }

    clientAddr := conn.RemoteAddr().String()
    s.logMessage(ip, fmt.Sprintf("INFO: Connection from %s", clientAddr))

    buffer := make([]byte, 1024*1024) // 1MB buffer
    var totalBytes int64
    start := time.Now()
    md5Hash := md5.New()

    for {
        n, err := conn.Read(buffer)
        if err != nil {
            if err != io.EOF {
                s.logMessage(ip, fmt.Sprintf("ERROR: Read error: %v", err))
            }
            break
        }
        totalBytes += int64(n)
        md5Hash.Write(buffer[:n])

        // Check for packet discards
        if err := s.checkPacketStats(ip, conn); err != nil {
            s.logMessage(ip, fmt.Sprintf("WARN: %v", err))
        }
    }

    duration := time.Since(start).Seconds()
    mbps := (float64(totalBytes) / 1024 / 1024) / duration * 8 // Convert to Mbps
    receivedMD5 := hex.EncodeToString(md5Hash.Sum(nil))

    // Receive client MD5
    md5Buffer := make([]byte, 32)
    n, err := conn.Read(md5Buffer)
    if err != nil || n != 32 {
        s.logMessage(ip, fmt.Sprintf("ERROR: Failed to receive MD5: %v", err))
        return
    }
    clientMD5 := string(md5Buffer)

    if receivedMD5 != clientMD5 {
        s.logMessage(ip, "ERROR: MD5 mismatch")
    } else {
        s.logMessage(ip, "INFO: MD5 verification passed")
    }

    s.mu.Lock()
    s.Stats.Lock()
    s.Stats.Network = append(s.Stats.Network, config.NetworkPerformance{
        Mbps: mbps,
    })
    s.Stats.Unlock()
    s.mu.Unlock()

    s.logMessage(ip, fmt.Sprintf("INFO: Transfer rate: %.2f Mbps, Total: %s", mbps, utils.FormatSize(totalBytes)))
}

func (s *Server) checkPacketStats(ip string, conn *net.TCPConn) error {
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

func (s *Server) Start() {
    // Configure network optimizations
    if err := utils.ConfigureNetwork(s.IPs, s.Debug); err != nil {
        s.logMessage("all", fmt.Sprintf("WARN: Network configuration issues: %v", err))
    }

    var wg sync.WaitGroup
    for _, ip := range s.IPs {
        wg.Add(1)
        go func(ip string) {
            defer wg.Done()
            addr := fmt.Sprintf("%s:%s", ip, s.Port)
            listener, err := net.Listen("tcp", addr)
            if err != nil {
                s.logMessage(ip, fmt.Sprintf("ERROR: Failed to listen on %s: %v", addr, err))
                return
            }
            defer listener.Close()

            s.logMessage(ip, fmt.Sprintf("INFO: Listening on %s", addr))
            for {
                conn, err := listener.Accept()
                if err != nil {
                    s.logMessage(ip, fmt.Sprintf("ERROR: Accept error: %v", err))
                    continue
                }
                go s.handleConnection(conn.(*net.TCPConn), ip)
            }
        }(ip)
    }
    wg.Wait()
}

func getLocalIPs() ([]string, error) {
    var ips []string
    ifaces, err := net.Interfaces()
    if err != nil {
        return nil, err
    }
    for _, iface := range ifaces {
        addrs, err := iface.Addrs()
        if err != nil {
            continue
        }
        for _, addr := range addrs {
            ipNet, ok := addr.(*net.IPNet)
            if ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
                ips = append(ips, ipNet.IP.String())
            }
        }
    }
    return ips, nil
}

func main() {
    ipList := flag.String("s", "", "Comma-separated list of server IPs")
    debug := flag.Bool("debug", false, "Enable debug logging")
    flag.Parse()

    var ips []string
    if *ipList != "" {
        ips = strings.Split(*ipList, ",")
    } else {
        var err error
        ips, err = getLocalIPs()
        if err != nil {
            log.Fatalf("Failed to get local IPs: %v", err)
        }
    }

    server := NewServer(ips, "5001", *debug)
    server.Start()
}
