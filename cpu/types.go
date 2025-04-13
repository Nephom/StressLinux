package cpu

// CPUConfig holds configuration for CPU tests
type CPUConfig struct {
	NumCores int
	Debug    bool
}

// CPUResult holds the results of CPU tests
type CPUResult struct {
	GFLOPS float64
	Errors []string
}
