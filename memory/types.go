// memory/types.go
package memory

// MemoryConfig holds configuration for memory tests
type MemoryConfig struct {
	UsagePercent float64
	Debug        bool
}

// MemoryResult holds the results of memory tests
type MemoryResult struct {
	ReadSpeed  float64
	WriteSpeed float64
	Errors     []string
}
