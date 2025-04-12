package rawdisk

// RawDiskTestConfig holds configuration for raw disk stress tests
type RawDiskTestConfig struct {
	DevicePaths []string
	TestSize int64
	BlockSize int64
	TestMode string
	StartOffset int64
}
