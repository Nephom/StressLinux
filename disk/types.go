// disk/types.go
package disk

// DiskTestConfig holds disk test configuration
type DiskTestConfig struct {
    MountPoints []string
    FileSize    int64
    TestMode    string
    BlockSize   int64
    NumFiles    int
}

// DiskResult holds the results of disk tests
/*type DiskResult struct {
    Performances []struct {
        MountPoint string
        Mode       string
        ReadSpeed  float64
        WriteSpeed float64
    }
    Errors []string
}
*/
