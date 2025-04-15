# System Stress Test Tool

A high-performance system stress testing tool written in Go, designed for testing CPU, memory, filesystem I/O, and raw disk performance.

## Features

* CPU load testing with adjustable intensity (High, Low, Default)
* Memory usage stress test with fine-grained percentage control
* Filesystem performance test on specified mount points
* Raw disk read/write test with configurable block size, test size, offset, and mode
* Configurable test durations and resource usage
* Sequential and random modes for I/O testing
* Detailed performance statistics, including operation counts
* Debug mode for detailed step logging
* System resource information display

## Installation

```
git clone https://github.com/yourname/StressLinux.git
cd StressLinux
go build -o stress
```

[Optional] If you don't set -d parameter and want to check debug message.
```
echo "{\"debug\": true}" > config.json
```

## Usage

```
./stress [options]
```

## Available Options

| Flag | Description |
|------|-------------|
| `-cpu` | Enable CPU stress testing |
| `-cpu-cores` | Number of CPU cores to stress (0 means all cores, default: 0) |
| `-cpu-load` | CPU load level: High (2), Low (1), or Default (0) |
| `-memory` | Memory testing percentage (0.1-9.9 for 1%-99% of total memory, e.g., 1.5 for 15%, default: 0) |
| `-l` | Comma-separated mount points to test (e.g., /mnt/disk1,/mnt/disk2) |
| `-disk` | Raw disk devices to test (e.g., /dev/sdb, /dev/nvme0n1) |
| `-diskoffset` | Start offset from the beginning of the raw device (e.g., 1G, 100M, supports K, M, G units, default: 1G) |
| `-mode` | Filesystem test mode: sequential, random, or both (default: both) |
| `-size` | File size for mount point testing (e.g., 10M, 1G, supports K, M, G units, default: 10M) |
| `-block` | Comma-separated block sizes for disk operations (e.g., 4K,1M, supports K, M, G units, default: 4K) |
| `-duration` | Total duration of the test (e.g., 30s, 5m, 1h, default: 10m) |
| `-d` | Enable debug mode |
| `-h` | Show help |
| `-list` or `-print` | Show system resource information |
| `-numa` | NUMA node to stress (e.g., 0 or 1; default -1 means all nodes) |

**Note:** At least one of `-cpu`, `-memory`, `-l`, or `-disk` must be specified.

## Example Usages

Test CPU with high load and memory:
```
./stress -cpu -cpu-load High -memory 2.5 -duration 5m
```

Test raw disk with custom block size and offset:
```
./stress -disk /dev/nvme0n1 -diskblock 4K -disksize 200M -diskoffset 1G
```

Filesystem I/O test on mount points:
```
./stress -l /mnt/ssd1,/mnt/ssd2 -size 100M -block 1M,4K
```

Enable debug logs:
```
./stress -cpu -d
```

## License

MIT License. See LICENSE for details.

## Contributing

Feel free to fork and submit pull requests. Suggestions and improvements are welcome!
