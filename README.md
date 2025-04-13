# System Stress Test Tool

A high-performance system stress testing tool written in Go, designed for testing CPU, memory, filesystem I/O, and raw disk performance.

## Features

- CPU load testing  
- Memory usage stress test  
- Filesystem performance test (mount points)  
- Raw disk read/write test (with block size, offset, and mode control)  
- Configurable test durations and resource usage  
- Sequential and random modes for I/O testing  
- Debug mode for detailed step logging  

## Installation

```bash
git clone https://github.com/yourname/StressLinux.git
cd StressLinux
go build -o stress

[Optional] If you don't set -d parameter and want to check debug message.
echo "{\"debug\": true} > config.json
```

## Usage

```bash
./stress [options]
```

### Available Options

| Flag         | Description                                                                 |
|--------------|-----------------------------------------------------------------------------|
| `-cpu`       | Enable CPU stress testing                                                   |
| `-cpu-cores`       | Number of CPU cores to stress (0 means all cores)                                                   |
| `-memory`    | Memory testing percentage (1-9 for 10%-90%, 1.5 for 15%, etc.)              |
| `-l`         | Comma-separated mount points to test (e.g., `/mnt/disk1,/mnt/disk2`)        |
| `-disk`      | Raw disk devices to test (e.g., `/dev/sdb`, `/dev/nvme0n1`)                 |
| `-diskblock` | Block size (bytes) for raw disk testing (default: `4096`)                   |
| `-disksize`  | Amount of bytes to test on each raw disk device (default: `104857600`)      |
| `-diskoffset`| Start offset (bytes) from the beginning of the device (default: `1GB`)      |
| `-diskmode`  | Raw disk test mode: `sequential`, `random`, or `both` (default: `both`)     |
| `-mode`      | Filesystem test mode: `sequential`, `random`, or `both` (default: `both`)   |
| `-size`      | File size for mount point testing (supports `K`, `M`, `G` units, default: `10MB`) |
| `-block`     | Comma-separated block sizes for disk ops (e.g., `4K,1M`, default: `4K`)     |
| `-duration`  | Total duration of the test (e.g., `30s`, `5m`, `1h`, default: `10m`)         |
| `-d`         | Enable debug mode                                                           |
| `-h`         | Show help                                                                   |
| `-list` or `-print`         | Show System resource info                                                                   |

> **Note:** At least one of `-cpu`, `-memory`, `-l`, or `-disk` must be specified.

## Example Usages

Test CPU and memory:

```bash
./stress -cpu -memory 2.5 -duration 5m
```

Test raw disk with custom block size and offset:

```bash
./stress -disk /dev/nvme0n1 -diskblock 4096 -disksize 209715200 -diskoffset 1073741824
```

Filesystem I/O test on mount points:

```bash
./stress -l /mnt/ssd1,/mnt/ssd2 -size 100MB -block 1M,4K
```

Enable debug logs:

```bash
./stress -cpu -d
```

## License

MIT License. See [LICENSE](./LICENSE) for details.

## Contributing

Feel free to fork and submit pull requests. Suggestions and improvements are welcome!
