# AOF benchmark 2026-11-09 at commit 45fe870

Median of 3 run(s).

- CPU: AMD Ryzen 7 7800X3D 8-Core Processor, 16 cores
- Kernel: 6.6.87.2-microsoft-standard-WSL2
- Python: 3.11.17
- Redis server v=8.10.3
- AOF directory file system: ext2/ext3

## SET throughput per fsync policy

Command: `redis-benchmark -t set -n 100000 -c 50 -P PIPELINE`, in ops/sec.

| Pipeline | appendfsync | mini_redis | real Redis |
| ---: | --- | ---: | ---: |
| 1 | always | 14,808 | 15,843 |
| 1 | everysec | 60,350 | 98,912 |
| 1 | no | 61,690 | 100,806 |
| 16 | always | 77,101 | 210,084 |
| 16 | everysec | 131,752 | 961,538 |
| 16 | no | 121,507 | 970,874 |

## BGREWRITEAOF pauses

1,000,000 keys with 100-byte values, appendfsync everysec. During the rewrite,
`redis-benchmark -t set -r 1000000 -d 100` writes with the load shown. A probe client
sends one PING at a time.

- Normal PING: the slowest PING in the second before BGREWRITEAOF.
- Fork pause: how long the BGREWRITEAOF reply took.
- Rewrite buffer: writes made while the child ran, which the parent writes to the new file.
- Finish pause: the slowest PING in the 2 seconds before the new file was swapped in.

| Writes during rewrite | Normal PING (ms) | Fork pause (ms) | Rewrite (s) | Rewrite buffer (MB) | Finish pause (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| no writes | 0.2 | 4.1 | 2.96 | 0.0 | 8.7 |
| SET, 50 clients, pipeline 1 | 1.7 | 6.5 | 3.31 | 39.6 | 36.6 |
| SET, 50 clients, pipeline 16 | 9.2 | 14.1 | 3.75 | 80.3 | 86.8 |

## All runs

| Writes during rewrite | Run | Normal PING (ms) | Fork pause (ms) | Rewrite (s) | Rewrite buffer (MB) | Finish pause (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| no writes | 1 | 0.2 | 3.5 | 3.05 | 0.0 | 8.7 |
| no writes | 2 | 0.2 | 4.1 | 2.85 | 0.0 | 12.2 |
| no writes | 3 | 0.2 | 4.3 | 2.96 | 0.0 | 3.4 |
| SET, 50 clients, pipeline 1 | 1 | 1.7 | 6.2 | 3.31 | 40.2 | 36.8 |
| SET, 50 clients, pipeline 1 | 2 | 2.2 | 8.0 | 3.21 | 38.6 | 36.6 |
| SET, 50 clients, pipeline 1 | 3 | 1.5 | 6.5 | 3.33 | 39.6 | 27.3 |
| SET, 50 clients, pipeline 16 | 1 | 10.1 | 14.1 | 3.75 | 79.7 | 86.8 |
| SET, 50 clients, pipeline 16 | 2 | 9.2 | 13.9 | 3.71 | 80.8 | 84.7 |
| SET, 50 clients, pipeline 16 | 3 | 9.1 | 16.8 | 3.80 | 80.3 | 89.2 |
