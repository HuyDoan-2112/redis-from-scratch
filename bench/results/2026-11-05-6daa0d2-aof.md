# AOF benchmark 2026-11-05 at commit 6daa0d2

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
| 1 | always | 14,083 | 15,635 |
| 1 | everysec | 57,078 | 97,847 |
| 1 | no | 60,277 | 97,561 |
| 16 | always | 76,046 | 196,078 |
| 16 | everysec | 131,752 | 909,091 |
| 16 | no | 126,904 | 862,069 |

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
| no writes | 0.2 | 4.4 | 3.07 | 0.0 | 31.1 |
| SET, 50 clients, pipeline 1 | 2.0 | 6.6 | 3.41 | 37.9 | 70.8 |
| SET, 50 clients, pipeline 16 | 11.9 | 16.7 | 3.90 | 76.1 | 110.0 |

## All runs

| Writes during rewrite | Run | Normal PING (ms) | Fork pause (ms) | Rewrite (s) | Rewrite buffer (MB) | Finish pause (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| no writes | 1 | 0.2 | 8.3 | 3.04 | 0.0 | 31.1 |
| no writes | 2 | 0.4 | 4.0 | 3.07 | 0.0 | 27.8 |
| no writes | 3 | 0.2 | 4.4 | 3.18 | 0.0 | 31.7 |
| SET, 50 clients, pipeline 1 | 1 | 2.0 | 6.0 | 3.72 | 40.2 | 70.8 |
| SET, 50 clients, pipeline 1 | 2 | 2.0 | 6.6 | 3.41 | 37.6 | 68.0 |
| SET, 50 clients, pipeline 1 | 3 | 1.6 | 7.2 | 3.41 | 37.9 | 92.9 |
| SET, 50 clients, pipeline 16 | 1 | 13.1 | 16.6 | 3.91 | 76.3 | 110.0 |
| SET, 50 clients, pipeline 16 | 2 | 11.9 | 18.6 | 3.53 | 75.9 | 102.2 |
| SET, 50 clients, pipeline 16 | 3 | 9.5 | 16.7 | 3.90 | 76.1 | 146.6 |
