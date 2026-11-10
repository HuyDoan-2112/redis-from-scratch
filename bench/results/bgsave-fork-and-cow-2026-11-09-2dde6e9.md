# BGSAVE benchmark 2026-11-09 at commit 2dde6e9

Median of 3 run(s).

- CPU: AMD Ryzen 7 7800X3D 8-Core Processor, 16 cores
- Kernel: 6.6.87.2-microsoft-standard-WSL2
- Python: 3.11.17
- Redis server v=8.10.3
- RDB directory file system: ext2/ext3

## Fork pause and copy-on-write memory

1,000,000 keys with 100-byte values. During the save, `redis-benchmark -t set -r 1000000 -d 100` writes with the load shown.
A probe client sends one PING at a time. Real Redis runs with `rdbcompression no`.

- Memory: the server's RSS just before BGSAVE.
- Fork pause: how long the BGSAVE reply took. fork() copies the page tables, so it grows with memory.
- Save: from BGSAVE until the child exited.
- Copy-on-write peak: the child's peak Private_Dirty, the memory the two processes no longer share.
- Copy-on-write %: the peak as a share of Memory.
- Slowest PING: the slowest probe PING from BGSAVE until the child exited.

| Server | Writes during save | Memory (MB) | Fork pause (ms) | Save (s) | Copy-on-write peak (MB) | Slowest PING (ms) | Copy-on-write % |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| mini_redis | no writes | 304 | 5.6 | 1.07 | 247 | 5.6 | 81% |
| mini_redis | SET, 50 clients, pipeline 1 | 319 | 5.6 | 1.12 | 272 | 16.8 | 85% |
| mini_redis + gc.freeze() | no writes | 304 | 4.2 | 0.96 | 247 | 4.1 | 81% |
| mini_redis + gc.freeze() | SET, 50 clients, pipeline 1 | 319 | 5.9 | 1.05 | 272 | 5.3 | 85% |
| real Redis | no writes | 170 | 7.1 | 0.55 | 1 | 6.1 | 1% |
| real Redis | SET, 50 clients, pipeline 1 | 199 | 29.8 | 0.65 | 32 | 28.9 | 16% |

## All runs

| Server | Writes during save | Run | Memory (MB) | Fork pause (ms) | Save (s) | Copy-on-write peak (MB) | Slowest PING (ms) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| mini_redis | no writes | 1 | 304 | 5.6 | 0.99 | 247 | 5.6 |
| mini_redis | no writes | 2 | 304 | 4.0 | 1.07 | 247 | 3.5 |
| mini_redis | no writes | 3 | 304 | 7.0 | 1.22 | 247 | 7.2 |
| mini_redis | SET, 50 clients, pipeline 1 | 1 | 319 | 6.5 | 1.06 | 272 | 16.8 |
| mini_redis | SET, 50 clients, pipeline 1 | 2 | 318 | 5.6 | 1.14 | 272 | 5.0 |
| mini_redis | SET, 50 clients, pipeline 1 | 3 | 319 | 5.0 | 1.12 | 273 | 23.1 |
| mini_redis + gc.freeze() | no writes | 1 | 304 | 4.2 | 1.03 | 247 | 3.9 |
| mini_redis + gc.freeze() | no writes | 2 | 304 | 4.1 | 0.96 | 247 | 4.3 |
| mini_redis + gc.freeze() | no writes | 3 | 304 | 4.7 | 0.95 | 247 | 4.1 |
| mini_redis + gc.freeze() | SET, 50 clients, pipeline 1 | 1 | 319 | 5.5 | 1.24 | 272 | 5.1 |
| mini_redis + gc.freeze() | SET, 50 clients, pipeline 1 | 2 | 318 | 5.9 | 1.05 | 272 | 5.3 |
| mini_redis + gc.freeze() | SET, 50 clients, pipeline 1 | 3 | 319 | 6.3 | 1.00 | 272 | 5.4 |
| real Redis | no writes | 1 | 170 | 7.1 | 0.53 | 1 | 6.1 |
| real Redis | no writes | 2 | 170 | 8.6 | 0.56 | 1 | 7.6 |
| real Redis | no writes | 3 | 170 | 3.2 | 0.55 | 1 | 2.9 |
| real Redis | SET, 50 clients, pipeline 1 | 1 | 199 | 27.8 | 0.65 | 32 | 27.6 |
| real Redis | SET, 50 clients, pipeline 1 | 2 | 199 | 42.8 | 0.64 | 32 | 41.9 |
| real Redis | SET, 50 clients, pipeline 1 | 3 | 197 | 29.8 | 0.66 | 32 | 28.9 |
