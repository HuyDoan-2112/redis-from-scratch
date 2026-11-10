# BGSAVE benchmark 2026-11-09 at commit e8edb44-dirty

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
| mini_redis | no writes | 304 | 3.6 | 0.94 | 247 | 3.0 | 81% |
| mini_redis | SET, 50 clients, pipeline 1 | 319 | 5.3 | 1.05 | 272 | 4.8 | 85% |
| mini_redis + gc.freeze() | no writes | 304 | 4.7 | 0.94 | 247 | 3.9 | 81% |
| mini_redis + gc.freeze() | SET, 50 clients, pipeline 1 | 319 | 5.6 | 1.04 | 272 | 5.4 | 85% |
| real Redis | no writes | 170 | 2.9 | 0.54 | 1 | 2.7 | 1% |
| real Redis | SET, 50 clients, pipeline 1 | 199 | 24.0 | 0.62 | 32 | 23.9 | 16% |

## All runs

| Server | Writes during save | Run | Memory (MB) | Fork pause (ms) | Save (s) | Copy-on-write peak (MB) | Slowest PING (ms) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| mini_redis | no writes | 1 | 304 | 3.5 | 0.93 | 247 | 2.8 |
| mini_redis | no writes | 2 | 304 | 3.6 | 0.94 | 247 | 3.0 |
| mini_redis | no writes | 3 | 304 | 4.3 | 0.98 | 247 | 4.0 |
| mini_redis | SET, 50 clients, pipeline 1 | 1 | 318 | 5.3 | 1.08 | 272 | 16.4 |
| mini_redis | SET, 50 clients, pipeline 1 | 2 | 319 | 5.7 | 0.99 | 272 | 4.5 |
| mini_redis | SET, 50 clients, pipeline 1 | 3 | 319 | 5.3 | 1.05 | 272 | 4.8 |
| mini_redis + gc.freeze() | no writes | 1 | 304 | 4.7 | 0.93 | 247 | 3.9 |
| mini_redis + gc.freeze() | no writes | 2 | 304 | 4.2 | 0.94 | 247 | 3.8 |
| mini_redis + gc.freeze() | no writes | 3 | 304 | 12.4 | 1.08 | 247 | 12.5 |
| mini_redis + gc.freeze() | SET, 50 clients, pipeline 1 | 1 | 319 | 5.5 | 1.04 | 272 | 5.4 |
| mini_redis + gc.freeze() | SET, 50 clients, pipeline 1 | 2 | 319 | 5.8 | 0.99 | 272 | 5.6 |
| mini_redis + gc.freeze() | SET, 50 clients, pipeline 1 | 3 | 319 | 5.6 | 1.14 | 272 | 5.0 |
| real Redis | no writes | 1 | 170 | 2.9 | 0.54 | 1 | 2.7 |
| real Redis | no writes | 2 | 170 | 2.8 | 0.52 | 1 | 2.7 |
| real Redis | no writes | 3 | 170 | 2.9 | 0.55 | 1 | 2.0 |
| real Redis | SET, 50 clients, pipeline 1 | 1 | 198 | 24.0 | 0.64 | 32 | 23.9 |
| real Redis | SET, 50 clients, pipeline 1 | 2 | 199 | 18.2 | 0.59 | 32 | 18.0 |
| real Redis | SET, 50 clients, pipeline 1 | 3 | 199 | 34.5 | 0.62 | 32 | 33.6 |
