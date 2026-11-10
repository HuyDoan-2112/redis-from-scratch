# Benchmark 2026-10-26 at commit d225a7f

Command: `redis-benchmark -t set,get -n 100000 -c CLIENTS -P PIPELINE`, median of 3 run(s), in ops/sec.
"old thread-per-client" is commit 12e9de6, with its listen backlog raised from 5 to 511 like the current code. AOF on means fsync every second.

- CPU: AMD Ryzen 7 7800X3D 8-Core Processor, 16 cores
- Kernel: 6.6.87.2-microsoft-standard-WSL2
- Python: 3.11.17
- Redis server v=8.10.3

## AOF off

| Clients | Pipeline | Test | old thread-per-client | mini_redis | real Redis | mini_redis / old |
| ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 50 | 1 | SET | 11,826 | 40,404 | 82,850 | 3.4x |
| 50 | 1 | GET | 12,235 | 57,405 | 81,500 | 4.7x |
| 50 | 16 | SET | 17,979 | 120,192 | 1,219,512 | 6.7x |
| 50 | 16 | GET | 18,008 | 137,174 | 1,369,863 | 7.6x |
| 1000 | 1 | SET | 13,814 | 34,650 | 77,942 | 2.5x |
| 1000 | 1 | GET | 14,747 | 55,525 | 78,802 | 3.8x |
| 1000 | 16 | SET | 19,168 | 126,263 | 1,136,364 | 6.6x |
| 1000 | 16 | GET | 21,039 | 142,248 | 1,204,819 | 6.8x |

## AOF on

| Clients | Pipeline | Test | old thread-per-client | mini_redis | real Redis | mini_redis / old |
| ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 50 | 1 | SET | 8,193 | 26,589 | 89,526 | 3.2x |
| 50 | 1 | GET | 12,042 | 54,407 | 80,841 | 4.5x |
| 50 | 16 | SET | 10,903 | 96,525 | 862,069 | 8.9x |
| 50 | 16 | GET | 17,992 | 138,122 | 1,388,889 | 7.7x |
| 1000 | 1 | SET | 7,768 | 24,331 | 84,459 | 3.1x |
| 1000 | 1 | GET | 14,075 | 50,633 | 77,882 | 3.6x |
| 1000 | 16 | SET | 9,797 | 97,943 | 819,672 | 10.0x |
| 1000 | 16 | GET | 20,555 | 140,845 | 1,250,000 | 6.9x |

## All runs

| Server | AOF | Clients | Pipeline | Test | ops/sec |
| --- | --- | ---: | ---: | --- | --- |
| old thread-per-client | off | 50 | 1 | SET | 11,696, 11,826, 11,857 |
| old thread-per-client | off | 50 | 1 | GET | 12,129, 12,235, 12,241 |
| old thread-per-client | off | 50 | 16 | SET | 17,892, 17,995, 17,979 |
| old thread-per-client | off | 50 | 16 | GET | 18,008, 18,086, 17,921 |
| old thread-per-client | off | 1000 | 1 | SET | 13,454, 13,897, 13,814 |
| old thread-per-client | off | 1000 | 1 | GET | 14,747, 14,890, 14,353 |
| old thread-per-client | off | 1000 | 16 | SET | 19,467, 18,447, 19,168 |
| old thread-per-client | off | 1000 | 16 | GET | 21,039, 20,387, 21,119 |
| old thread-per-client | on | 50 | 1 | SET | 8,187, 8,193, 8,210 |
| old thread-per-client | on | 50 | 1 | GET | 12,108, 12,034, 12,042 |
| old thread-per-client | on | 50 | 16 | SET | 10,807, 10,958, 10,903 |
| old thread-per-client | on | 50 | 16 | GET | 17,851, 17,992, 18,021 |
| old thread-per-client | on | 1000 | 1 | SET | 7,801, 7,768, 7,749 |
| old thread-per-client | on | 1000 | 1 | GET | 13,650, 14,075, 14,168 |
| old thread-per-client | on | 1000 | 16 | SET | 9,797, 9,781, 9,800 |
| old thread-per-client | on | 1000 | 16 | GET | 20,555, 20,916, 20,333 |
| mini_redis | off | 50 | 1 | SET | 40,617, 40,000, 40,404 |
| mini_redis | off | 50 | 1 | GET | 57,703, 54,945, 57,405 |
| mini_redis | off | 50 | 16 | SET | 119,760, 120,337, 120,192 |
| mini_redis | off | 50 | 16 | GET | 137,552, 135,501, 137,174 |
| mini_redis | off | 1000 | 1 | SET | 34,037, 37,161, 34,650 |
| mini_redis | off | 1000 | 1 | GET | 55,525, 57,737, 55,006 |
| mini_redis | off | 1000 | 16 | SET | 126,263, 126,743, 125,156 |
| mini_redis | off | 1000 | 16 | GET | 142,248, 140,647, 143,472 |
| mini_redis | on | 50 | 1 | SET | 27,248, 26,212, 26,589 |
| mini_redis | on | 50 | 1 | GET | 57,241, 54,407, 53,107 |
| mini_redis | on | 50 | 16 | SET | 96,525, 95,969, 96,712 |
| mini_redis | on | 50 | 16 | GET | 138,313, 137,363, 138,122 |
| mini_redis | on | 1000 | 1 | SET | 24,331, 24,102, 24,331 |
| mini_redis | on | 1000 | 1 | GET | 48,473, 51,600, 50,633 |
| mini_redis | on | 1000 | 16 | SET | 97,943, 97,087, 99,404 |
| mini_redis | on | 1000 | 16 | GET | 143,472, 139,470, 140,845 |
| real Redis | off | 50 | 1 | SET | 82,850, 83,472, 82,237 |
| real Redis | off | 50 | 1 | GET | 82,305, 81,500, 80,645 |
| real Redis | off | 50 | 16 | SET | 1,176,471, 1,234,568, 1,219,512 |
| real Redis | off | 50 | 16 | GET | 1,369,863, 1,369,863, 1,388,889 |
| real Redis | off | 1000 | 1 | SET | 78,555, 76,805, 77,942 |
| real Redis | off | 1000 | 1 | GET | 78,186, 78,802, 79,177 |
| real Redis | off | 1000 | 16 | SET | 1,162,791, 1,123,596, 1,136,364 |
| real Redis | off | 1000 | 16 | GET | 1,204,819, 1,219,512, 1,204,819 |
| real Redis | on | 50 | 1 | SET | 88,417, 90,416, 89,526 |
| real Redis | on | 50 | 1 | GET | 80,841, 80,645, 81,235 |
| real Redis | on | 50 | 16 | SET | 847,458, 862,069, 892,857 |
| real Redis | on | 50 | 16 | GET | 1,369,863, 1,408,451, 1,388,889 |
| real Redis | on | 1000 | 1 | SET | 84,459, 83,264, 84,746 |
| real Redis | on | 1000 | 1 | GET | 77,882, 79,365, 77,459 |
| real Redis | on | 1000 | 16 | SET | 819,672, 819,672, 840,336 |
| real Redis | on | 1000 | 16 | GET | 1,250,000, 1,298,701, 1,234,568 |
