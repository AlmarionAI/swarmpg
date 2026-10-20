Run 20261020T220935Z: 10000 agents x 20 requests, 1000 agents in flight. Darwin arm64; Docker 29.7.2; 16 CPUs and 15 GiB for containers; 2 unrelated containers running.

| Setup | Agents | Agent connections | Peak Postgres backends | Requests | Errors | Req/s | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| PgBouncer + RLS (baseline) | 10000 | 10000 | 32 | 200000 | 0 | 11000 | 87.2 | 110.0 | 115.1 |
| SwarmPG gateway + stock Postgres | 10000 | 1000 | 32 | 200000 | 0 | 26480 | 36.5 | 47.2 | 75.0 |
| SwarmPG gateway + SwarmPG kernel | 10000 | 1000 | 32 | 200000 | 0 | 25763 | 37.8 | 48.1 | 58.8 |

Peak memory per container (MiB):

| Setup | Container | Peak MiB |
|---|---|---:|
| baseline | pg-upstream | 54 |
| baseline | pgbouncer | 68 |
| swarmpg-stock | gateway-stock | 99 |
| swarmpg-stock | pg-upstream | 219 |
| swarmpg-kernel | gateway-kernel | 98 |
| swarmpg-kernel | pg-kernel | 73 |
