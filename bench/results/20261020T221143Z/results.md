Run 20261020T221143Z: 10000 agents x 20 requests, 1000 agents in flight. Darwin arm64; Docker 29.7.2; 16 CPUs and 15 GiB for containers; 2 unrelated containers running.

| Setup | Agents | Agent connections | Peak Postgres backends | Requests | Errors | Req/s | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| PgBouncer + RLS (baseline) | 10000 | 10000 | 32 | 200000 | 0 | 10706 | 89.2 | 114.8 | 128.2 |
| SwarmPG gateway + stock Postgres | 10000 | 1000 | 32 | 200000 | 0 | 21990 | 42.3 | 64.7 | 121.4 |
| SwarmPG gateway + SwarmPG kernel | 10000 | 1000 | 32 | 200000 | 0 | 22062 | 43.4 | 63.2 | 78.5 |

Peak memory per container (MiB):

| Setup | Container | Peak MiB |
|---|---|---:|
| baseline | pg-upstream | 185 |
| baseline | pgbouncer | 67 |
| swarmpg-stock | gateway-stock | 100 |
| swarmpg-stock | pg-upstream | 73 |
| swarmpg-kernel | gateway-kernel | 100 |
| swarmpg-kernel | pg-kernel | 224 |
