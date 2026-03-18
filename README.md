# Big Data Storage and Retrieval

This repository contains:
- Data Modeling and Storage scripts for PostgreSQL, MongoDB, and Neo4j/Memgraph
- Data Analysis scripts (`q1`, `q2`, `q3`) for the same three systems

## 1) Prerequisites

Required tools:
- `python3`
- `psql` (PostgreSQL client)
- `mongosh` (MongoDB shell)
- `cypher-shell` (or Docker Neo4j with `cypher-shell` inside the container)

Create Python virtual environment:

```bash
python3 -m venv venv
venv/bin/python -m pip install --upgrade pip
venv/bin/python -m pip install -r requirements.txt
```

## 2) Data Modeling and Storage Tasks (Load Scripts)

Data files are read from `data/processed/` by default.

### 2.1 Load PostgreSQL

```bash
TARGET_DB=customer_campaign_analytics \
PGHOST=localhost PGPORT=5432 PGUSER=postgres \
bash scripts/load_data_psql.sh
```

If your user needs password:

```bash
export PGPASSWORD='your_password'
```

### 2.2 Load MongoDB

```bash
TARGET_DB=customer_campaign_analytics \
MONGO_URI='mongodb://localhost:27017' \
bash scripts/load_data_mongodb.sh
```

### 2.3 Load Neo4j / Memgraph

```bash
NEO4J_URI='bolt://localhost:7687' \
NEO4J_USER='neo4j' \
NEO4J_PASSWORD='neo4jneo4j' \
NEO4J_DATABASE='neo4j' \
bash scripts/load_data_graph.sh
```

Notes:
- Graph loader script: `scripts/load_data_graph.sh`
- Graph template Cypher file: `scripts/load_data_graph.cypherl`
- In Docker mode, loader auto-detects a running `neo4j_server` container (`NEO4J_LOAD_MODE=auto`).

## 3) Data Analysis Tasks

Main analysis scripts:
- `scripts/analyze_psql.py`
- `scripts/analyze_mongodb.py`
- `scripts/analyze_graph.py`
- `scripts/run_analysis.sh` (runs all)

### 3.1 Run all analyses (recommended)

```bash
TARGET_DB=customer_campaign_analytics \
PGHOST=localhost PGPORT=5432 PGUSER=postgres \
MONGO_URI='mongodb://localhost:27017' \
NEO4J_URI='bolt://localhost:7687' \
NEO4J_USER='neo4j' \
NEO4J_PASSWORD='neo4jneo4j' \
NEO4J_DATABASE='neo4j' \
PYTHONUNBUFFERED=1 \
bash scripts/run_analysis.sh | tee output/run_analysis.log
```

### 3.2 Run each analysis separately

```bash
venv/bin/python scripts/analyze_psql.py
venv/bin/python scripts/analyze_mongodb.py
venv/bin/python scripts/analyze_graph.py
venv/bin/python scripts/build_analysis_summary.py
```

For Mongo progress heartbeat logs every 20 seconds:

```bash
MONGO_PROGRESS_SECONDS=20 \
TARGET_DB=customer_campaign_analytics \
MONGO_URI='mongodb://localhost:27017' \
venv/bin/python -u scripts/analyze_mongodb.py | tee output/analyze_mongodb.log
```

## 4) Benchmarking Tasks

Benchmark scripts:
- `scripts/benchmark_psql.py`
- `scripts/benchmark_mongo.py`
- `scripts/benchmark_graph.py`
- `scripts/run_benchmarks.sh` (runs all 3)

Benchmark argument:
- `N` = number of times each query (`q1`, `q2`, `q3`) is executed
- if omitted, default is `N=1`

### 4.1 Run all benchmarks (recommended)

```bash
cd /mnt/e/git_repos/big-data-storage-and-retrieval
source venv/bin/activate
./scripts/run_benchmarks.sh 5
```

Run with default `N=1`:

```bash
./scripts/run_benchmarks.sh
```

If needed once:

```bash
chmod +x scripts/run_benchmarks.sh
```

### 4.2 Run each benchmark separately

```bash
cd /mnt/e/git_repos/big-data-storage-and-retrieval
source venv/bin/activate

python scripts/benchmark_psql.py 5
python scripts/benchmark_mongo.py 5
python scripts/benchmark_graph.py 5
```

### 4.3 Optional connection settings

Use env vars if your local host/credentials differ:

```bash
TARGET_DB=customer_campaign_analytics \
PGHOST=localhost PGPORT=5432 PGUSER=postgres \
MONGO_URI='mongodb://localhost:27017' \
NEO4J_URI='bolt://localhost:7687' \
NEO4J_USER='neo4j' \
NEO4J_PASSWORD='neo4jneo4j' \
NEO4J_DATABASE='neo4j' \
./scripts/run_benchmarks.sh 5
```

## 5) Output Files

After analysis and benchmarking, key output files are:
- `output/analysis_psql.json`
- `output/analysis_mongodb.json`
- `output/analysis_graph.json`
- `output/analysis_summary.csv`
- `output/run_analysis.log` (if using `tee`)
- `benchmark_results/postgres_results.csv`
- `benchmark_results/mongo_results.csv`
- `benchmark_results/graph_results.csv`

Benchmark CSV format:
- `query_name,run_number,execution_time_ms,database`

Check quickly:

```bash
ls -lh output/analysis_psql.json \
       output/analysis_mongodb.json \
       output/analysis_graph.json \
       output/analysis_summary.csv \
       benchmark_results/postgres_results.csv \
       benchmark_results/mongo_results.csv \
       benchmark_results/graph_results.csv
```

## 6) Quick Health Checks

Before running analysis/benchmarks, you can test DB connectivity:

```bash
pg_isready -h localhost -p 5432
mongosh --quiet --eval 'db.adminCommand({ ping: 1 })'
venv/bin/python - <<'PY'
from neo4j import GraphDatabase
d = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j","neo4jneo4j"))
with d.session(database="neo4j") as s:
    print(s.run("RETURN 1 AS ok").single()["ok"])
d.close()
PY
```

## 7) Common Issues

### MongoDB connection refused (`localhost:27017`)
- MongoDB is not running, or not reachable from WSL.
- Start MongoDB service/container, or use a valid `MONGO_URI` host.

### No new output for a long time
- Some queries (especially Mongo `q1`) are heavy on full dataset.
- Use unbuffered output and heartbeat logs:

```bash
PYTHONUNBUFFERED=1 MONGO_PROGRESS_SECONDS=20 venv/bin/python -u scripts/analyze_mongodb.py
```

### Neo4j memory issues during load
- Lower batch sizes in `load_data_graph.sh` environment variables, for example:

```bash
DELETE_REL_BATCH_SIZE=2000 DELETE_BATCH_SIZE=500 EVENT_BATCH_SIZE=25 MESSAGE_BATCH_SIZE=10 bash scripts/load_data_graph.sh
```

### Neo4j `Unable to connect to localhost:7687` in WSL
- Confirm Neo4j is actually running and listening:

```bash
docker ps -a --filter "name=neo4j_server"
docker inspect neo4j_server --format '{{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}}'
```

- If the container is stopped, restart it:

```bash
docker start neo4j_server
docker logs --tail 100 neo4j_server
```

- If Neo4j is running on Windows host (not inside WSL), use:

```bash
NEO4J_URI='bolt://host.docker.internal:7687' bash scripts/load_data_graph.sh
```
