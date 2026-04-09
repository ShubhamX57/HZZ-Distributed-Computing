# Distributed ATLAS Higgs → 4ℓ Analysis

[![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-CC0%20%2B%20MIT-blue)](LICENSE)

This project implements a **distributed, cloud‑native analysis** of the [ATLAS Open Data](http://opendata.atlas.cern) to rediscover the Higgs boson in the four‑lepton decay channel  
**H → ZZ* → ℓℓℓℓ**

The system uses a **message‑queue architecture** (RabbitMQ) with a single **coordinator** and multiple **worker** containers. It automatically scales horizontally—just add more workers.

**Key features**
- Fully automatic configuration using `atlasopenmagic` (no manual file downloads).
- Parallel processing of 40+ ROOT files across multiple Docker containers.
- Identical physics results to the official ATLAS Open Data notebook (**4.5σ** significance).
- Ready to run with a single `docker compose up` command.

---


## Repository Structure

- `coordinator/` – builds the dataset list, sends tasks, aggregates results, produces the final plot.
- `worker/` – processes individual ROOT files, applying the physics selection and MC weights.
- `docker-compose.yml` – launches RabbitMQ, the coordinator, and one or more workers.


## Architecture Diagram
                    ┌─────────────┐
                    │  RabbitMQ   │
                    │ (Broker)    │
                    └──────┬──────┘
           ┌───────────────┼───────────────┐
           │               │               │
     ┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼─────┐
     │  Worker 1 │   │  Worker 2 │   │  Worker 3 │
     └───────────┘   └───────────┘   └───────────┘
           │               │               │
           └───────────────┼───────────────┘
                           │
                   ┌───────▼────────┐
                   │  Coordinator   │
                   │ (Aggregator)   │
                   └────────────────┘
  
## Requirements

- Docker & Docker Compose
- ~10 GB free disk space (intermediate caching of ROOT files)

## Quick Start

1. Clone the repository.
   ```bash
   git clone https://github.com/ShubhamX57/HZZ-Distributed-Computing.git
   
3. Build and run with a single worker:
   ```bash
   docker compose up --build
4. To use multiple workers (e.g., 3 workers):
   ```bash
    docker compose up --build --scale worker=3
5. Retrieve Results:
   ```bash
   
   a.First, find your volume name:

       docker volume ls

   b.Then copy (replace folder_results_vol with your actual volume name)

    
    docker run --rm -v folder_results_vol:/results -v "$(pwd):/out" alpine sh -c 'cp /results/* /out/'

4. Clean Up :
   ```bash
   docker compose down -v

