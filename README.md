# exo-watchdog

Queue-backed control-plane service for routing requests through a distributed local-LLM cluster and recovering from backend failures.

## Highlights

- Single ingress point for local-LLM-bound workloads and queue-based execution
- Cluster monitoring, model discovery, stale-job recovery, and remote restart workflows
- Stress-testing utilities for simulated news and hourly workloads

## Tech Stack

- Python, FastAPI, SQLite, httpx, Docker Compose, remote restart and watchdog tooling

## Repository Layout

- `app.py`
- `docker-compose.yml`
- `stress_test.py`
- `requirements.txt`

## Getting Started

- Install Python dependencies from `requirements.txt` or build the provided Docker image.
- Set cluster and watchdog environment variables before starting the service.
- Run `docker compose up --build` or `uvicorn app:app --host 0.0.0.0 --port 8000` for local development.

## Current Status

- This README was refreshed from a code audit and is intentionally scoped to what is directly visible in the repository.
