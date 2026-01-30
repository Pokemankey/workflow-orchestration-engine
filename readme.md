# Event-Driven Workflow Orchestration Engine

A distributed workflow orchestration system that executes DAG-based workflows with parallel task execution, built with Python, Kafka, and Redis.

---

## Setup Instructions

### Prerequisites

- **Docker**
- **Docker Compose**

### Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/Pokemankey/workflow-orchestration-engine
cd workflow-orchestrator

# 2. Start all services
docker-compose up --build

# 3. Verify services are running (in another terminal)
docker-compose ps

# Expected output:
# kafka                 Up (healthy)
# kafka-init            Exited (0)
# redis                 Up (healthy)
# workflow-api          Up
# workflow-orchestrator Up
# worker-1              Up
# worker-2              Up
# worker-3              Up

# 4. Test API health
curl http://localhost:8000/health
```

## Architecture Overview

### System Components

**API Service** REST endpoints for workflow submission and status queries
**Orchestrator** Manages workflow lifecycle, resolves dependencies, dispatches tasks
**Workers** Execute tasks (mock API calls, LLM calls), update state in Redis
**Kafka** Event bus for asynchronous communication between services
**Redis** Stores workflow state and provides distributed locking

### Data Flow

1. **Client** submits workflow JSON → **API**
2. **API** validates DAG (cycle detection) → stores in **Redis** → publishes to **Kafka** (`workflow.start`)
3. **Orchestrator** receives event → identifies ready nodes (no dependencies)
4. **Orchestrator** publishes tasks → **Kafka** (`task.dispatch`)
5. **Workers** consume tasks → execute handlers → update **Redis** → publish completion → **Kafka** (`task.complete`)
6. **Orchestrator** receives completion → checks dependencies → dispatches next tasks
7. Repeat until all nodes complete
8. Workflow marked `COMPLETED` in **Redis**

### Supported Handlers

- **`input`**: Entry point, initializes workflow data
- **`output`**: Exit point, aggregates final results
- **`call_external_service`**: Mocks HTTP API calls (sleeps 1-2s)
- **`llm`**: Mocks LLM/AI calls (sleeps 1-2s)

---

## How to Trigger Test Workflows

### Install requests

pip install requests

### Run tests

python test_workflows.py

## API Reference

- **POST** `/workflows` - Submit workflow
- **POST** `/workflows/{execution_id}/trigger` - Trigger workflow execution
- **GET** `/workflows/{execution_id}` - Get workflow status
- **GET** `/workflows/{execution_id}/results` - Get final results
- **GET** `/health` - Health check

**Interactive API docs:** http://localhost:8000/docs

---

## Technology Stack

- **Python 3.10+** with type hints
- **FastAPI** - REST API framework
- **Apache Kafka** (KRaft mode) - Event streaming
- **Redis** - State management
- **Docker & Docker Compose** - Containerization
- **Pydantic** - Data validation
