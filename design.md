# Design Document - Event-Driven Workflow Orchestration Engine

## Architecture Overview

- **API Service** REST endpoints for workflow submission and status queries
- **Orchestrator** Manages workflow lifecycle, resolves dependencies, dispatches tasks
- **Workers** Execute tasks (mock API calls, LLM calls), update state in Redis
- **Kafka** Event bus for asynchronous communication between services
- **Redis** Stores workflow state and provides distributed locking

## Core Design Decisions

### 1. Workflow Parsing & Validation

**Cycle Detection**:

- Implemented DFS-based cycle detection with path tracking
- Workflows with cycles are rejected before execution begins

**Structure Validation**:

- Verified all dependency references point to existing nodes
- Validated JSON schema (node_id, handler, config, dependencies)
- Ensured workflow has at least one node

**DAG Construction**:

- Parse JSON into internal graph structure
- Store nodes with their dependencies and configurations

### 2. Event-Driven Orchestration

**Dependency Resolution**:

- Orchestrator checks which nodes have all dependencies satisfied
- Uses `DAGValidator.get_ready_nodes(completed_nodes)` to find eligible nodes
- A node is ready when all its parent nodes are in COMPLETED state
- Ready nodes with PENDING status are dispatched immediately

**Parallelism**:

- Ready nodes are dispatched immediately
- Multiple workers can process tasks concurrently
- Kafka partitioning enables horizontal scaling of workers

**State Management**:

```
PENDING → RUNNING → COMPLETED or FAILED

```

- States stored in Redis: `workflow:{execution_id}`
- Orchestrator updates states based on Kafka events
- Worker reports status changes via `TOPIC_TASK_COMPLETE`

**Communication Flow**:

```
API → TOPIC_WORKFLOW_START → Orchestrator
Orchestrator → TOPIC_TASK_DISPATCH → Workers
Workers → TOPIC_TASK_COMPLETE → Orchestrator
```

### 3. Data Passing & Template Resolution

**Output Storage**:

- Worker results stored in the main workflow state object in Redis
- Key: `workflow:{execution_id}` contains entire `WorkflowExecution` object
- Node outputs stored in `execution.node_outputs[node_id]` dictionary
- Updated atomically using Redis distributed locks to prevent race conditions

**Template Resolution**:

- Pattern: `{{ node_id.output_key }}`
- Regex-based extraction: `r'\{\{\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*\}\}'`
- Recursive resolution in config objects (dicts, lists, strings)
- Performed by orchestrator before dispatching tasks to workers

**Resolution Logic**:

```python
# Before dispatch
config = {"prompt": "{{ fetch_data.result }}"}
# After resolution (fetch_data returned {"result": "API response"})
config = {"prompt": "API response"}
```

### 4. Worker Implementation & Mocking

**Handler Types**:

- `external_service`: Sleeps 1-2s, returns mock JSON
- `llm_service`: Accepts prompt, sleeps, returns mock completion
- `data_processor`: Processes input data with simple transformations

**Idempotency**:

- Manual offset commit in Kafka (`enable.auto.commit=False`)
- Tasks designed to be re-runnable (stateless operations)
- Redis can track processed tasks if strict idempotency needed

**Message Processing**:

- Workers poll from `TOPIC_TASK_DISPATCH`
- Execute task handler with resolved config
- Report results to `TOPIC_TASK_COMPLETE`
- Synchronous commit after successful processing (at-least-once delivery)

## Handling Core Scenarios

### Scenario A: Linear Chain (A → B → C)

1. A dispatched immediately (no dependencies)
2. A completes, stores result in `execution.node_outputs['A']`
3. Orchestrator triggered, checks ready nodes: B has all dependencies completed
4. B dispatched with A's output resolved in config via template resolution
5. B completes, stores result in `execution.node_outputs['B']`
6. Orchestrator triggered, C becomes ready (B completed)
7. C dispatched with B's (or A's) output resolved

### Scenario B: Fan-Out/Fan-In (A → B,C → D)

1. A completes, stores result
2. Orchestrator checks ready nodes: both B and C have all dependencies satisfied
3. **B and C dispatched in parallel** (both have PENDING status, both ready)
4. B completes → publishes to `TOPIC_TASK_RESULT` → orchestrator processes workflow
5. C completes → publishes to `TOPIC_TASK_RESULT` → orchestrator processes workflow
6. On either completion event, orchestrator checks D: not ready yet (still missing one dependency)
7. On second completion event, orchestrator checks D: all dependencies satisfied
8. D dispatched with both `{{ B.result }}` and `{{ C.result }}` resolved

**Aggregation**: D's config can reference both `{{ B.result }}` and `{{ C.result }}`

### Scenario C: Race Conditions

**Problem**: B and C complete simultaneously, could trigger D twice

**Solution**:

- **Redis distributed locking**: `lock:workflow:{execution_id}` prevents concurrent state updates
- When B completes: acquires lock → updates state → releases lock → triggers orchestrator
- When C completes: acquires lock (waits if B still has it) → updates state → releases lock → triggers orchestrator
- Orchestrator's `process_workflow()` checks if D is still PENDING before dispatching
- If already dispatched to RUNNING by first completion event, second event skips it:

```python
  ready_nodes = [
      node_id for node_id in ready_nodes
      if execution.node_states[node_id] == TaskStatus.PENDING
  ]
```

- **D dispatched exactly once** even with simultaneous completions

## Trade-offs & Design Choices

### Redis vs PostgreSQL

- **Chose Redis**: Lower latency for state queries, simpler setup
- Trade-off: No persistence across Redis restarts (acceptable as per requirement sheet)

### Kafka vs Redis Streams

- **Chose Kafka**: Better semantics for distributed workers, consumer groups
- Better handling of backpressure and message replay

### Synchronous vs Asynchronous Commit

- **Chose Synchronous**: Stronger guarantees, predictable behavior
- Trade-off: Slightly lower throughput (acceptable given task execution time >> commit time)

### Template Resolution Timing

- **Resolved by Orchestrator**: Workers receive ready-to-execute configs
- Alternative: Workers resolve templates (rejected - orchestrator has full graph context)

## Error Handling

- **Node Failure**: Status set to FAILED, workflow marked as failed
- **Worker Crash**: Kafka redelivers message, task re-executed (at-least-once)
- **Orchestrator Crash**: Restarts and resumes from Kafka offset
- **Invalid Templates**: Caught during resolution, node marked as FAILED

## Scalability Considerations

- **Horizontal Worker Scaling**: Add more worker containers, Kafka handles distribution (can scale up to the number of partitions for the task.dispatch topic)
- **Orchestrator**: Single instance per design (can scale up to the number of partitions for the workflow.start and task.complete topic)
- **Redis**: Single instance (could use Redis Cluster for production)
- **Kafka**: Partitioned topics enable parallel processing

## Testing Strategy

- **Unit Tests**: Graph validation, cycle detection, template resolution
- **Integration Tests**: End-to-end workflow execution with mocked handlers
- **Edge Cases**: Simultaneous completions, missing dependencies, circular graphs
