import os
import json
import uuid
import logging
from typing import Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import redis

from shared.models import WorkflowDefinition, WorkflowExecution, TaskStatus
from shared.kafka_utils import get_producer, publish_message, TOPIC_WORKFLOW_START

# Import DAG validator
from orchestrator.dag_validator import DAGValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Redis client
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
    socket_connect_timeout=5
)

# Initialize Kafka producer
kafka_producer = get_producer()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events"""
    logger.info("Starting API service...")
    try:
        redis_client.ping()
        logger.info("Redis connection successful")
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        raise
    
    logger.info("API service started successfully")
    
    yield
    
    logger.info("Shutting down API service...")
    kafka_producer.flush()
    logger.info("API service shut down")

# Initialize FastAPI app
app = FastAPI(
    title="Workflow Orchestration Engine",
    description="Event-driven DAG workflow execution system",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/health")
async def root():
    """Root endpoint"""
    return {
        "service": "Workflow Orchestration Engine",
        "status": "running",
        "version": "1.0.0"
    }

@app.post("/workflows", status_code=201)
async def submit_workflow(workflow: WorkflowDefinition) -> Dict[str, str]:
    """
    Submit a new workflow for execution (auto-triggers)
    
    Args:
        workflow: Workflow definition with name and DAG
        
    Returns:
        Dict containing execution_id
    """
    try:
        # Validate the workflow DAG
        validator = DAGValidator(workflow)
        is_valid, error_message = validator.validate()
        
        if not is_valid:
            logger.warning(f"Invalid workflow submitted: {error_message}")
            raise HTTPException(status_code=400, detail=error_message)
        
        # Generate unique execution ID
        execution_id = str(uuid.uuid4())
        
        # Initialize node states
        node_states = {node.id: TaskStatus.PENDING for node in workflow.dag.nodes}
        
        # Create execution object
        execution = WorkflowExecution(
            execution_id=execution_id,
            name=workflow.name,
            workflow=workflow,
            status=TaskStatus.PENDING,
            node_states=node_states,
            node_outputs={}
        )
        
        # Store in Redis
        redis_key = f"workflow:{execution_id}"
        redis_client.set(redis_key, execution.model_dump_json())
        
        logger.info(f"Workflow '{workflow.name}' submitted: {execution_id} with {len(workflow.dag.nodes)} nodes")
        
        return {
            "execution_id": execution_id,
            "status": "pending",
            "message": f"Workflow '{workflow.name}' submitted"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error submitting workflow: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/workflow/trigger/{execution_id}")
async def trigger_workflow(execution_id: str) -> Dict[str, str]:
    """
    Manually trigger execution of a submitted workflow
    
    Args:
        execution_id: ID of the workflow to trigger
        
    Returns:
        Dict with status and execution_id
    """
    try:
        redis_key = f"workflow:{execution_id}"
        workflow_data = redis_client.get(redis_key)
        
        if not workflow_data:
            raise HTTPException(status_code=404, detail="Workflow not found")
        
        publish_message(
            producer=kafka_producer,
            topic=TOPIC_WORKFLOW_START,
            message={"execution_id": execution_id, "action": "start"},
            key=execution_id
        )
        
        logger.info(f"Workflow manually triggered: {execution_id}")
        
        return {
            "status": "triggered",
            "execution_id": execution_id,
            "message": "Workflow execution started"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error triggering workflow: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/workflows/{execution_id}")
async def get_workflow_status(execution_id: str) -> Dict[str, Any]:
    """
    Get current status of a workflow execution
    
    Args:
        execution_id: ID of the workflow
        
    Returns:
        Dict containing workflow status and node states
    """
    try:
        redis_key = f"workflow:{execution_id}"
        workflow_data = redis_client.get(redis_key)
        
        if not workflow_data:
            raise HTTPException(status_code=404, detail="Workflow not found")
        
        execution = WorkflowExecution(**json.loads(workflow_data))
        
        # Calculate progress
        total_nodes = len(execution.node_states)
        completed_nodes = sum(
            1 for status in execution.node_states.values()
            if status == TaskStatus.COMPLETED
        )
        failed_nodes = sum(
            1 for status in execution.node_states.values()
            if status == TaskStatus.FAILED
        )
        
        return {
            "execution_id": execution_id,
            "name": execution.name,
            "status": execution.status,
            "progress": {
                "total": total_nodes,
                "completed": completed_nodes,
                "failed": failed_nodes,
                "percentage": round((completed_nodes / total_nodes) * 100, 2) if total_nodes > 0 else 0
            },
            "node_states": execution.node_states,
            "error": execution.error
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting workflow status: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/workflows/{execution_id}/results")
async def get_workflow_results(execution_id: str) -> Dict[str, Any]:
    """
    Get final results of a completed workflow
    
    Args:
        execution_id: ID of the workflow
        
    Returns:
        Dict containing workflow results
    """
    try:
        redis_key = f"workflow:{execution_id}"
        workflow_data = redis_client.get(redis_key)
        
        if not workflow_data:
            raise HTTPException(status_code=404, detail="Workflow not found")
        
        execution = WorkflowExecution(**json.loads(workflow_data))
        
        if execution.status not in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
            raise HTTPException(
                status_code=400,
                detail=f"Workflow not finished. Current status: {execution.status}"
            )
        
        return {
            "execution_id": execution_id,
            "name": execution.name,
            "status": execution.status,
            "results": execution.node_outputs,
            "error": execution.error
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting workflow results: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)