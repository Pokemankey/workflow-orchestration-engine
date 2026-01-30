import os
import json
import time
import logging
from typing import Set

import redis
from confluent_kafka import KafkaError

from shared.models import WorkflowExecution, TaskStatus
from shared.kafka_utils import (
    get_consumer,
    get_producer,
    publish_message,
    TOPIC_WORKFLOW_START,
    TOPIC_TASK_DISPATCH,
    TOPIC_TASK_COMPLETE
)
from shared.template_resolver import TemplateResolver
from orchestrator.dag_validator import DAGValidator 

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Redis
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)

# Initialize Kafka
producer = get_producer()
consumer = get_consumer(
    group_id='orchestrator-group',
    topics=[TOPIC_WORKFLOW_START, TOPIC_TASK_COMPLETE]
)

def process_workflow(execution_id: str) -> None:
    """
    Main orchestration logic - determines which tasks to run next
    
    Args:
        execution_id: Workflow execution ID
    """
    try:
        # Load workflow state
        redis_key = f"workflow:{execution_id}"
        workflow_data = redis_client.get(redis_key)
        
        if not workflow_data:
            logger.error(f"Workflow {execution_id} not found in Redis")
            return
        
        execution = WorkflowExecution(**json.loads(workflow_data))
        
        # Check if workflow is already completed or failed
        if execution.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
            logger.info(f"Workflow {execution_id} already {execution.status}")
            return
        
        # Get validator
        validator = DAGValidator(execution.workflow)
        
        # Get completed nodes
        completed_nodes: Set[str] = {
            node_id for node_id, status in execution.node_states.items()
            if status == TaskStatus.COMPLETED
        }
        
        # Get ready nodes (all dependencies satisfied)
        ready_nodes = validator.get_ready_nodes(completed_nodes)
        
        # Filter out nodes already running or failed
        ready_nodes = [
            node_id for node_id in ready_nodes
            if execution.node_states[node_id] == TaskStatus.PENDING
        ]
        
        # Dispatch ready nodes
        if ready_nodes:
            logger.info(f"Workflow {execution_id}: Dispatching {len(ready_nodes)} tasks: {ready_nodes}")
            
            for node_id in ready_nodes:
                try:
                    dispatch_task(execution_id, node_id, execution)
                    execution.node_states[node_id] = TaskStatus.RUNNING
                except Exception as e:
                    logger.error(f"Failed to dispatch task {node_id}: {e}")
                    execution.node_states[node_id] = TaskStatus.FAILED
                    execution.status = TaskStatus.FAILED
                    execution.error = f"Failed to dispatch task {node_id}: {str(e)}"
        
        # Check if workflow is complete
        if all(status == TaskStatus.COMPLETED for status in execution.node_states.values()):
            execution.status = TaskStatus.COMPLETED
            logger.info(f"Workflow {execution_id} COMPLETED")
        elif any(status == TaskStatus.FAILED for status in execution.node_states.values()):
            execution.status = TaskStatus.FAILED
            logger.warning(f"Workflow {execution_id} FAILED")
        elif ready_nodes:
            execution.status = TaskStatus.RUNNING
        
        # Save updated state
        redis_client.set(redis_key, execution.model_dump_json())
        
    except Exception as e:
        logger.error(f"Error processing workflow {execution_id}: {e}", exc_info=True)

def dispatch_task(execution_id: str, node_id: str, execution: WorkflowExecution) -> None:
    """Dispatch a task to workers via Kafka"""
    # Find the node configuration
    node = next((n for n in execution.workflow.dag.nodes if n.id == node_id), None)
    
    if not node:
        raise ValueError(f"Node {node_id} not found in workflow")
    
    # Resolve templates in node config
    try:
        resolver = TemplateResolver(execution.node_outputs)
        resolved_config = resolver.resolve_config(node.config)
    except Exception as e:
        logger.error(f"Template resolution failed for node {node_id}: {e}")
        raise
    
    # Create task message
    task_message = {
        "execution_id": execution_id,
        "node_id": node_id,
        "handler": node.handler, 
        "config": resolved_config
    }
    
    # Publish to Kafka
    publish_message(
        producer=producer,
        topic=TOPIC_TASK_DISPATCH,
        message=task_message,
        key=node_id
    )
    
    logger.info(f"Dispatched task: {node_id} (handler: {node.handler})")

def handle_task_completion(execution_id: str, node_id: str) -> None:
    """
    Handle task completion event
    
    Args:
        execution_id: Workflow execution ID
        node_id: Completed node ID
    """
    logger.info(f"Task completed: {node_id} in workflow {execution_id}")
    
    # Re-process workflow to check for newly ready tasks
    process_workflow(execution_id)

def main():
    """Main orchestrator event loop"""
    logger.info("Orchestrator service started")
    logger.info(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    logger.info(f"Listening to topics: {[TOPIC_WORKFLOW_START, TOPIC_TASK_COMPLETE]}")
    
    # Test Redis connection
    try:
        redis_client.ping()
        logger.info("Redis connection successful")
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        return
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
            
            # Parse message
            topic = msg.topic()
            value = json.loads(msg.value().decode('utf-8'))
            
            logger.debug(f"Received message from {topic}: {value}")
            
            # Handle different event types
            if topic == TOPIC_WORKFLOW_START:
                execution_id = value['execution_id']
                logger.info(f"Starting workflow: {execution_id}")
                process_workflow(execution_id)
            
            elif topic == TOPIC_TASK_COMPLETE:
                execution_id = value['execution_id']
                node_id = value['node_id']
                handle_task_completion(execution_id, node_id)
            
            # Commit offset after successful processing
            consumer.commit(asynchronous=False)
    
    except KeyboardInterrupt:
        logger.info("Orchestrator interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error in orchestrator: {e}", exc_info=True)
    finally:
        logger.info("Shutting down orchestrator...")
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    main()