import sys
import os

import json
import time
import logging
from typing import Dict, Any

import redis
from confluent_kafka import KafkaError

from shared.models import WorkflowExecution, TaskStatus
from shared.kafka_utils import (
    get_consumer,
    get_producer,
    publish_message,
    TOPIC_TASK_DISPATCH,
    TOPIC_TASK_COMPLETE
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

WORKER_ID = os.getenv('WORKER_ID', 'worker-unknown')

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
    group_id='worker-group',
    topics=[TOPIC_TASK_DISPATCH]
)


def execute_input_handler(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle input node (just passes through or initializes data)
    """
    time.sleep(1)  # Simulate processing
    
    return {
        "status": "success",
        "message": "Input received",
        "timestamp": time.time(),
        "worker_id": WORKER_ID
    }


def execute_output_handler(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle output node (aggregates results)
    """
    time.sleep(1)  # Simulate processing
    
    return {
        "status": "success",
        "message": "Output generated",
        "timestamp": time.time(),
        "worker_id": WORKER_ID
    }


def execute_call_external_service(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mock external service call (like external_api)
    """
    url = config.get('url', 'https://api.example.com')
    
    # Simulate network delay
    time.sleep(1)
    
    result = {
        "status": "success",
        "data": f"Mock response from {url}",
        "timestamp": time.time(),
        "url": url,
        "worker_id": WORKER_ID
    }
    
    logger.info(f"External service called: {url}")
    return result


def execute_llm_handler(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mock LLM generation
    """
    prompt = config.get('prompt', 'Default prompt')
    model = config.get('model', 'gpt-4')
    
    time.sleep(2.0)
    
    result = {
        "response": f"Mock LLM response for prompt: '{prompt[:50]}...'",
        "model": model,
        "timestamp": time.time(),
        "worker_id": WORKER_ID,
        "tokens_used": 150
    }
    
    logger.info(f"LLM generation completed: {model}")
    return result


def execute_task(execution_id: str, node_id: str, handler: str, config: Dict[str, Any]) -> None:
    """
    Execute a single task based on handler type
    """
    logger.info(f"[{WORKER_ID}] Executing task: {node_id} (handler: {handler}) for workflow {execution_id}")
    
    try:
        # Route to appropriate handler
        if handler == "input":
            result = execute_input_handler(config)
        elif handler == "output":
            result = execute_output_handler(config)
        elif handler in ["call_external_service", "external_api"]:
            result = execute_call_external_service(config)
        elif handler == "llm":
            result = execute_llm_handler(config)
        else:
            raise ValueError(f"Unknown handler: {handler}")
        
        # Update state in Redis
        update_task_state(execution_id, node_id, TaskStatus.COMPLETED, result)
        
        # Publish completion event
        completion_message = {
            "execution_id": execution_id,
            "node_id": node_id,
            "status": "COMPLETED"
        }
        
        publish_message(
            producer=producer,
            topic=TOPIC_TASK_COMPLETE,
            message=completion_message,
            key=execution_id
        )
        
        logger.info(f"[{WORKER_ID}] Task {node_id} completed successfully")
    
    except Exception as e:
        logger.error(f"[{WORKER_ID}] Task {node_id} failed: {e}", exc_info=True)
        
        error_result = {"error": str(e)}
        update_task_state(execution_id, node_id, TaskStatus.FAILED, error_result)
        
        failure_message = {
            "execution_id": execution_id,
            "node_id": node_id,
            "status": "FAILED",
            "error": str(e)
        }
        
        publish_message(
            producer=producer,
            topic=TOPIC_TASK_COMPLETE,
            message=failure_message,
            key=execution_id
        )


def update_task_state(
    execution_id: str,
    node_id: str,
    status: TaskStatus,
    output: Dict[str, Any]
) -> None:
    """Update task state in Redis with distributed locking"""
    redis_key = f"workflow:{execution_id}"
    lock_key = f"lock:{redis_key}"
    
    lock = redis_client.lock(lock_key, timeout=10)
    
    try:
        lock.acquire(blocking=True, blocking_timeout=5)
        
        workflow_data = redis_client.get(redis_key)
        if not workflow_data:
            logger.error(f"Workflow {execution_id} not found")
            return
        
        execution = WorkflowExecution(**json.loads(workflow_data))
        
        if execution.node_states.get(node_id) == TaskStatus.COMPLETED:
            logger.warning(f"Task {node_id} already completed, skipping update")
            return
        
        execution.node_states[node_id] = status
        execution.node_outputs[node_id] = output
        
        redis_client.set(redis_key, execution.model_dump_json())
        logger.debug(f"Updated state for {node_id}: {status}")
    
    finally:
        try:
            lock.release()
        except:
            pass


def main():
    """Main worker event loop"""
    logger.info(f"Worker service started: {WORKER_ID}")
    logger.info(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    logger.info(f"Listening to topic: {TOPIC_TASK_DISPATCH}")
    
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
            
            task = json.loads(msg.value().decode('utf-8'))
            
            execution_id = task['execution_id']
            node_id = task['node_id']
            handler = task['handler']
            config = task['config']
            
            execute_task(execution_id, node_id, handler, config)
            
            consumer.commit(asynchronous=False)
    
    except KeyboardInterrupt:
        logger.info(f"Worker {WORKER_ID} interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error in worker: {e}", exc_info=True)
    finally:
        logger.info(f"Shutting down worker {WORKER_ID}...")
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()