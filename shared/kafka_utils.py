import json
import os
from typing import Dict, Any, List
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import logging

logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# Topic names
TOPIC_WORKFLOW_START = 'workflow.start'
TOPIC_TASK_DISPATCH = 'task.dispatch'
TOPIC_TASK_COMPLETE = 'task.complete'


def get_producer() -> Producer:
    """
    Create and return a Kafka producer instance
    
    Returns:
        Producer: Configured Kafka producer
    """
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'workflow-engine-producer',
        'acks': 'all',  # Wait for all replicas
        'retries': 3,
        'max.in.flight.requests.per.connection': 1,  # Ensure ordering
    }
    
    producer = Producer(conf)
    logger.info(f"Kafka producer created: {KAFKA_BOOTSTRAP_SERVERS}")
    return producer


def get_consumer(group_id: str, topics: List[str], auto_commit: bool = False) -> Consumer:
    """
    Create and return a Kafka consumer instance
    
    Args:
        group_id: Consumer group ID
        topics: List of topics to subscribe to
        auto_commit: Whether to auto-commit offsets
        
    Returns:
        Consumer: Configured Kafka consumer
    """
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': auto_commit,
        'max.poll.interval.ms': 300000,  # 5 minutes
        'session.timeout.ms': 10000,
    }
    
    consumer = Consumer(conf)
    consumer.subscribe(topics)
    logger.info(f"Kafka consumer created: group={group_id}, topics={topics}")
    return consumer


def publish_message(producer: Producer, topic: str, message: Dict[str, Any], key: str = None) -> None:
    """
    Publish a message to a Kafka topic
    
    Args:
        producer: Kafka producer instance
        topic: Topic name
        message: Message payload (will be JSON encoded)
        key: Optional message key for partitioning
    """
    try:
        # Serialize message to JSON
        value = json.dumps(message).encode('utf-8')
        key_bytes = key.encode('utf-8') if key else None
        
        # Produce message
        producer.produce(
            topic=topic,
            key=key_bytes,
            value=value
        )
        
        # Flush to ensure delivery
        producer.flush()
        
    except Exception as e:
        logger.error(f"Failed to publish message to {topic}: {e}")
        raise