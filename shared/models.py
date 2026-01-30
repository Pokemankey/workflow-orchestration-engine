from typing import List, Dict, Any, Optional
from enum import Enum
from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    """Task execution status"""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class NodeConfig(BaseModel):
    """Configuration for a single workflow node"""
    id: str = Field(..., description="Unique node identifier")
    handler: str = Field(..., description="Node handler type")
    dependencies: List[str] = Field(default_factory=list, description="List of parent node IDs")
    config: Dict[str, Any] = Field(default_factory=dict, description="Node-specific configuration")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "get_user",
                "handler": "call_external_service",
                "dependencies": ["input"],
                "config": {"url": "http://localhost:8911/document/policy/list"}
            }
        }

class DAG(BaseModel):
    """DAG structure containing nodes"""
    nodes: List[NodeConfig] = Field(..., description="List of nodes in the DAG")

class WorkflowDefinition(BaseModel):
    """Complete workflow definition"""
    name: str = Field(..., description="Workflow name")
    dag: DAG = Field(..., description="DAG structure")

    class Config:
        json_schema_extra = {
            "example": {
                "name": "Parallel API Fetcher",
                "dag": {
                    "nodes": [
                        {
                            "id": "input",
                            "handler": "input",
                            "dependencies": []
                        },
                        {
                            "id": "get_user",
                            "handler": "call_external_service",
                            "dependencies": ["input"],
                            "config": {"url": "http://localhost:8911/api/users"}
                        }
                    ]
                }
            }
        }

class WorkflowExecution(BaseModel):
    """Runtime state of a workflow execution"""
    execution_id: str
    name: str
    workflow: WorkflowDefinition
    status: TaskStatus
    node_states: Dict[str, TaskStatus] = Field(default_factory=dict)
    node_outputs: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None