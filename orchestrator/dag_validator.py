from typing import List, Set, Dict, Tuple
import logging

from shared.models import WorkflowDefinition, NodeConfig

logger = logging.getLogger(__name__)

class DAGValidator:
    """
    Validates workflow DAGs and provides dependency resolution
    """
    
    def __init__(self, workflow: WorkflowDefinition):
        """
        Initialize validator with workflow definition
        
        Args:
            workflow: Workflow definition to validate
        """
        self.workflow = workflow
        self.nodes: Dict[str, NodeConfig] = {node.id: node for node in workflow.dag.nodes}
    
    def validate(self) -> Tuple[bool, str]:
        """
        Validate the workflow DAG
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check for empty workflow
        if not self.workflow.dag.nodes:
            return False, "Workflow must contain at least one node"
        
        # Check for duplicate node IDs
        node_ids = [node.id for node in self.workflow.dag.nodes]
        if len(node_ids) != len(set(node_ids)):
            return False, "Duplicate node IDs found"
        
        # Check all dependencies exist
        if not self._check_dependencies_exist():
            return False, "Invalid dependency reference: one or more dependencies do not exist"
        
        # Check for cycles
        if self._has_cycle():
            return False, "Workflow contains cycles (circular dependencies)"
        
        # Check for valid handler types
        valid_handlers = {"input", "output", "call_external_service", "llm", "external_api"}
        for node in self.workflow.dag.nodes:
            if node.handler not in valid_handlers:
                return False, f"Invalid handler '{node.handler}'. Must be one of: {valid_handlers}"
        
        logger.info(f"Workflow validated successfully: {len(self.nodes)} nodes")
        return True, ""
    
    def _check_dependencies_exist(self) -> bool:
        """
        Ensure all referenced dependencies actually exist as nodes
        
        Returns:
            True if all dependencies are valid
        """
        for node in self.workflow.dag.nodes:
            for dep in node.dependencies:
                if dep not in self.nodes:
                    logger.warning(f"Node '{node.id}' depends on non-existent node '{dep}'")
                    return False
        return True
    
    def _has_cycle(self) -> bool:
        """
        Detect cycles in the DAG using depth-first search
        
        Returns:
            True if a cycle is detected
        """
        visited: Set[str] = set()
        rec_stack: Set[str] = set()
        
        def dfs(node_id: str) -> bool:
            visited.add(node_id)
            rec_stack.add(node_id)
            
            for dep in self.nodes[node_id].dependencies:
                if dep not in visited:
                    if dfs(dep):
                        return True
                elif dep in rec_stack:
                    logger.warning(f"Cycle detected: {node_id} -> {dep}")
                    return True
            
            rec_stack.remove(node_id)
            return False
        
        for node_id in self.nodes:
            if node_id not in visited:
                if dfs(node_id):
                    return True
        
        return False
    
    def get_ready_nodes(self, completed_nodes: Set[str]) -> List[str]:
        """
        Get nodes that are ready to execute (all dependencies satisfied)
        
        Args:
            completed_nodes: Set of node IDs that have completed
            
        Returns:
            List of node IDs ready to execute
        """
        ready = []
        
        for node in self.workflow.dag.nodes:
            if node.id in completed_nodes:
                continue
            
            if all(dep in completed_nodes for dep in node.dependencies):
                ready.append(node.id)
        
        return ready
    
    def get_root_nodes(self) -> List[str]:
        """
        Get nodes with no dependencies (entry points)
        
        Returns:
            List of root node IDs
        """
        return [node.id for node in self.workflow.dag.nodes if not node.dependencies]
    
    def topological_sort(self) -> List[str]:
        """
        Perform topological sort of the DAG
        
        Returns:
            List of node IDs in topological order
        """
        visited: Set[str] = set()
        stack: List[str] = []
        
        def dfs(node_id: str):
            visited.add(node_id)
            for dep in self.nodes[node_id].dependencies:
                if dep not in visited:
                    dfs(dep)
            stack.append(node_id)
        
        for node_id in self.nodes:
            if node_id not in visited:
                dfs(node_id)
        
        return stack