import re
from typing import Dict, Any, List, Tuple
import logging

logger = logging.getLogger(__name__)

class TemplateResolver:
    """
    Resolves template variables in the format {{ node_id.key }}
    
    Example:
        template = "Process {{ fetch_user.user_id }} with {{ config.api_key }}"
        outputs = {
            "fetch_user": {"user_id": 123, "name": "John"},
            "config": {"api_key": "secret"}
        }
        result = "Process 123 with secret"
    """
    
    TEMPLATE_PATTERN = r'\{\{\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*\}\}'
    
    def __init__(self, node_outputs: Dict[str, Any]):
        """
        Initialize resolver with node outputs
        
        Args:
            node_outputs: Dictionary mapping node_id to output data
        """
        self.node_outputs = node_outputs
    
    def extract_variables(self, template: str) -> List[Tuple[str, str]]:
        """
        Extract all template variables from a string
        
        Args:
            template: String containing template variables
            
        Returns:
            List of (node_id, key) tuples
        """
        matches = re.findall(self.TEMPLATE_PATTERN, template)
        return matches
    
    def resolve(self, template: str) -> str:
        """
        Resolve all template variables in a string
        
        Args:
            template: String containing template variables
            
        Returns:
            String with all variables resolved
            
        Raises:
            ValueError: If a referenced node or key is not found
        """
        if not isinstance(template, str):
            return template
        
        variables = self.extract_variables(template)
        resolved = template
        
        for node_id, key in variables:
            # Check if node exists
            if node_id not in self.node_outputs:
                raise ValueError(f"Node '{node_id}' not found in outputs")
            
            node_output = self.node_outputs[node_id]
            
            # Check if key exists
            if key not in node_output:
                raise ValueError(f"Key '{key}' not found in node '{node_id}' output")
            
            # Replace variable
            value = node_output[key]
            pattern = f'{{{{ {node_id}.{key} }}}}'
            resolved = resolved.replace(pattern, str(value))
            
            # Also handle without spaces
            pattern_no_space = f'{{{{{node_id}.{key}}}}}'
            resolved = resolved.replace(pattern_no_space, str(value))
        
        return resolved
    
    def resolve_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve all template variables in a configuration dictionary
        
        Args:
            config: Configuration dictionary that may contain template strings
            
        Returns:
            Dictionary with all template variables resolved
        """
        resolved_config = {}
        
        for key, value in config.items():
            if isinstance(value, str):
                resolved_config[key] = self.resolve(value)
            elif isinstance(value, dict):
                resolved_config[key] = self.resolve_config(value)
            elif isinstance(value, list):
                resolved_config[key] = [
                    self.resolve(item) if isinstance(item, str) else item
                    for item in value
                ]
            else:
                resolved_config[key] = value
        
        return resolved_config