"""
Config Parser Service
Parses and validates YAML configuration files for multi-source pipelines
"""

import yaml
import os
from typing import Dict, Any, List, Optional
from pathlib import Path


class ConfigValidationError(Exception):
    """Raised when config file validation fails"""
    pass


def parse_config_file(config_path: str) -> Dict[str, Any]:
    """
    Parse YAML configuration file for multi-source pipeline
    
    Args:
        config_path: Path to the YAML config file
        
    Returns:
        Dictionary containing parsed config with sources and relationships
        
    Raises:
        ConfigValidationError: If config is invalid
        FileNotFoundError: If config file doesn't exist
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigValidationError(f"Invalid YAML format: {str(e)}")
    except Exception as e:
        raise ConfigValidationError(f"Error reading config file: {str(e)}")
    
    # Validate config structure
    validate_config(config)
    
    return config


def validate_config(config: Dict[str, Any]) -> None:
    """
    Validate configuration structure and values
    
    Args:
        config: Parsed config dictionary
        
    Raises:
        ConfigValidationError: If validation fails
    """
    if not isinstance(config, dict):
        raise ConfigValidationError("Config must be a dictionary")
    
    # Validate sources
    if "sources" not in config:
        raise ConfigValidationError("Config must contain 'sources' key")
    
    if not isinstance(config["sources"], list):
        raise ConfigValidationError("'sources' must be a list")
    
    if len(config["sources"]) == 0:
        raise ConfigValidationError("At least one source must be defined")
    
    source_names = []
    for i, source in enumerate(config["sources"]):
        if not isinstance(source, dict):
            raise ConfigValidationError(f"Source {i} must be a dictionary")
        
        # Validate required fields
        if "name" not in source:
            raise ConfigValidationError(f"Source {i} must have 'name' field")
        if "type" not in source:
            raise ConfigValidationError(f"Source {i} must have 'type' field")
        if "config" not in source:
            raise ConfigValidationError(f"Source {i} must have 'config' field")
        
        source_name = source["name"]
        if source_name in source_names:
            raise ConfigValidationError(f"Duplicate source name: {source_name}")
        source_names.append(source_name)
        
        # Validate source type
        source_type = source["type"]
        if source_type not in ["csv", "postgres"]:
            raise ConfigValidationError(
                f"Source {i} has invalid type '{source_type}'. Must be 'csv' or 'postgres'"
            )
        
        # Validate source config based on type
        source_config = source["config"]
        if not isinstance(source_config, dict):
            raise ConfigValidationError(f"Source {i} config must be a dictionary")
        
        if source_type == "csv":
            if "file_path" not in source_config:
                raise ConfigValidationError(
                    f"CSV source '{source_name}' must have 'file_path' in config"
                )
        elif source_type == "postgres":
            required_fields = ["host", "database", "table_name"]
            for field in required_fields:
                if field not in source_config:
                    raise ConfigValidationError(
                        f"PostgreSQL source '{source_name}' must have '{field}' in config"
                    )
    
    # Validate relationships (optional)
    if "relationships" in config:
        if not isinstance(config["relationships"], list):
            raise ConfigValidationError("'relationships' must be a list")
        
        for i, rel in enumerate(config["relationships"]):
            if not isinstance(rel, dict):
                raise ConfigValidationError(f"Relationship {i} must be a dictionary")
            
            # Validate 'from' field
            if "from" not in rel:
                raise ConfigValidationError(f"Relationship {i} must have 'from' field")
            from_field = rel["from"]
            if not isinstance(from_field, dict):
                raise ConfigValidationError(f"Relationship {i} 'from' must be a dictionary")
            if "source" not in from_field or "column" not in from_field:
                raise ConfigValidationError(
                    f"Relationship {i} 'from' must have 'source' and 'column' fields"
                )
            
            # Validate 'to' field
            if "to" not in rel:
                raise ConfigValidationError(f"Relationship {i} must have 'to' field")
            to_field = rel["to"]
            if not isinstance(to_field, dict):
                raise ConfigValidationError(f"Relationship {i} 'to' must be a dictionary")
            if "source" not in to_field or "column" not in to_field:
                raise ConfigValidationError(
                    f"Relationship {i} 'to' must have 'source' and 'column' fields"
                )
            
            # Validate that referenced sources exist
            from_source = from_field["source"]
            to_source = to_field["source"]
            
            if from_source not in source_names:
                raise ConfigValidationError(
                    f"Relationship {i} references unknown source '{from_source}'"
                )
            if to_source not in source_names:
                raise ConfigValidationError(
                    f"Relationship {i} references unknown source '{to_source}'"
                )
            
            # For postgres sources, validate table name in 'to' field
            to_source_obj = next(s for s in config["sources"] if s["name"] == to_source)
            if to_source_obj["type"] == "postgres":
                if "table" not in to_field:
                    raise ConfigValidationError(
                        f"Relationship {i} 'to' must have 'table' field for PostgreSQL source"
                    )
                # Validate table name matches source config
                expected_table = to_source_obj["config"]["table_name"]
                if to_field["table"] != expected_table:
                    raise ConfigValidationError(
                        f"Relationship {i} 'to' table '{to_field['table']}' doesn't match "
                        f"source config table '{expected_table}'"
                    )


def get_config_from_path(config_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Get config from path, with fallback to default location
    
    Args:
        config_path: Optional path to config file. If None, looks for pipeline_config.yaml
                     in project root (datagrep directory)
        
    Returns:
        Parsed config dictionary or None if not found
    """
    if config_path is None:
        # Try to find config in datagrep directory
        # Get the backend directory and go up one level
        backend_dir = Path(__file__).parent.parent
        datagrep_dir = backend_dir.parent
        config_path = datagrep_dir / "pipeline_config.yaml"
        config_path = str(config_path)
    
    if not os.path.exists(config_path):
        return None
    
    return parse_config_file(config_path)
