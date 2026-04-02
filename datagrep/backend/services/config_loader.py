"""
Config Loader Service
Loads and validates multi-source pipeline configuration from YAML/JSON files or inline dict
"""

import os
from typing import Dict, Any, Union

try:
    import yaml
except ImportError:
    yaml = None


def load_pipeline_config(path_or_config: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Load pipeline config from file path or accept inline dict.

    Args:
        path_or_config: Either a file path (str) to YAML/JSON, or an inline config dict

    Returns:
        Validated config dictionary with 'sources' and 'relationships' keys

    Raises:
        ValueError: If config is invalid or file cannot be loaded
    """
    if isinstance(path_or_config, dict):
        config = path_or_config.copy()
    elif isinstance(path_or_config, str):
        if not os.path.exists(path_or_config):
            raise ValueError(f"Config file not found: {path_or_config}")

        with open(path_or_config, "r") as f:
            content = f.read()

        if path_or_config.endswith(".json"):
            import json
            config = json.loads(content)
        elif path_or_config.endswith((".yaml", ".yml")):
            if yaml is None:
                raise ValueError(
                    "PyYAML is required for YAML config files. Install with: pip install pyyaml"
                )
            config = yaml.safe_load(content)
        else:
            # Try YAML first, then JSON
            if yaml is not None:
                try:
                    config = yaml.safe_load(content)
                except yaml.YAMLError:
                    import json
                    config = json.loads(content)
            else:
                import json
                config = json.loads(content)
    else:
        raise ValueError("path_or_config must be a file path (str) or config dict")

    validate_config_structure(config)
    return config


def validate_config_structure(config: Dict[str, Any]) -> None:
    """
    Validate config has required structure: sources and relationships.
    Does NOT validate that relationship columns exist in schemas (that happens in unified_schema).

    Args:
        config: Config dict to validate

    Raises:
        ValueError: If structure is invalid
    """
    if not isinstance(config, dict):
        raise ValueError("Config must be a dictionary")

    sources = config.get("sources")
    if not sources:
        raise ValueError("Config must have a 'sources' key with at least one source")
    if not isinstance(sources, list):
        raise ValueError("'sources' must be a list")

    source_ids = set()
    for i, src in enumerate(sources):
        if not isinstance(src, dict):
            raise ValueError(f"Source at index {i} must be a dictionary")
        src_id = src.get("id")
        if not src_id:
            raise ValueError(f"Source at index {i} must have an 'id'")
        if src_id in source_ids:
            raise ValueError(f"Duplicate source id: {src_id}")
        source_ids.add(src_id)

        src_type = src.get("type")
        if src_type not in ("postgres", "csv"):
            raise ValueError(
                f"Source '{src_id}' has invalid type '{src_type}'. Must be 'postgres' or 'csv'"
            )

        src_config = src.get("config")
        if not isinstance(src_config, dict):
            raise ValueError(f"Source '{src_id}' must have a 'config' dictionary")

        if src_type == "csv" and "file_path" not in src_config:
            raise ValueError(f"CSV source '{src_id}' must have 'file_path' in config")
        if src_type == "postgres" and "table_name" not in src_config:
            raise ValueError(
                f"Postgres source '{src_id}' must have 'table_name' in config "
                "(or use env vars for connection)"
            )

    relationships = config.get("relationships")
    if relationships is None:
        config["relationships"] = []
        relationships = []
    if not isinstance(relationships, list):
        raise ValueError("'relationships' must be a list")

    for i, rel in enumerate(relationships):
        if not isinstance(rel, dict):
            raise ValueError(f"Relationship at index {i} must be a dictionary")
        frm = rel.get("from")
        to = rel.get("to")
        if not frm or not to:
            raise ValueError(
                f"Relationship at index {i} must have 'from' and 'to' with "
                "'source' and 'column' keys"
            )
        for side, label in [(frm, "from"), (to, "to")]:
            if not isinstance(side, dict):
                raise ValueError(f"Relationship '{label}' must be a dict with 'source' and 'column'")
            if "source" not in side or "column" not in side:
                raise ValueError(f"Relationship '{label}' must have 'source' and 'column'")
            if side["source"] not in source_ids:
                raise ValueError(
                    f"Relationship references unknown source '{side['source']}'. "
                    f"Valid source ids: {sorted(source_ids)}"
                )
