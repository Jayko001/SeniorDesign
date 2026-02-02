"""
Unified Schema Service
Builds a unified schema from multiple sources and their relationships
"""

from typing import Dict, Any, List

from services.schema_inference import infer_schema_csv, infer_schema_postgres


def build_unified_schema(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build unified schema from config by inferring each source's schema
    and attaching relationships.

    Args:
        config: Pipeline config with 'sources' and 'relationships'

    Returns:
        {
            "sources": [
                { "id": str, "type": str, "schema": {...}, "config": {...} },
                ...
            ],
            "relationships": [
                { "from": { "source": str, "column": str }, "to": { "source": str, "column": str } },
                ...
            ]
        }

    Raises:
        ValueError: If schema inference fails or relationship columns don't exist
    """
    sources = config.get("sources", [])
    relationships = config.get("relationships", [])

    unified_sources: List[Dict[str, Any]] = []
    source_schemas: Dict[str, Dict[str, Any]] = {}

    for src in sources:
        src_id = src["id"]
        src_type = src["type"]
        src_config = src["config"]

        if src_type == "csv":
            schema = infer_schema_csv(src_config)
        elif src_type == "postgres":
            schema = infer_schema_postgres(src_config)
        else:
            raise ValueError(f"Unsupported source type: {src_type}")

        source_schemas[src_id] = schema
        unified_sources.append({
            "id": src_id,
            "type": src_type,
            "schema": schema,
            "config": src_config,
        })

    # Validate that relationship columns exist in their respective schemas
    _validate_relationships(source_schemas, relationships)

    return {
        "sources": unified_sources,
        "relationships": relationships,
    }


def _validate_relationships(
    source_schemas: Dict[str, Dict[str, Any]],
    relationships: List[Dict[str, Any]],
) -> None:
    """Ensure each relationship references existing columns."""
    for rel in relationships:
        frm = rel["from"]
        to = rel["to"]

        for side, label in [(frm, "from"), (to, "to")]:
            src_id = side["source"]
            col_name = side["column"]
            schema = source_schemas.get(src_id)
            if not schema:
                raise ValueError(f"Relationship references unknown source: {src_id}")

            columns = schema.get("columns", [])
            col_names = {c["name"] for c in columns}
            if col_name not in col_names:
                raise ValueError(
                    f"Relationship {label} references column '{col_name}' in source '{src_id}', "
                    f"but that column does not exist. Available columns: {sorted(col_names)}"
                )
