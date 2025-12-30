import uuid
from typing import Dict, Any, Optional

_PIPELINE_STORE: Dict[str, Dict[str, Any]] = {}


def save_pipeline(pipeline: Dict[str, Any]) -> str:
    pipeline_id = str(uuid.uuid4())
    _PIPELINE_STORE[pipeline_id] = pipeline
    return pipeline_id


def get_pipeline(pipeline_id: str) -> Optional[Dict[str, Any]]:
    return _PIPELINE_STORE.get(pipeline_id)
