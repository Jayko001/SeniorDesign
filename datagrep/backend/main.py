"""
Datagrep MVP - FastAPI Backend
Main API server for natural language to pipeline generation
"""

from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import os
import tempfile
from dotenv import load_dotenv

from services.schema_inference import infer_schema_csv, infer_schema_postgres
from services.pipeline_generator import generate_pipeline
from services.supabase_client import get_supabase_client

load_dotenv()

app = FastAPI(title="Datagrep API", version="0.1.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class PipelineRequest(BaseModel):
    """Request model for pipeline generation"""
    natural_language: str
    source_type: str  # "csv" or "postgres"
    source_config: Dict[str, Any]  # Connection details or file info
    transformations: Optional[List[str]] = None


class SchemaRequest(BaseModel):
    """Request model for schema inference"""
    source_type: str  # "csv" or "postgres"
    source_config: Dict[str, Any]


@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Datagrep API is running", "version": "0.1.0"}


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy"}


@app.post("/api/schema/infer")
async def infer_schema(request: SchemaRequest):
    """
    Infer schema from data source
    """
    try:
        if request.source_type == "csv":
            # For CSV, source_config should contain file_path or file_id
            schema = infer_schema_csv(request.source_config)
        elif request.source_type == "postgres":
            # For PostgreSQL, source_config should contain connection details
            schema = infer_schema_postgres(request.source_config)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported source type: {request.source_type}"
            )
        
        return {"schema": schema, "source_type": request.source_type}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/schema/infer-csv")
async def infer_schema_csv_upload(file: UploadFile = File(...)):
    """
    Infer schema from uploaded CSV file
    """
    try:
        # Save uploaded file temporarily
        temp_path = f"/tmp/{file.filename}"
        with open(temp_path, "wb") as f:
            content = await file.read()
            f.write(content)
        
        schema = infer_schema_csv({"file_path": temp_path})
        
        # Clean up temp file
        os.remove(temp_path)
        
        return {"schema": schema, "source_type": "csv"}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/pipeline/generate")
async def generate_pipeline_endpoint(request: PipelineRequest):
    """
    Generate pipeline from natural language request
    """
    try:
        # First, infer schema if not provided
        schema = None
        if request.source_type == "csv":
            # Check if file_path exists, if not, raise error
            file_path = request.source_config.get("file_path")
            if not file_path or not os.path.exists(file_path):
                raise HTTPException(
                    status_code=400,
                    detail=f"CSV file not found. Please upload the file first using /api/pipeline/generate-csv endpoint."
                )
            schema = infer_schema_csv(request.source_config)
        elif request.source_type == "postgres":
            schema = infer_schema_postgres(request.source_config)
        
        # Generate pipeline using LLM
        pipeline = await generate_pipeline(
            natural_language=request.natural_language,
            source_type=request.source_type,
            schema=schema,
            source_config=request.source_config,
            transformations=request.transformations
        )
        
        return {
            "pipeline": pipeline,
            "source_type": request.source_type,
            "schema": schema
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/pipeline/generate-csv")
async def generate_pipeline_csv(
    file: UploadFile = File(...),
    natural_language: str = Form(...),
    source_type: str = Form(default="csv")
):
    """
    Generate pipeline from uploaded CSV file and natural language request
    """
    temp_path = None
    try:
        # Save uploaded file temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
            content = await file.read()
            tmp_file.write(content)
            temp_path = tmp_file.name
        
        # Infer schema
        schema = infer_schema_csv({"file_path": temp_path})
        
        # Generate pipeline using LLM
        pipeline = await generate_pipeline(
            natural_language=natural_language,
            source_type=source_type,
            schema=schema,
            source_config={"file_path": temp_path},
            transformations=None
        )
        
        return {
            "pipeline": pipeline,
            "source_type": source_type,
            "schema": schema
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Clean up temp file
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except:
                pass


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

