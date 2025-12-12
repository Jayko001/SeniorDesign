"""
Schema Inference Service
Infers schema from CSV files and PostgreSQL databases
"""

import pandas as pd
import json
import numpy as np
from typing import Dict, Any, List
import psycopg2
from psycopg2.extras import RealDictCursor
import os


def _convert_to_native_type(value: Any) -> Any:
    """
    Convert numpy/pandas types to native Python types for JSON serialization
    """
    # Check for arrays/Series first before using pd.isna()
    if isinstance(value, (np.ndarray, pd.Series)):
        return value.tolist()
    elif isinstance(value, pd.Timestamp):
        return value.isoformat()
    elif isinstance(value, dict):
        return {k: _convert_to_native_type(v) for k, v in value.items()}
    elif isinstance(value, (list, tuple)):
        return [_convert_to_native_type(item) for item in value]
    # Now check for scalar NaN values (must be scalar, not array)
    try:
        if pd.isna(value):
            return None
    except (ValueError, TypeError):
        # pd.isna() fails on arrays, skip this check
        pass
    
    # Check for NaN using numpy for numeric types
    if isinstance(value, (float, np.floating)):
        try:
            if np.isnan(value):
                return None
        except (TypeError, ValueError):
            pass
    elif isinstance(value, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(value)
    elif isinstance(value, (np.floating, np.float64, np.float32, np.float16)):
        return float(value)
    elif isinstance(value, (np.bool_, bool)):
        return bool(value)
    else:
        return value


def infer_schema_csv(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Infer schema from CSV file
    
    Args:
        config: Dictionary containing 'file_path' or file data
        
    Returns:
        Dictionary with schema information
    """
    file_path = config.get("file_path")
    
    if not file_path or not os.path.exists(file_path):
        raise ValueError(f"CSV file not found: {file_path}")
    
    # Read CSV with pandas
    df = pd.read_csv(file_path, nrows=1000)  # Sample first 1000 rows
    
    # Convert sample rows to native types
    sample_rows = df.head(5).to_dict(orient="records")
    sample_rows = [_convert_to_native_type(row) for row in sample_rows]
    
    schema = {
        "columns": [],
        "row_count": int(len(df)),
        "sample_rows": sample_rows
    }
    
    # Infer column types and stats
    for col in df.columns:
        col_info = {
            "name": str(col),
            "type": str(df[col].dtype),
            "nullable": bool(df[col].isna().any()),
            "unique_count": int(df[col].nunique()),
            "sample_values": _convert_to_native_type(df[col].dropna().head(3).tolist())
        }
        
        # Add statistics for numeric columns
        if pd.api.types.is_numeric_dtype(df[col]):
            if not df[col].isna().all():
                col_info["min"] = _convert_to_native_type(df[col].min())
                col_info["max"] = _convert_to_native_type(df[col].max())
                col_info["mean"] = _convert_to_native_type(df[col].mean())
            else:
                col_info["min"] = None
                col_info["max"] = None
                col_info["mean"] = None
        
        schema["columns"].append(col_info)
    
    return schema


def infer_schema_postgres(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Infer schema from PostgreSQL database (Supabase)
    
    Args:
        config: Dictionary containing connection details:
            - host, port, database, user, password
            - OR supabase_url, supabase_key
            - table_name (optional, if specific table)
        
    Returns:
        Dictionary with schema information
    """
    # Support Supabase connection
    supabase_url = config.get("supabase_url")
    supabase_key = config.get("supabase_key")
    table_name = config.get("table_name")
    
    if supabase_url and supabase_key:
        # Use Supabase connection
        from supabase import create_client, Client
        supabase: Client = create_client(supabase_url, supabase_key)
        
        # If table_name specified, get schema for that table
        if table_name:
            # Get sample data
            response = supabase.table(table_name).select("*").limit(5).execute()
            sample_rows = response.data if response.data else []
            
            # Get column info (we'll infer from sample)
            if sample_rows:
                columns = []
                for key in sample_rows[0].keys():
                    col_info = {
                        "name": key,
                        "type": _infer_type_from_value(sample_rows[0][key]),
                        "nullable": any(row.get(key) is None for row in sample_rows),
                        "sample_values": [row.get(key) for row in sample_rows[:3] if row.get(key) is not None]
                    }
                    columns.append(col_info)
                
                return {
                    "table_name": table_name,
                    "columns": columns,
                    "sample_rows": sample_rows
                }
        else:
            # List all tables
            # Note: Supabase doesn't have a direct way to list tables via client
            # This would require direct PostgreSQL connection
            raise ValueError("table_name is required for Supabase schema inference")
    
    # Direct PostgreSQL connection
    conn_params = {
        "host": config.get("host"),
        "port": config.get("port", 5432),
        "database": config.get("database"),
        "user": config.get("user"),
        "password": config.get("password")
    }
    
    if not all([conn_params["host"], conn_params["database"], conn_params["user"]]):
        raise ValueError("Missing required PostgreSQL connection parameters")
    
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        if table_name:
            # Get schema for specific table
            cursor.execute(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row["column_name"],
                    "type": row["data_type"],
                    "nullable": row["is_nullable"] == "YES"
                })
            
            # Get sample rows
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
            sample_rows = [dict(row) for row in cursor.fetchall()]
            
            return {
                "table_name": table_name,
                "columns": columns,
                "sample_rows": sample_rows
            }
        else:
            # List all tables
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            tables = [row["table_name"] for row in cursor.fetchall()]
            return {"tables": tables}
    
    finally:
        cursor.close()
        conn.close()


def _infer_type_from_value(value: Any) -> str:
    """Infer PostgreSQL type from Python value"""
    if value is None:
        return "unknown"
    elif isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        return "integer"
    elif isinstance(value, float):
        return "numeric"
    elif isinstance(value, str):
        return "text"
    else:
        return "text"

