"""
Schema Inference Service
Infers schema from CSV files and PostgreSQL databases
"""

import pandas as pd
import json
import numpy as np
from typing import Dict, Any, List, Optional, Set, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql
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
    supabase_url = config.get("supabase_url") or os.getenv("SUPABASE_URL")
    supabase_key = config.get("supabase_key") or os.getenv("SUPABASE_KEY")
    table_name = config.get("table_name")
    
    if (supabase_url or supabase_key) and not (supabase_url and supabase_key):
        raise ValueError("Both supabase_url and supabase_key are required for Supabase schema inference")

    # The Supabase client can only infer a single table cheaply. For database-wide
    # inference we need a direct PostgreSQL connection so the model can reason over
    # related tables and avoid hallucinated joins/metrics.
    if supabase_url and supabase_key and table_name:
        # Use Supabase connection
        from supabase import create_client, Client
        supabase: Client = create_client(supabase_url, supabase_key)
        
        # If table_name specified, get schema for that table
        if table_name:
            # Get sample data
            response = supabase.table(table_name).select("*").limit(5).execute()
            sample_rows = response.data if response.data else []
            sample_rows = [_convert_to_native_type(row) for row in sample_rows]
            
            # Get column info (we'll infer from sample)
            if sample_rows:
                columns = []
                for key in sample_rows[0].keys():
                    col_info = {
                        "name": key,
                        "type": _infer_type_from_value(sample_rows[0][key]),
                        "nullable": any(row.get(key) is None for row in sample_rows),
                        "sample_values": _convert_to_native_type(
                            [row.get(key) for row in sample_rows[:3] if row.get(key) is not None]
                        )
                    }
                    columns.append(col_info)
                
                return {
                    "table_name": table_name,
                    "columns": columns,
                    "sample_rows": sample_rows
                }
    
    # Direct PostgreSQL connection
    conn_params = _build_postgres_connection_params(config)

    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        if table_name:
            # Get schema for specific table
            columns = _fetch_table_columns(cursor, table_name)
            sample_rows = _fetch_sample_rows(cursor, table_name)

            return {
                "table_name": table_name,
                "columns": columns,
                "sample_rows": sample_rows
            }
        else:
            tables = _fetch_all_table_schemas(cursor)
            relationships = _fetch_foreign_key_relationships(cursor)
            semantic_hints = _build_semantic_hints(tables, relationships)
            return {
                "database_schema": "public",
                "tables": tables,
                "relationships": relationships,
                "semantic_hints": semantic_hints
            }
    
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


def _derive_supabase_host(url: Optional[str]) -> Optional[str]:
    """Derive the direct Postgres host from a Supabase project URL."""
    if not url:
        return None
    try:
        ref = url.split("//", 1)[1].split(".", 1)[0]
        return f"db.{ref}.supabase.co"
    except Exception:
        return None


def _build_postgres_connection_params(config: Dict[str, Any]) -> Dict[str, Any]:
    port_value = config.get("port") or os.getenv("POSTGRES_PORT", 5432)
    try:
        port_value = int(port_value)
    except (TypeError, ValueError):
        pass

    supabase_url = config.get("supabase_url") or os.getenv("SUPABASE_URL")
    conn_params = {
        "host": (
            config.get("host")
            or os.getenv("POSTGRES_HOST")
            or _derive_supabase_host(supabase_url)
        ),
        "port": port_value,
        "database": config.get("database") or os.getenv("POSTGRES_DB") or "postgres",
        "user": config.get("user") or os.getenv("POSTGRES_USER") or "postgres",
        "password": config.get("password") or os.getenv("POSTGRES_PASSWORD")
    }

    if not all([conn_params["host"], conn_params["database"], conn_params["user"]]):
        raise ValueError("Missing required PostgreSQL connection parameters")
    if not conn_params["password"]:
        raise ValueError("Missing required PostgreSQL password")

    return conn_params


def _fetch_table_columns(cursor, table_name: str) -> List[Dict[str, Any]]:
    cursor.execute(
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table_name,)
    )

    columns = []
    for row in cursor.fetchall():
        columns.append({
            "name": row["column_name"],
            "type": row["data_type"],
            "nullable": row["is_nullable"] == "YES"
        })
    return columns


def _fetch_sample_rows(cursor, table_name: str, limit: int = 5) -> List[Dict[str, Any]]:
    query = sql.SQL("SELECT * FROM {} LIMIT {}").format(
        sql.Identifier(table_name),
        sql.Literal(limit),
    )
    cursor.execute(query)
    return [_convert_to_native_type(dict(row)) for row in cursor.fetchall()]


def _fetch_all_table_schemas(cursor) -> List[Dict[str, Any]]:
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    )
    table_names = [row["table_name"] for row in cursor.fetchall()]

    tables = []
    for name in table_names:
        tables.append({
            "name": name,
            "columns": _fetch_table_columns(cursor, name),
            "sample_rows": _fetch_sample_rows(cursor, name, limit=3)
        })
    return tables


def _fetch_foreign_key_relationships(cursor) -> List[Dict[str, str]]:
    cursor.execute(
        """
        SELECT
            tc.table_name AS from_table,
            kcu.column_name AS from_column,
            ccu.table_name AS to_table,
            ccu.column_name AS to_column
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = 'public'
        ORDER BY tc.table_name, kcu.column_name
        """
    )

    relationships = []
    for row in cursor.fetchall():
        relationships.append({
            "from_table": row["from_table"],
            "from_column": row["from_column"],
            "to_table": row["to_table"],
            "to_column": row["to_column"],
        })
    return relationships


def _build_semantic_hints(
    tables: List[Dict[str, Any]],
    relationships: List[Dict[str, str]],
) -> List[Dict[str, Any]]:
    """
    Add deterministic business hints for common analytics terms so LLM prompts
    can resolve ambiguous requests like "total revenue" without inventing logic.
    """
    table_columns = {
        table["name"]: {column["name"] for column in table.get("columns", [])}
        for table in tables
    }
    hints: List[Dict[str, Any]] = []

    has_order_items_price = _has_column(table_columns, "order_items", "price_usd")
    has_order_item_refunds = _has_column(table_columns, "order_item_refunds", "refund_amount_usd")
    has_orders_price = _has_column(table_columns, "orders", "price_usd")

    if has_order_items_price and has_order_item_refunds:
        hints.append({
            "metric": "total_revenue_usd",
            "business_terms": ["revenue", "sales", "total revenue", "net revenue"],
            "definition": (
                "Use item-level sales and subtract refunds. Gross sales come from "
                "order_items.price_usd and refunds come from "
                "order_item_refunds.refund_amount_usd."
            ),
            "sql_strategy": (
                "Compute total_revenue_usd as COALESCE(SUM(order_items.price_usd), 0) "
                "- COALESCE(SUM(order_item_refunds.refund_amount_usd), 0). "
                "Avoid double-counting by aggregating sales and refunds in separate "
                "subqueries before combining them."
            ),
            "preferred_time_column": (
                "Use order_items.created_at for item-level revenue trends. "
                "Use orders.created_at only when the chart is explicitly order-level."
            ),
            "guardrails": [
                "Do not use employees.salary or departments.budget for revenue.",
                "Do not invent a revenue column if none exists.",
            ],
        })
    elif has_orders_price:
        hints.append({
            "metric": "total_revenue_usd",
            "business_terms": ["revenue", "sales", "total revenue"],
            "definition": "Use orders.price_usd as order-level revenue when item-level data is unavailable.",
            "sql_strategy": "Compute total_revenue_usd as COALESCE(SUM(orders.price_usd), 0).",
            "guardrails": [
                "Do not use employees.salary or departments.budget for revenue.",
                "Do not invent a revenue column if none exists.",
            ],
        })

    if _has_column(table_columns, "products", "product_name") and has_order_items_price:
        hints.append({
            "dimension": "product",
            "definition": (
                "For revenue by product, join order_items.product_id to "
                "products.product_id and group by products.product_name."
            ),
        })

    if (
        _has_column(table_columns, "orders", "order_id")
        and _has_column(table_columns, "orders", "price_usd")
        and _has_column(table_columns, "order_items", "order_id")
        and _has_column(table_columns, "order_items", "product_id")
        and _has_column(table_columns, "products", "product_id")
        and _has_column(table_columns, "products", "product_name")
    ):
        hints.append({
            "metric": "average_order_value_usd_by_product",
            "business_terms": [
                "average order value by product",
                "avg order value by product",
                "aov by product",
            ],
            "definition": (
                "Use orders.price_usd as the order-level value. Join orders to order_items "
                "on order_id, map items to products via product_id, and average order totals "
                "per product after deduplicating each order-product pair."
            ),
            "sql_strategy": (
                "Build a product_orders CTE with DISTINCT product_id, order_id, and orders.price_usd. "
                "Then join to products and compute AVG(product_orders.price_usd) grouped by "
                "products.product_name as average_order_value_usd."
            ),
            "guardrails": [
                "Do not use a non-existent column such as order_items.order_value.",
                "Do not average order_items.price_usd when the request asks for order value.",
            ],
        })

    if _has_column(table_columns, "website_sessions", "utm_source") and _has_column(table_columns, "orders", "website_session_id"):
        hints.append({
            "dimension": "traffic_source",
            "definition": (
                "For revenue by acquisition source, join orders.website_session_id to "
                "website_sessions.website_session_id and group by session attributes "
                "such as utm_source, utm_campaign, or device_type."
            ),
        })

    if relationships:
        hints.append({
            "relationship_summary": [
                f"{rel['from_table']}.{rel['from_column']} -> {rel['to_table']}.{rel['to_column']}"
                for rel in relationships
            ]
        })

    return hints


def _has_column(table_columns: Dict[str, Set[str]], table_name: str, column_name: str) -> bool:
    return column_name in table_columns.get(table_name, set())
