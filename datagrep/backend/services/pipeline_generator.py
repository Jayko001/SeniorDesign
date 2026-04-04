"""
#edited by dhiren
Pipeline Generator Service
Uses OpenAI to generate data pipelines from natural language
"""

import os
from openai import OpenAI
from openai import APIConnectionError, APIError, RateLimitError
from typing import Dict, Any, List, Optional
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_openai_client() -> OpenAI:
    """Get OpenAI client with API key validation"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError(
            "OPENAI_API_KEY environment variable is not set. "
            "Please set it in your .env file (root directory) or export it as an environment variable."
        )
    if api_key.startswith("your_") or "example" in api_key.lower():
        raise ValueError("OPENAI_API_KEY appears to be a placeholder. Please set your actual API key.")
    return OpenAI(api_key=api_key)


def _has_relational_catalog(schema: Dict[str, Any]) -> bool:
    return isinstance(schema.get("tables"), list) and len(schema.get("tables", [])) > 0


def _table_columns_from_schema(schema: Dict[str, Any]) -> Dict[str, set]:
    if not _has_relational_catalog(schema):
        return {}
    return {
        table.get("name", ""): {column.get("name") for column in table.get("columns", [])}
        for table in schema.get("tables", [])
    }


def _get_semantic_hints(schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    hints = schema.get("semantic_hints", [])
    return hints if isinstance(hints, list) else []


def _find_metric_hint(schema: Dict[str, Any], metric_name: str) -> Optional[Dict[str, Any]]:
    for hint in _get_semantic_hints(schema):
        if hint.get("metric") == metric_name:
            return hint
    return None


def _is_total_revenue_request(natural_language: str) -> bool:
    text = natural_language.lower()
    if "gross revenue" in text or "gross sales" in text:
        return False
    if "total revenue" in text or "total sales" in text or "net revenue" in text:
        return True
    if "revenue" not in text and "sales" not in text:
        return False

    chart_terms = ["chart", "plot", "graph", "dashboard", "visualize", "visualization", "show"]
    dimension_terms = [" by ", " per ", " over ", "trend", "daily", "weekly", "monthly", "quarterly", "yearly"]
    return any(term in text for term in chart_terms) and not any(term in text for term in dimension_terms)


def _is_average_order_value_by_product_request(natural_language: str) -> bool:
    text = natural_language.lower()
    aov_terms = ["average order value", "avg order value", "aov"]
    product_terms = ["by product", "per product", "product-wise", "product wise"]
    return any(term in text for term in aov_terms) and any(term in text for term in product_terms)


def _build_total_revenue_query(schema: Dict[str, Any]) -> Optional[str]:
    table_columns = _table_columns_from_schema(schema)
    has_order_items_price = "price_usd" in table_columns.get("order_items", set())
    has_order_item_refunds = "refund_amount_usd" in table_columns.get("order_item_refunds", set())
    has_orders_price = "price_usd" in table_columns.get("orders", set())

    if has_order_items_price and has_order_item_refunds:
        return """
WITH gross_sales AS (
    SELECT COALESCE(SUM(price_usd), 0) AS gross_revenue_usd
    FROM order_items
),
refunds AS (
    SELECT COALESCE(SUM(refund_amount_usd), 0) AS refunded_revenue_usd
    FROM order_item_refunds
)
SELECT
    (gross_sales.gross_revenue_usd - refunds.refunded_revenue_usd) AS total_revenue_usd
FROM gross_sales
CROSS JOIN refunds
""".strip()

    if has_orders_price:
        return """
SELECT COALESCE(SUM(price_usd), 0) AS total_revenue_usd
FROM orders
""".strip()

    return None


def _build_average_order_value_by_product_query(schema: Dict[str, Any]) -> Optional[str]:
    table_columns = _table_columns_from_schema(schema)
    required_columns = [
        ("orders", "order_id"),
        ("orders", "price_usd"),
        ("order_items", "order_id"),
        ("order_items", "product_id"),
        ("products", "product_id"),
        ("products", "product_name"),
    ]
    if not all(column in table_columns.get(table, set()) for table, column in required_columns):
        return None

    return """
WITH product_orders AS (
    SELECT DISTINCT
        oi.product_id,
        o.order_id,
        o.price_usd
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE oi.product_id IS NOT NULL
      AND o.price_usd IS NOT NULL
)
SELECT
    p.product_name,
    AVG(product_orders.price_usd) AS average_order_value_usd
FROM product_orders
JOIN products p ON product_orders.product_id = p.product_id
GROUP BY p.product_name
ORDER BY average_order_value_usd DESC, p.product_name
""".strip()


def _build_postgres_query_pipeline(
    query: str,
    description: str,
    steps: List[str],
) -> Dict[str, Any]:
    code = f"""import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor

conn = None
cur = None

try:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        sslmode="require",
    )
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(\"\"\"{query}\"\"\")
    rows = cur.fetchall()
    print(json.dumps(rows, default=str))
except Exception as e:
    print(f"Error: {{e}}")
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
"""

    return {
        "code": code,
        "language": "python",
        "description": description,
        "steps": steps,
        "dependencies": ["psycopg2-binary"],
        "source_type": "postgres",
        "model_used": "semantic-rule",
    }


def _build_semantic_postgres_pipeline(
    natural_language: str,
    schema: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Use deterministic SQL for high-confidence metrics so chart requests do not
    depend on an LLM guessing business logic from partial schema context.
    """
    if not _has_relational_catalog(schema):
        return None

    if _is_total_revenue_request(natural_language) and _find_metric_hint(schema, "total_revenue_usd"):
        revenue_query = _build_total_revenue_query(schema)
        if revenue_query:
            return _build_postgres_query_pipeline(
                query=revenue_query,
                description="Deterministic total revenue pipeline generated from inferred relational schema",
                steps=[
                    "Infer multi-table PostgreSQL schema and semantic hints",
                    "Use the ecommerce revenue definition grounded in order_items and refunds",
                    "Return a single revenue metric suitable for chart generation",
                ],
            )

    if (
        _is_average_order_value_by_product_request(natural_language)
        and _find_metric_hint(schema, "average_order_value_usd_by_product")
    ):
        average_order_value_query = _build_average_order_value_by_product_query(schema)
        if average_order_value_query:
            return _build_postgres_query_pipeline(
                query=average_order_value_query,
                description="Deterministic average order value by product pipeline generated from inferred relational schema",
                steps=[
                    "Infer multi-table PostgreSQL schema and semantic hints",
                    "Deduplicate order-product pairs through order_items",
                    "Average orders.price_usd by products.product_name for chart-ready output",
                ],
            )

    return None


async def generate_pipeline(
    natural_language: str,
    source_type: str,
    schema: Dict[str, Any],
    source_config: Dict[str, Any],
    transformations: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Generate pipeline code from natural language request
    
    Args:
        natural_language: User's request in plain English
        source_type: "csv" or "postgres"
        schema: Inferred schema from data source
        source_config: Configuration for data source
        transformations: Optional list of specific transformations
        
    Returns:
        Dictionary containing generated pipeline code and metadata
    """
    semantic_pipeline = _build_semantic_postgres_pipeline(
        natural_language=natural_language,
        schema=schema,
    )
    if semantic_pipeline:
        return semantic_pipeline
    
    # Build prompt for OpenAI
    prompt = _build_pipeline_prompt(
        natural_language=natural_language,
        source_type=source_type,
        schema=schema,
        source_config=source_config,
        transformations=transformations
    )
    
    # Get OpenAI client
    try:
        client = get_openai_client()
    except ValueError as e:
        raise Exception(f"OpenAI API key not configured: {str(e)}")
    
    # Try GPT-4 first, fallback to GPT-3.5-turbo if unavailable
    models_to_try = ["gpt-4", "gpt-3.5-turbo"]
    last_error = None
    
    for model in models_to_try:
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert data engineer. Generate production-ready data pipeline code based on user requirements. Always return valid Python or SQL code."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.3,  # Lower temperature for more deterministic code
                max_tokens=2000
            )
            
            generated_code = response.choices[0].message.content
            
            # Parse the response to extract code and metadata
            pipeline = _parse_pipeline_response(generated_code, source_type)
            
            return {
                "code": pipeline["code"],
                "language": pipeline["language"],
                "description": pipeline.get("description", ""),
                "steps": pipeline.get("steps", []),
                "dependencies": pipeline.get("dependencies", []),
                "source_type": source_type,
                "model_used": model
            }
        
        except APIConnectionError as e:
            last_error = f"Connection error: Unable to connect to OpenAI API. Please check your internet connection and API key. Details: {str(e)}"
            # Don't try next model on connection error
            break
        except RateLimitError as e:
            last_error = f"Rate limit error: {str(e)}. Please try again later."
            # Don't try next model on rate limit
            break
        except APIError as e:
            last_error = f"OpenAI API error: {str(e)}"
            # Try next model if this one fails
            continue
        except Exception as e:
            last_error = f"Unexpected error: {str(e)}"
            # Try next model
            continue
    
    # If we get here, all models failed
    raise Exception(f"Failed to generate pipeline: {last_error}")


def _build_pipeline_prompt(
    natural_language: str,
    source_type: str,
    schema: Dict[str, Any],
    source_config: Dict[str, Any],
    transformations: Optional[List[str]] = None
) -> str:
    """Build the prompt for OpenAI"""
    
    schema_str = json.dumps(schema, indent=2, default=str)
    
    prompt = f"""Generate a data pipeline based on the following requirements:

USER REQUEST:
{natural_language}

DATA SOURCE TYPE: {source_type}

SCHEMA:
{schema_str}

SOURCE CONFIG:
{json.dumps(source_config, indent=2, default=str)}

"""
    
    if transformations:
        prompt += f"SPECIFIC TRANSFORMATIONS REQUESTED:\n{json.dumps(transformations, indent=2, default=str)}\n\n"
    
    if source_type == "csv":
        # Extract filename from source_config
        file_path = source_config.get("file_path", "")
        csv_filename = os.path.basename(file_path) if file_path else "data.csv"
        csv_mount_path = f"/data/{csv_filename}"
        
        prompt += f"""Generate a Python pipeline that:
1. Reads the CSV file from {csv_mount_path} (the file is mounted at this exact path)
2. Performs the requested transformations (filters, joins, aggregations, etc.)
3. Outputs the result using print() statements - the output will be captured automatically

IMPORTANT FOR EXECUTION:
- The CSV file is mounted at: {csv_mount_path}
- Use this EXACT path in your code: file_path = '{csv_mount_path}'
- Use print() to output results (e.g., print(df.head()), print(df.describe()), print(result.to_dict()))
- For structured data, print as JSON: print(json.dumps(result.to_dict(orient='records')))
- The code will be executed in a sandbox with pandas, psycopg2, and numpy available
- PostgreSQL connection is available via environment variables: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

Include:
- Error handling with try/except blocks
- Data validation
- Clear comments
- Use print() to show results (not file writes)
- Use the exact file path: {csv_mount_path}

IMPORTANT: Return ONLY the Python code directly. Do NOT wrap it in JSON or markdown code blocks. 
Just output the raw Python code that can be executed directly.
"""
    elif source_type == "postgres":
        relational_catalog = _has_relational_catalog(schema)
        prompt += """Generate a Python pipeline (NOT raw SQL) that:
1. Connects to PostgreSQL using psycopg2 with credentials from environment variables:
   POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
2. Executes the required SQL to satisfy the user request
3. Prints results as JSON (use json.dumps with default=str)

Execution environment details:
- psycopg2 and json are available
- SSL is required; pass sslmode="require" in psycopg2.connect
- The code runs inside a sandbox container; do not read/write local files

Include in the generated code:
- Small debug prints: show host/port/db/user before connecting
- Connect with psycopg2.connect(..., sslmode="require")
- Use RealDictCursor to get dict rows
- try/except/finally with safe cleanup; initialize conn/cur to None
- If an error occurs, print the error message
- Print the final result as JSON on the LAST line (so it can be parsed)

IMPORTANT: Return ONLY the Python code directly. Do NOT wrap it in JSON or markdown code blocks.
Just output raw executable Python.
"""
        if relational_catalog:
            prompt += """

The SCHEMA above is a relational catalog of multiple tables, not just one table.
Follow these rules strictly:
- Use ONLY the tables, columns, and foreign-key relationships listed in SCHEMA.
- NEVER invent tables, columns, joins, or metrics.
- If SCHEMA includes semantic_hints, treat them as the source of truth for ambiguous business terms.
- If the user asks for revenue/sales and SCHEMA includes total_revenue_usd guidance, use that exact definition unless the user explicitly requests a different revenue definition.
- If the user asks for average order value by product and SCHEMA includes average_order_value_usd_by_product guidance, use that exact definition and do not invent columns such as order_items.order_value.
- If the request is for a chart/plot/graph/dashboard and no dimension is specified, return a single-row result with a clearly named metric column such as total_revenue_usd.
- Prefer joins that follow the listed foreign-key relationships.
- Do not use unrelated tables such as employees or departments for ecommerce metrics.
"""
    
    return prompt


async def generate_multi_source_pipeline(
    natural_language: str,
    unified_schema: Dict[str, Any],
    transformations: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Generate pipeline code for multiple data sources with defined relationships.

    Args:
        natural_language: User's request in plain English
        unified_schema: Output from build_unified_schema (sources + relationships)
        transformations: Optional list of specific transformations

    Returns:
        Dictionary containing generated pipeline code and metadata
    """
    prompt = _build_multi_source_prompt(
        natural_language=natural_language,
        unified_schema=unified_schema,
        transformations=transformations,
    )

    try:
        client = get_openai_client()
    except ValueError as e:
        raise Exception(f"OpenAI API key not configured: {str(e)}")

    models_to_try = ["gpt-4", "gpt-3.5-turbo"]
    last_error = None

    for model in models_to_try:
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are an expert data engineer. Generate production-ready data pipeline "
                            "code that combines multiple data sources (PostgreSQL and CSV) using the "
                            "provided join relationships. Always return valid Python code."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,
                max_tokens=2000,
            )

            generated_code = response.choices[0].message.content
            pipeline = _parse_pipeline_response(generated_code, "python")

            return {
                "code": pipeline["code"],
                "language": pipeline["language"],
                "description": pipeline.get("description", ""),
                "steps": pipeline.get("steps", []),
                "dependencies": pipeline.get("dependencies", []),
                "source_type": "multi",
                "model_used": model,
            }

        except APIConnectionError as e:
            last_error = (
                f"Connection error: Unable to connect to OpenAI API. "
                f"Please check your internet connection and API key. Details: {str(e)}"
            )
            break
        except RateLimitError as e:
            last_error = f"Rate limit error: {str(e)}. Please try again later."
            break
        except APIError as e:
            last_error = f"OpenAI API error: {str(e)}"
            continue
        except Exception as e:
            last_error = f"Unexpected error: {str(e)}"
            continue

    raise Exception(f"Failed to generate pipeline: {last_error}")


def _build_multi_source_prompt(
    natural_language: str,
    unified_schema: Dict[str, Any],
    transformations: Optional[List[str]] = None,
) -> str:
    """Build the prompt for multi-source pipeline generation."""
    sources = unified_schema.get("sources", [])
    relationships = unified_schema.get("relationships", [])

    sources_section = []
    for src in sources:
        src_id = src["id"]
        src_type = src["type"]
        schema = src.get("schema", {})
        config = src.get("config", {})

        block = f"\n### Source: {src_id} (type: {src_type})\n"
        block += f"Config: {json.dumps(config, indent=2)}\n"
        block += f"Schema:\n{json.dumps(schema, indent=2)}\n"
        sources_section.append(block)

    rels_str = json.dumps(relationships, indent=2)

    prompt = f"""Generate a data pipeline that combines MULTIPLE data sources based on the following:

USER REQUEST:
{natural_language}

DATA SOURCES:
{"".join(sources_section)}

RELATIONSHIPS (use these for JOINs - from.column joins to to.column):
{rels_str}
"""

    if transformations:
        prompt += f"\nSPECIFIC TRANSFORMATIONS REQUESTED:\n{json.dumps(transformations, indent=2)}\n\n"

    # Build execution instructions for each source type
    csv_instructions = []
    postgres_instructions = []
    csv_mounts = []

    for src in sources:
        src_id = src["id"]
        src_type = src["type"]
        config = src.get("config", {})
        if src_type == "csv":
            file_path = config.get("file_path", "")
            csv_filename = os.path.basename(file_path) if file_path else f"{src_id}.csv"
            csv_mount_path = f"/data/{csv_filename}"
            csv_mounts.append((src_id, csv_mount_path, file_path))
        elif src_type == "postgres":
            postgres_instructions.append((src_id, config.get("table_name", "")))

    # Build explicit load instructions per source
    prompt += "\nYOU MUST LOAD EACH SOURCE EXPLICITLY. Define a variable for each source:\n\n"

    for src_id, mount_path, _ in csv_mounts:
        prompt += f"- {src_id} = pd.read_csv('{mount_path}')\n"

    if postgres_instructions:
        prompt += "\nFor Postgres sources, you MUST (import os and psycopg2.extras):\n"
        prompt += "1. conn = psycopg2.connect(host=os.getenv('POSTGRES_HOST'), port=os.getenv('POSTGRES_PORT'), dbname=os.getenv('POSTGRES_DB'), user=os.getenv('POSTGRES_USER'), password=os.getenv('POSTGRES_PASSWORD'), sslmode='require')\n"
        prompt += "2. cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)\n"
        prompt += "3. cur.execute('SELECT * FROM table_name'); rows = cur.fetchall()\n"
        prompt += "4. df = pd.DataFrame(rows); cur.close(); conn.close()\n\n"
        for src_id, table_name in postgres_instructions:
            prompt += f"- {src_id} = pd.DataFrame(...)  # from query: SELECT * FROM {table_name}\n"

    prompt += """
CRITICAL: Every source variable (employees, departments, etc.) MUST be defined by your code.
Do NOT reference any DataFrame that you have not explicitly loaded from CSV or queried from Postgres.
If a source comes from Postgres, you MUST write the psycopg2 connection and query code to load it.

Then:
1. Join using RELATIONSHIPS: from.column = to.column (e.g., employees.dept_id = departments.id)
2. Apply the user's requested transformations
3. Output: print(json.dumps(result.to_dict(orient='records'), default=str))

- Include try/except and proper error handling
- Return ONLY raw Python code. No markdown, no code blocks.
"""

    return prompt


def _parse_pipeline_response(response: str, source_type: str) -> Dict[str, Any]:
    """Parse OpenAI response to extract pipeline code - expects direct code output, not JSON"""
    
    # Extract code directly (may be in code blocks or raw)
    code = None
    language = "python" if source_type == "csv" else "python" if source_type == "postgres" else "sql"
    
    # Look for code blocks first
    if "```python" in response:
        code_start = response.find("```python") + 9
        code_end = response.find("```", code_start)
        code = response[code_start:code_end].strip()
        language = "python"
    elif "```sql" in response:
        code_start = response.find("```sql") + 6
        code_end = response.find("```", code_start)
        code = response[code_start:code_end].strip()
        language = "sql"
    elif "```" in response:
        # Generic code block - extract content
        code_start = response.find("```") + 3
        code_end = response.find("```", code_start)
        code = response[code_start:code_end].strip()
        # Remove language identifier if present (e.g., "python\n" at start)
        lines = code.split('\n', 1)
        if len(lines) > 1 and lines[0] in ["python", "sql", "py"]:
            code = lines[1]
    else:
        # No code blocks - use response directly
        code = response.strip()
    
    return {
        "code": code,
        "language": language,
        "description": "Generated pipeline",
        "steps": [],
        "dependencies": []
    }
