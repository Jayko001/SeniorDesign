"""
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
    
    schema_str = json.dumps(schema, indent=2)
    
    prompt = f"""Generate a data pipeline based on the following requirements:

USER REQUEST:
{natural_language}

DATA SOURCE TYPE: {source_type}

SCHEMA:
{schema_str}

SOURCE CONFIG:
{json.dumps(source_config, indent=2)}

"""
    
    if transformations:
        prompt += f"SPECIFIC TRANSFORMATIONS REQUESTED:\n{json.dumps(transformations, indent=2)}\n\n"
    
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

IMPORTANT: Return ONLY the Python code directly. Do NOT wrap it in JSON or markdown code blocks.
Just output raw executable Python.
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
