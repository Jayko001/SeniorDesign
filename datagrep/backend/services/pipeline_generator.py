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
            
            # Detect if this is multi-source (unified schema)
            is_multi_source = "sources" in schema and isinstance(schema.get("sources"), list) and len(schema.get("sources", [])) > 1
            
            # Determine language - multi-source always uses Python
            if is_multi_source:
                language = "python"
            else:
                language = "python" if source_type == "csv" else "sql"
            
            # Parse the response to extract code and metadata
            pipeline = _parse_pipeline_response(generated_code, language)
            
            return {
                "code": pipeline["code"],
                "language": pipeline["language"],
                "description": pipeline.get("description", ""),
                "steps": pipeline.get("steps", []),
                "dependencies": pipeline.get("dependencies", []),
                "source_type": "multi" if is_multi_source else source_type,
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
    
    # Check if this is a unified/multi-source schema
    is_multi_source = "sources" in schema and isinstance(schema.get("sources"), list) and len(schema.get("sources", [])) > 1
    
    schema_str = json.dumps(schema, indent=2)
    
    prompt = f"""Generate a data pipeline based on the following requirements:

USER REQUEST:
{natural_language}

DATA SOURCE TYPE: {source_type if not is_multi_source else "multi-source (multiple data sources)"}

SCHEMA:
{schema_str}

SOURCE CONFIG:
{json.dumps(source_config, indent=2)}

"""
    
    if transformations:
        prompt += f"SPECIFIC TRANSFORMATIONS REQUESTED:\n{json.dumps(transformations, indent=2)}\n\n"
    
    # Handle multi-source pipelines
    if is_multi_source:
        sources = schema.get("sources", [])
        relationships = schema.get("relationships", [])
        
        prompt += """MULTI-SOURCE PIPELINE INSTRUCTIONS:

You are working with multiple data sources. Follow these steps:

1. LOAD DATA FROM ALL SOURCES:
"""
        
        csv_sources = []
        postgres_sources = []
        for source in sources:
            source_name = source["name"]
            source_type_src = source["type"]
            source_cfg = source.get("config", {})
            
            if source_type_src == "csv":
                file_path = source_cfg.get("file_path", "")
                csv_filename = os.path.basename(file_path) if file_path else f"{source_name}.csv"
                csv_mount_path = f"/data/{csv_filename}"
                csv_sources.append((source_name, csv_mount_path))
                prompt += f"   - Load CSV '{source_name}' from: {csv_mount_path}\n"
            elif source_type_src == "postgres":
                postgres_sources.append((source_name, source_cfg))
                table_name = source_cfg.get("table_name", "")
                prompt += f"   - Load PostgreSQL table '{source_name}' (table: {table_name}) using psycopg2\n"
        
        prompt += "\n2. JOIN/MERGE DATA USING RELATIONSHIPS:\n"
        if relationships:
            for rel in relationships:
                from_source = rel["from"]["source"]
                from_col = rel["from"]["column"]
                to_source = rel["to"]["source"]
                to_col = rel["to"]["column"]
                rel_type = rel.get("type", "foreign_key")
                rel_desc = rel.get("description", "")
                
                prompt += f"   - {from_source}.{from_col} -> {to_source}.{to_col} ({rel_type})"
                if rel_desc:
                    prompt += f" - {rel_desc}"
                prompt += "\n"
        else:
            prompt += "   - No explicit relationships defined. Use common sense to join based on column names and data types.\n"
        
        prompt += f"""
3. PERFORM REQUESTED TRANSFORMATIONS:
   - Apply filters, aggregations, and other transformations as requested
   - Use pandas for data manipulation when working with CSV data
   - Use SQL queries for PostgreSQL data, or load into pandas DataFrames for complex joins

4. OUTPUT RESULTS:
   - Use print() to output results (e.g., print(df.head()), print(df.describe()))
   - For structured data, print as JSON: print(json.dumps(result.to_dict(orient='records')))
   - The output will be captured automatically

IMPORTANT FOR EXECUTION:
"""
        
        if csv_sources:
            prompt += "- CSV files are mounted at /data/ with their filenames\n"
            for source_name, mount_path in csv_sources:
                prompt += f"  - {source_name}: {mount_path}\n"
        
        if postgres_sources:
            prompt += "- PostgreSQL connection is available via environment variables:\n"
            prompt += "  - POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD\n"
            prompt += "- Use psycopg2 to connect: conn = psycopg2.connect(host=os.getenv('POSTGRES_HOST'), ...)\n"
        
        prompt += """
- The code will be executed in a sandbox with pandas, psycopg2, and numpy available
- For multi-source joins, you can:
  * Load PostgreSQL data into pandas DataFrames and join with CSV data
  * Or use SQL to query PostgreSQL and then join results with CSV data in pandas

Include:
- Error handling with try/except blocks
- Data validation
- Clear comments explaining each step
- Use print() to show results (not file writes)

IMPORTANT: Return ONLY the Python code directly. Do NOT wrap it in JSON or markdown code blocks.
Just output the raw Python code that can be executed directly.
"""
    
    elif source_type == "csv":
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
        prompt += """Generate a SQL pipeline that:
1. Queries the PostgreSQL database
2. Performs the requested transformations (filters, joins, aggregations, etc.)
3. Can create views, materialized views, or output tables

Include:
- Proper SQL syntax
- Index suggestions if applicable
- Clear comments

IMPORTANT: Return ONLY the SQL code directly. Do NOT wrap it in JSON or markdown code blocks.
Just output the raw SQL code that can be executed directly.
"""
    
    return prompt


def _parse_pipeline_response(response: str, language: str = "python") -> Dict[str, Any]:
    """Parse OpenAI response to extract pipeline code - expects direct code output, not JSON"""
    
    # Extract code directly (may be in code blocks or raw)
    code = None
    
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

