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
        prompt += """Generate a Python pipeline that:
1. Reads the CSV file
2. Performs the requested transformations (filters, joins, aggregations, etc.)
3. Outputs the result (can be to another CSV, database, or return as JSON)

Include:
- Error handling
- Data validation
- Clear comments
- Requirements/dependencies

Format your response as JSON with:
{
  "description": "Brief description of what the pipeline does",
  "language": "python",
  "code": "The complete Python code here",
  "steps": ["step1", "step2", ...],
  "dependencies": ["pandas", "numpy", ...]
}
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

Format your response as JSON with:
{
  "description": "Brief description of what the pipeline does",
  "language": "sql",
  "code": "The complete SQL code here",
  "steps": ["step1", "step2", ...],
  "dependencies": []
}
"""
    
    return prompt


def _parse_pipeline_response(response: str, source_type: str) -> Dict[str, Any]:
    """Parse OpenAI response to extract pipeline code"""
    
    # Try to extract JSON from response
    try:
        # Look for JSON code block
        if "```json" in response:
            json_start = response.find("```json") + 7
            json_end = response.find("```", json_start)
            json_str = response[json_start:json_end].strip()
        elif "```" in response:
            # Try to find any code block
            json_start = response.find("```") + 3
            json_end = response.find("```", json_start)
            json_str = response[json_start:json_end].strip()
            # Remove language identifier if present
            if json_str.startswith("json"):
                json_str = json_str[4:].strip()
        else:
            # Assume entire response is JSON
            json_str = response.strip()
        
        pipeline = json.loads(json_str)
        return pipeline
    
    except json.JSONDecodeError:
        # If JSON parsing fails, try to extract code directly
        # Look for Python code block
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
            code_start = response.find("```") + 3
            code_end = response.find("```", code_start)
            code = response[code_start:code_end].strip()
            language = source_type  # Default based on source
        else:
            code = response.strip()
            language = "python" if source_type == "csv" else "sql"
        
        return {
            "code": code,
            "language": language,
            "description": "Generated pipeline",
            "steps": [],
            "dependencies": []
        }

