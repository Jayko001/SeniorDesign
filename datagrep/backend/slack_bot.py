"""
Slack Bot PoC for Datagrep
Integrates with datagrep API to provide data pipeline generation via Slack
"""

import os
import tempfile
import asyncio
import concurrent.futures
import re
import time
from typing import Dict, Any, Optional
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
import json
import shutil
import requests

from services.schema_inference import infer_schema_csv
from services.pipeline_generator import generate_pipeline, generate_multi_source_pipeline
from services.config_loader import load_pipeline_config
from services.unified_schema import build_unified_schema
from services.code_executor import execute_python_code, execute_python_code_with_output
from services.visualization_generator import infer_visualization_spec, build_plot_code

load_dotenv()

# Initialize Slack app
app = App(token=os.environ.get("SLACK_BOT_TOKEN"))
client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))

# API base URL (can be configured via env var)
API_BASE_URL = os.environ.get("DATAGREP_API_URL", "http://localhost:8000")
PROCESSED_MESSAGE_TTL_SECONDS = 300
SHOW_SCHEMA_IN_SLACK = False
SHOW_EXECUTION_RESULTS_IN_SLACK = False
processed_messages: Dict[str, float] = {}
recent_files: Dict[str, tuple] = {}


def run_async(coro):
    """Helper to run async function from sync context"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If loop is running, create a new event loop in a thread
            def run_in_thread():
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(coro)
                finally:
                    new_loop.close()
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_thread)
                return future.result()
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        # No event loop, create new one
        return asyncio.run(coro)


def _prune_processed_messages(now: Optional[float] = None) -> None:
    """Drop old dedupe entries so the cache stays bounded."""
    current_time = now if now is not None else time.time()
    expired_keys = [
        key for key, seen_at in processed_messages.items()
        if current_time - seen_at > PROCESSED_MESSAGE_TTL_SECONDS
    ]
    for key in expired_keys:
        processed_messages.pop(key, None)


def _message_dedupe_key(body: Optional[Dict[str, Any]], message: Dict[str, Any]) -> Optional[str]:
    if body and body.get("event_id"):
        return f"event:{body['event_id']}"

    channel = message.get("channel") or ""
    user = message.get("user") or ""
    ts = message.get("client_msg_id") or message.get("ts") or ""
    text = message.get("text") or ""
    if not channel or not ts:
        return None
    return f"message:{channel}:{user}:{ts}:{text}"


def _is_duplicate_message(body: Optional[Dict[str, Any]], message: Dict[str, Any]) -> bool:
    key = _message_dedupe_key(body, message)
    if not key:
        return False

    now = time.time()
    _prune_processed_messages(now)
    if key in processed_messages:
        return True

    processed_messages[key] = now
    return False

# TODO: add SQL
def format_code_block(code: str, language: str = "python") -> str:
    """Format code as a Slack code block"""
    return f"```{language}\n{code}\n```"


def format_execution_summary(execution_result: Dict[str, Any]) -> str:
    """Format a concise execution summary for Slack."""
    status = execution_result.get("status", "unknown")
    execution_time = execution_result.get("execution_time", "n/a")
    is_success = status == "success"

    lines = [
        f"\n*Execution Results* :white_check_mark:" if is_success else "\n*Execution Results* :x:",
        f"*Status:* {status.capitalize()} ({execution_time}s)"
    ]

    if execution_result.get("error"):
        error = execution_result["error"]
        if len(error) > 1500:
            error = error[:1500] + "\n... (truncated)"
        lines.append(f"*Error:*\n{format_code_block(error, 'text')}")

    if execution_result.get("result_data"):
        result_json = json.dumps(execution_result["result_data"], indent=2, default=str)
        if len(result_json) > 1500:
            result_json = result_json[:1500] + "\n... (truncated)"
        lines.append(f"*Result Data:*\n{format_code_block(result_json, 'json')}")

    return "\n".join(lines)


def format_schema_response(schema: Dict[str, Any]) -> str:
    """Format schema information for Slack"""
    lines = ["*Schema Information:*"]
    
    if "columns" in schema:
        lines.append("\n*Columns:*")
        for col in schema.get("columns", [])[:10]:  # Limit to 10 columns
            col_info = f"  • *{col['name']}* ({col.get('type', 'unknown')})"
            if col.get("nullable"):
                col_info += " [nullable]"
            lines.append(col_info)
    
    if "row_count" in schema:
        lines.append(f"\n*Total rows:* {schema['row_count']}")
    
    if "sample_rows" in schema and schema["sample_rows"]:
        lines.append("\n*Sample rows:*")
        sample_json = json.dumps(schema["sample_rows"][:2], indent=2, default=str)
        lines.append(format_code_block(sample_json, "json"))
    
    return "\n".join(lines)


def make_say_in_thread(say, message: dict):
    """Create a say function that replies in the message's thread."""
    thread_ts = message.get("thread_ts") or message.get("ts")

    def say_in_thread(text, **kwargs):
        kwargs.setdefault("thread_ts", thread_ts)
        return say(text, **kwargs)

    return say_in_thread


@app.message("help")
def handle_help(message, say):
    """Show help message"""
    say = make_say_in_thread(say, message)
    help_text = """*Datagrep Slack Bot Commands:*

• `@datagrep generate pipeline: <description>` - Generate a data pipeline from natural language (auto-executes)
• `@datagrep generate multi-source pipeline: <description>` - Generate pipeline from multiple sources (Postgres + CSV) using config
• `@datagrep generate pipeline: <description> no execute` - Generate pipeline without executing
• `@datagrep infer schema: <file>` - Infer schema from uploaded CSV file
• `@datagrep help` - Show this help message

*Examples:*
• `@datagrep generate pipeline: Filter rows where age > 25 and group by department`
• Upload a CSV file and mention @datagrep with "infer schema"
• `@datagrep generate pipeline: Calculate average salary by department from employees.csv`
• `@datagrep generate multi-source pipeline: Join employees with departments and show average salary by department`

*Note:* 
- For CSV files, upload the file first, then reference it in your request
- Pipelines are automatically executed after generation unless you specify "no execute"
"""
    say(help_text)


def download_csv_file(file_id: str) -> str:
    """Download a CSV file from Slack and return temporary file path"""
    try:
        # Get file info to retrieve download URL
        file_info = client.files_info(file=file_id)["file"]
        url_private = file_info.get("url_private_download") or file_info.get("url_private")
        
        if not url_private:
            raise Exception("File URL not found")
        
        # Download file using requests with bot token for authentication
        headers = {
            "Authorization": f"Bearer {os.environ.get('SLACK_BOT_TOKEN')}"
        }
        
        response = requests.get(url_private, headers=headers)
        response.raise_for_status()
        
        # Save to temp file
        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".csv") as tmp_file:
            tmp_file.write(response.content)
            return tmp_file.name
    except Exception as e:
        raise Exception(f"Error downloading file: {str(e)}")


def find_csv_file(message: dict, channel_id: str) -> tuple:
    """Find CSV file from message attachments only (do not reuse old uploads)."""
    files = message.get("files", [])
    if files:
        for file_info in files:
            if file_info.get("mimetype") == "text/csv" or file_info.get("name", "").endswith(".csv"):
                return file_info["id"], file_info.get("name", "file.csv")
    return None, None


def wants_postgres(description: str) -> bool:
    """Heuristically detect postgres/supabase intent without requiring a CSV upload."""
    keywords = [
        "postgres", "postgre", "supabase", "table", "database", "db", "sql",
        "revenue", "sales", "refund", "orders", "products", "sessions", "pageviews"
    ]
    return any(word in description.lower() for word in keywords)


def wants_dashboard(description: str) -> bool:
    """Detect dashboard/visualization intent."""
    keywords = ["dashboard", "visualize", "visualization", "chart", "plot", "graph"]
    return any(word in description.lower() for word in keywords)


GENERIC_TABLE_TOKENS = {
    "postgres", "postgre", "postgresql", "supabase", "database", "db", "sql",
    "schema", "public", "table", "tables", "source", "data"
}
GENERIC_SOURCE_TOKENS = {"postgres", "postgre", "postgresql", "supabase", "database", "db", "sql"}
TABLE_NAME_FILLERS = {"the", "a", "an", "public", "schema"}


def _normalize_table_candidate(token: str) -> str:
    candidate = token.strip().strip(",.;:()[]{}<>\"'`")
    if candidate.startswith("public."):
        candidate = candidate.split(".", 1)[1]
    return candidate.lower()


def extract_table_name(description: str) -> str:
    """Very lightweight table name extractor (looks for 'table <name>' or 'from <name>')."""
    tokens = description.lower().split()
    for i, tok in enumerate(tokens):
        if tok not in {"table", "from"}:
            continue
        for offset, candidate_token in enumerate(tokens[i + 1:i + 5], start=1):
            candidate = _normalize_table_candidate(candidate_token)
            if not candidate or candidate in TABLE_NAME_FILLERS or candidate in GENERIC_TABLE_TOKENS:
                if tok == "from" and offset == 1 and candidate in GENERIC_SOURCE_TOKENS:
                    break
                continue
            if re.fullmatch(r"[a-z_][a-z0-9_]*", candidate):
                return candidate
    return ""


def upload_image_to_slack(channel_id: str, image_path: str, title: str = "Dashboard") -> Optional[str]:
    """Upload an image file to Slack. Returns error string on failure."""
    try:
        # files.upload is deprecated; use files_upload_v2
        client.files_upload_v2(
            channels=channel_id,
            file=image_path,
            filename=os.path.basename(image_path),
            title=title
        )
        return None
    except SlackApiError as e:
        err = e.response.get("error") if e.response else str(e)
        return err
    except Exception as e:
        return str(e)


@app.message("")
def handle_message(message, say, body=None):
    """Handle general messages"""
    text = message.get("text", "").lower()
    user_id = message.get("user")
    channel_id = message.get("channel")
    
    # Ignore bot messages
    if message.get("bot_id"):
        return

    if _is_duplicate_message(body, message):
        return
    
    # Reply in thread (or create thread from top-level message)
    say = make_say_in_thread(say, message)
    
    # Get bot user ID
    try:
        bot_user_id = client.auth_test()["user_id"]
    except:
        bot_user_id = None
    
    # Check if bot is mentioned
    bot_mentioned = bot_user_id and f"<@{bot_user_id}>" in message.get("text", "")
    if not bot_mentioned and not message.get("channel_type") == "im":
        return
    
    # Remove bot mention from text
    if bot_user_id:
        cleaned_text = text.replace(f"<@{bot_user_id}>", "").strip()
    else:
        cleaned_text = text.strip()
    
    if "help" in cleaned_text:
        handle_help(message, say)
        return
    
    # Acknowledge message
    say("Processing your request... :hourglass_flowing_sand:")
    
    # Find CSV file (from message or recent files)
    file_id, file_name = find_csv_file(message, channel_id)
    csv_file_path = None
    
    if file_id:
        try:
            csv_file_path = download_csv_file(file_id)
        except Exception as e:
            say(f"Error downloading file: {str(e)}")
            return
    
    # Parse command
    if "execute" in cleaned_text and ("pipeline" in cleaned_text or "code" in cleaned_text):
        # Manual execution command - will need code from previous generation
        # For now, we'll require a file to be present
        if csv_file_path:
            say("To execute a pipeline, first generate one with `generate pipeline: <description>`. Execution happens automatically after generation.")
        else:
            say("Please upload a CSV file and generate a pipeline first. Execution happens automatically after generation.")
        return
    
    if "generate pipeline" in cleaned_text or "create pipeline" in cleaned_text:
        # Extract natural language description
        desc_start = cleaned_text.find(":") + 1 if ":" in cleaned_text else len(cleaned_text)
        description = cleaned_text[desc_start:].strip()
        
        if not description:
            say("Please provide a description of the pipeline you want to generate.\nExample: `generate pipeline: Filter rows where age > 25`")
            return

        # Multi-source pipeline (uses config from sample_data)
        if "multi-source" in cleaned_text or "multisource" in cleaned_text:
            try:
                config_path = os.path.join(
                    os.path.dirname(__file__), "..", "sample_data", "pipeline_config_slack.yaml"
                )
                if not os.path.exists(config_path):
                    say(f"Config not found: {config_path}. Ensure sample_data/pipeline_config_slack.yaml exists.")
                    return
                config = load_pipeline_config(config_path)
                unified_schema = build_unified_schema(config)
                auto_execute = "no execute" not in cleaned_text.lower() and "skip execute" not in cleaned_text.lower()
                run_async(handle_multi_source_pipeline_slack(
                    say, description, unified_schema, config, auto_execute=auto_execute
                ))
            except Exception as e:
                say(f"Error generating multi-source pipeline: {str(e)}")
            return
        
        dashboard_requested = wants_dashboard(description)

        if csv_file_path:
            # Generate pipeline from CSV
            try:
                auto_execute = "no execute" not in cleaned_text.lower() and "skip execute" not in cleaned_text.lower()
                if dashboard_requested:
                    auto_execute = True
                run_async(handle_pipeline_generation(
                    say,
                    description,
                    csv_file_path,
                    auto_execute=auto_execute,
                    channel_id=channel_id,
                    wants_dashboard=dashboard_requested
                ))
            except Exception as e:
                say(f"Error generating pipeline: {str(e)}")
            finally:
                if csv_file_path and os.path.exists(csv_file_path):
                    os.remove(csv_file_path)
        elif wants_postgres(description):
            # Defer to API: let backend infer schema + generate against Postgres/Supabase
            try:
                table_name = extract_table_name(description)
                payload = {
                    "natural_language": description,
                    "source_type": "postgres",
                    "source_config": {"table_name": table_name} if table_name else {}
                }
                resp = requests.post(f"{API_BASE_URL}/api/pipeline/generate", json=payload)
                if not resp.ok:
                    try:
                        err_detail = resp.json()
                    except Exception:
                        err_detail = resp.text
                    raise Exception(f"{resp.status_code} {resp.reason}: {err_detail}")
                data = resp.json()
                pipeline = data.get("pipeline", {})
                response_parts = [
                    "*Pipeline Generated* :rocket:",
                    f"\n*Description:* {pipeline.get('description', 'N/A')}",
                    f"\n*Language:* {pipeline.get('language', 'python')}",
                    f"\n*Generated Code:*",
                    format_code_block(pipeline.get('code', ''), pipeline.get('language', 'python'))
                ]
                if SHOW_SCHEMA_IN_SLACK and data.get("schema"):
                    response_parts.append("\n*Schema (from Postgres/Supabase):*")
                    response_parts.append(format_code_block(json.dumps(data["schema"], indent=2, default=str), "json"))
                say("\n".join(response_parts))

                # Auto-execute for Postgres as well
                if pipeline.get("code"):
                    say("Executing pipeline against Postgres/Supabase... :hourglass_flowing_sand:")
                    db_config = {
                        "host": os.getenv("POSTGRES_HOST"),
                        "port": os.getenv("POSTGRES_PORT"),
                        "database": os.getenv("POSTGRES_DB"),
                        "user": os.getenv("POSTGRES_USER"),
                        "password": os.getenv("POSTGRES_PASSWORD"),
                        "supabase_url": os.getenv("SUPABASE_URL"),
                        "supabase_key": os.getenv("SUPABASE_KEY"),
                    }
                    try:
                        execution_result = run_async(execute_python_code(
                            code=pipeline.get("code", ""),
                            file_paths=None,
                            db_config=db_config,
                            timeout=60
                        ))
                        if SHOW_EXECUTION_RESULTS_IN_SLACK:
                            exec_parts = [f"\n*Execution Results* :white_check_mark:" if execution_result["status"] == "success" else "\n*Execution Results* :x:"]
                            exec_parts.append(f"*Status:* {execution_result['status'].capitalize()} ({execution_result['execution_time']}s)")
                            if execution_result.get("error"):
                                error = execution_result["error"]
                                if len(error) > 1500:
                                    error = error[:1500] + "\n... (truncated)"
                                exec_parts.append(f"*Error:*\n{format_code_block(error, 'text')}")
                            if execution_result.get("output"):
                                output = execution_result["output"]
                                if len(output) > 1500:
                                    output = output[:1500] + "\n... (truncated)"
                                exec_parts.append(f"*Output:*\n{format_code_block(output, 'text')}")
                            if execution_result.get("result_data"):
                                result_json = json.dumps(execution_result["result_data"], indent=2, default=str)
                                if len(result_json) > 1500:
                                    result_json = result_json[:1500] + "\n... (truncated)"
                                exec_parts.append(f"*Result Data:*\n{format_code_block(result_json, 'json')}")
                            say("\n".join(exec_parts))

                        if dashboard_requested and execution_result.get("result_data"):
                            generate_dashboard_and_upload(
                                say,
                                channel_id,
                                description,
                                execution_result.get("result_data")
                            )
                    except Exception as e:
                        say(f"*Execution Error:* {str(e)}")
            except Exception as e:
                say(f"Error generating Postgres pipeline: {str(e)}")
        else:
            say("Please upload a CSV file (or share one in this channel) to generate a pipeline from it, or mention postgres/supabase/table to target the database.")
    
    elif "infer schema" in cleaned_text or "schema" in cleaned_text:
        if csv_file_path:
            try:
                handle_schema_inference(say, csv_file_path)
            except Exception as e:
                say(f"Error inferring schema: {str(e)}")
            finally:
                if csv_file_path and os.path.exists(csv_file_path):
                    os.remove(csv_file_path)
        else:
            say("Please upload a CSV file (or share one in this channel) to infer its schema.")
    
    else:
        # Default: try to generate pipeline if CSV is uploaded
        if csv_file_path:
            try:
                run_async(handle_pipeline_generation(say, cleaned_text, csv_file_path, auto_execute=True))
            except Exception as e:
                say(f"Error: {str(e)}")
            finally:
                if csv_file_path and os.path.exists(csv_file_path):
                    os.remove(csv_file_path)
        else:
            say("I didn't understand that. Type `help` for available commands.")

async def handle_pipeline_generation(
    say,
    description: str,
    csv_file_path: str,
    auto_execute: bool = True,
    channel_id: Optional[str] = None,
    wants_dashboard: bool = False
):
    """Handle pipeline generation request"""
    try:
        # Infer schema first
        schema = infer_schema_csv({"file_path": csv_file_path})
        
        # Generate pipeline
        pipeline = await generate_pipeline(
            natural_language=description,
            source_type="csv",
            schema=schema,
            source_config={"file_path": csv_file_path},
            transformations=None
        )
        
        # Format response
        response_parts = [
            f"*Pipeline Generated* :rocket:",
            f"\n*Description:* {pipeline.get('description', 'N/A')}",
            f"\n*Language:* {pipeline.get('language', 'python')}",
            f"\n*Generated Code:*",
            format_code_block(pipeline.get("code", ""), pipeline.get("language", "python"))
        ]
        
        if pipeline.get("steps"):
            response_parts.append(f"\n*Steps:*\n" + "\n".join([f"  • {step}" for step in pipeline["steps"]]))
        
        if pipeline.get("dependencies"):
            response_parts.append(f"\n*Dependencies:* `{', '.join(pipeline['dependencies'])}`")
        
        say("\n".join(response_parts))
        
        # Auto-execute if requested
        if auto_execute and pipeline.get("code"):
            say("Executing pipeline... :hourglass_flowing_sand:")
            try:
                execution_result = await execute_python_code(
                    code=pipeline.get("code", ""),
                    file_paths=[csv_file_path],
                    db_config=None,
                    timeout=60
                )
                
                if SHOW_EXECUTION_RESULTS_IN_SLACK:
                    exec_parts = [f"\n*Execution Results* :white_check_mark:"]
                    
                    if execution_result["status"] == "success":
                        exec_parts.append(f"*Status:* Success ({execution_result['execution_time']}s)")
                        if execution_result.get("output"):
                            output = execution_result["output"]
                            # Truncate long outputs for Slack
                            if len(output) > 1500:
                                output = output[:1500] + "\n... (truncated)"
                            exec_parts.append(f"*Output:*\n{format_code_block(output, 'text')}")
                        if execution_result.get("result_data"):
                            result_json = json.dumps(execution_result["result_data"], indent=2, default=str)
                            if len(result_json) > 1500:
                                result_json = result_json[:1500] + "\n... (truncated)"
                            exec_parts.append(f"*Result Data:*\n{format_code_block(result_json, 'json')}")
                    else:
                        exec_parts.append(f"*Status:* Error ({execution_result['execution_time']}s)")
                        if execution_result.get("error"):
                            error = execution_result["error"]
                            if len(error) > 1500:
                                error = error[:1500] + "\n... (truncated)"
                            exec_parts.append(f"*Error:*\n{format_code_block(error, 'text')}")
                        if execution_result.get("output"):
                            output = execution_result["output"]
                            if len(output) > 500:
                                output = output[:500] + "\n... (truncated)"
                            exec_parts.append(f"*Output:*\n{format_code_block(output, 'text')}")
                    
                    say("\n".join(exec_parts))

                if wants_dashboard and channel_id and execution_result.get("result_data"):
                    generate_dashboard_and_upload(
                        say,
                        channel_id,
                        description,
                        execution_result.get("result_data")
                    )
            
            except Exception as e:
                say(f"*Execution Error:* {str(e)}")
    
    except Exception as e:
        say(f"Error generating pipeline: {str(e)}")
        raise

async def handle_multi_source_pipeline_slack(
    say, description: str, unified_schema: Dict[str, Any],
    config: Dict[str, Any], auto_execute: bool = True,
):
    """Handle multi-source pipeline generation from Slack (uses sample_data config)."""
    try:
        pipeline = await generate_multi_source_pipeline(
            natural_language=description,
            unified_schema=unified_schema,
            transformations=None,
        )

        response_parts = [
            "*Multi-Source Pipeline Generated* :rocket:",
            f"\n*Description:* {pipeline.get('description', 'N/A')}",
            f"\n*Sources:* {len(unified_schema['sources'])} source(s)",
            f"\n*Relationships:* {len(unified_schema.get('relationships', []))} relationship(s)",
            f"\n*Generated Code:*",
            format_code_block(pipeline.get("code", ""), pipeline.get("language", "python")),
        ]
        say("\n".join(response_parts))

        if auto_execute and pipeline.get("code"):
            say("Executing pipeline... :hourglass_flowing_sand:")
            file_paths = []
            db_config = None
            for src in unified_schema.get("sources", []):
                cfg = src.get("config", {})
                if src.get("type") == "csv" and cfg.get("file_path") and os.path.exists(cfg["file_path"]):
                    file_paths.append(cfg["file_path"])
                elif src.get("type") == "postgres" and db_config is None:
                    db_config = {
                        "host": os.getenv("POSTGRES_HOST"),
                        "port": os.getenv("POSTGRES_PORT"),
                        "database": os.getenv("POSTGRES_DB"),
                        "user": os.getenv("POSTGRES_USER"),
                        "password": os.getenv("POSTGRES_PASSWORD"),
                        "supabase_url": os.getenv("SUPABASE_URL"),
                        "supabase_key": os.getenv("SUPABASE_KEY"),
                        **cfg,
                    }
            try:
                execution_result = await execute_python_code(
                    code=pipeline.get("code", ""),
                    file_paths=file_paths if file_paths else None,
                    db_config=db_config,
                    timeout=60,
                )
                if SHOW_EXECUTION_RESULTS_IN_SLACK:
                    exec_parts = [f"\n*Execution Results* :white_check_mark:" if execution_result["status"] == "success" else "\n*Execution Results* :x:"]
                    exec_parts.append(f"*Status:* {execution_result['status'].capitalize()} ({execution_result['execution_time']}s)")
                    if execution_result.get("error"):
                        error = execution_result["error"]
                        if len(error) > 1500:
                            error = error[:1500] + "\n... (truncated)"
                        exec_parts.append(f"*Error:*\n{format_code_block(error, 'text')}")
                    if execution_result.get("output"):
                        output = execution_result["output"]
                        if len(output) > 1500:
                            output = output[:1500] + "\n... (truncated)"
                        exec_parts.append(f"*Output:*\n{format_code_block(output, 'text')}")
                    if execution_result.get("result_data"):
                        result_json = json.dumps(execution_result["result_data"], indent=2, default=str)
                        if len(result_json) > 1500:
                            result_json = result_json[:1500] + "\n... (truncated)"
                        exec_parts.append(f"*Result Data:*\n{format_code_block(result_json, 'json')}")
                    say("\n".join(exec_parts))
            except Exception as e:
                say(f"*Execution Error:* {str(e)}")
    except Exception as e:
        say(f"Error generating multi-source pipeline: {str(e)}")
        raise


def handle_schema_inference(say, csv_file_path: str):
    """Handle schema inference request"""
    try:
        schema = infer_schema_csv({"file_path": csv_file_path})
        response = format_schema_response(schema)
        say(response)
    except Exception as e:
        say(f"Error inferring schema: {str(e)}")


def generate_dashboard_and_upload(say, channel_id: str, description: str, result_data: Any):
    """Generate a visualization from result data and upload to Slack."""
    if not result_data:
        say("No data returned to visualize.")
        return

    try:
        viz_spec = infer_visualization_spec(description, result_data)
    except Exception as e:
        say(f"Error inferring visualization: {str(e)}")
        return

    plot_code = build_plot_code(viz_spec, result_data)
    output_dir = tempfile.mkdtemp(prefix="datagrep_viz_")
    try:
        os.chmod(output_dir, 0o777)
    except Exception:
        pass

    try:
        exec_result = run_async(execute_python_code_with_output(
            code=plot_code,
            output_dir=output_dir,
            file_paths=None,
            db_config={},  # uses env for any required defaults
            timeout=60
        ))
        if exec_result.get("status") != "success":
            say(f"Visualization failed: {exec_result.get('error')}")
            return

        image_path = os.path.join(output_dir, "plot.png")
        if os.path.exists(image_path):
            err = upload_image_to_slack(channel_id, image_path, title=viz_spec.get("title", "Dashboard"))
            if err:
                if err == "missing_scope":
                    say("Cannot upload image: Slack app is missing `files:write` scope. Add it, reinstall the app, then retry.")
                else:
                    say(f"Image upload failed: {err}")
        else:
            say("Visualization did not produce an image.")
    finally:
        try:
            shutil.rmtree(output_dir)
        except Exception:
            pass


# Note: Slack sends BOTH a message event AND an app_mention event for mentions.
# The message handler does the actual work. This no-op handler exists only to
# prevent Slack from retrying unhandled app_mention events.


@app.event("app_mention")
def ignore_app_mention_events(body, logger):
    logger.debug("Ignoring app_mention event %s because message listener already handled it.", body.get("event_id"))


@app.event("file_shared")
def handle_file_shared(event, say):
    """Handle when a file is shared"""
    file_id = event.get("file_id")
    channel_id = event.get("channel_id")
    
    try:
        file_info = client.files_info(file=file_id)["file"]
        if file_info.get("mimetype") == "text/csv" or file_info.get("name", "").endswith(".csv"):
            # Store this file as the most recent CSV for this channel
            recent_files[channel_id] = (file_id, file_info.get("name", "file.csv"))
            say(f"CSV file detected: *{file_info['name']}*. You can mention me with 'infer schema' or 'generate pipeline: <description>' to process it.")
    except Exception as e:
        say(f"Error processing file: {str(e)}")


@app.event("assistant_thread_started")
def handle_assistant_thread_started(event):
    """
    Handle when a new assistant thread is started (AI chat interface)
    This event fires when users open the split view AI chat
    """
    thread_id = event.get("thread_id")
    user_id = event.get("user")
    
    # You can optionally send a welcome message or set up context here
    # For now, we'll let the normal message handlers deal with it
    print(f"Assistant thread started: {thread_id} by user {user_id}")


def get_suggested_prompts() -> list:
    """
    Generate suggested prompts for the AI assistant interface
    This can be called dynamically or configured as fixed prompts in Slack settings
    Returns a list of prompt suggestions
    """
    return [
        {
            "text": "Generate pipeline: Calculate average salary by department",
            "description": "Create a pipeline that groups data by department and calculates average salary"
        },
        {
            "text": "Infer schema from CSV file",
            "description": "Analyze the structure and columns of a CSV file"
        },
        {
            "text": "Generate pipeline: Filter rows where age > 25",
            "description": "Create a pipeline to filter data based on conditions"
        },
        {
            "text": "Generate pipeline: Join two data sources",
            "description": "Create a pipeline to combine data from multiple sources"
        }
    ]


def main():
    """Start the Slack bot"""
    handler = SocketModeHandler(app, os.environ.get("SLACK_APP_TOKEN"))
    print("Starting Datagrep Slack Bot...")
    handler.start()


if __name__ == "__main__":
    main()
