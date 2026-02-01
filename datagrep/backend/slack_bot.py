"""
Slack Bot PoC for Datagrep
Integrates with datagrep API to provide data pipeline generation via Slack
"""

import os
import tempfile
import asyncio
import concurrent.futures
from typing import Dict, Any
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk import WebClient
from dotenv import load_dotenv
import json
import requests

from services.schema_inference import infer_schema_csv, build_unified_schema
from services.pipeline_generator import generate_pipeline
from services.code_executor import execute_python_code
from services.config_parser import get_config_from_path

load_dotenv()

# Initialize Slack app
app = App(token=os.environ.get("SLACK_BOT_TOKEN"))
client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))

# API base URL (can be configured via env var)
API_BASE_URL = os.environ.get("DATAGREP_API_URL", "http://localhost:8000")

# Store recent CSV files per channel (file_id -> file_info)
recent_files = {}


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

# TODO: add SQL
def format_code_block(code: str, language: str = "python") -> str:
    """Format code as a Slack code block"""
    return f"```{language}\n{code}\n```"


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
        sample_json = json.dumps(schema["sample_rows"][:2], indent=2)
        lines.append(format_code_block(sample_json, "json"))
    
    return "\n".join(lines)


@app.message("help")
def handle_help(message, say):
    """Show help message"""
    help_text = """*Datagrep Slack Bot Commands:*

• `@datagrep generate pipeline: <description>` - Generate a data pipeline from natural language (auto-executes)
• `@datagrep generate pipeline: <description> no execute` - Generate pipeline without executing
• `@datagrep generate multi-source pipeline: <description>` - Generate multi-source pipeline using config file
• `@datagrep infer schema: <file>` - Infer schema from uploaded CSV file
• `@datagrep help` - Show this help message

*Examples:*
• `@datagrep generate pipeline: Filter rows where age > 25 and group by department`
• Upload a CSV file and mention @datagrep with "infer schema"
• `@datagrep generate pipeline: Calculate average salary by department from employees.csv`

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
    """Find CSV file from message attachments or recent files in channel"""
    # First, check if file is attached to this message
    files = message.get("files", [])
    if files:
        for file_info in files:
            if file_info.get("mimetype") == "text/csv" or file_info.get("name", "").endswith(".csv"):
                return file_info["id"], file_info.get("name", "file.csv")
    
    # If no file in message, try to find recent CSV file in this channel
    if channel_id in recent_files:
        file_id, file_name = recent_files[channel_id]
        try:
            # Verify file still exists
            file_info = client.files_info(file=file_id)["file"]
            if file_info.get("mimetype") == "text/csv" or file_info.get("name", "").endswith(".csv"):
                return file_id, file_info.get("name", "file.csv")
        except:
            # File no longer available, remove from cache
            del recent_files[channel_id]
    
    # Try to find recent files in conversation using files.list
    try:
        # Get recent CSV files (up to 5)
        response = client.files_list(
            types="csv",
            count=5
        )
        files_list = response.get("files", [])
        if files_list:
            # Filter files that might be from this channel (check if file has channel info)
            # Or just use the most recent CSV file
            recent_file = files_list[0]
            file_id = recent_file["id"]
            file_name = recent_file.get("name", "file.csv")
            # Cache it
            recent_files[channel_id] = (file_id, file_name)
            return file_id, file_name
    except Exception as e:
        # If files_list fails (e.g., no permission), continue
        pass
    
    return None, None


@app.message("")
def handle_message(message, say):
    """Handle general messages"""
    text = message.get("text", "").lower()
    user_id = message.get("user")
    channel_id = message.get("channel")
    
    # Ignore bot messages
    if message.get("bot_id"):
        return
    
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
    
    # Check for multi-source pipeline request
    if "generate multi-source pipeline" in cleaned_text or "multi-source pipeline" in cleaned_text:
        # Extract natural language description
        desc_start = cleaned_text.find(":") + 1 if ":" in cleaned_text else len(cleaned_text)
        description = cleaned_text[desc_start:].strip()
        
        if not description:
            say("Please provide a description of the multi-source pipeline you want to generate.\nExample: `generate multi-source pipeline: Join employees CSV with departments table`")
            return
        
        # Try to get config from default location
        try:
            config = get_config_from_path()
            if config is None:
                say("Config file not found. Please create `pipeline_config.yaml` in the datagrep directory with your sources and relationships.")
                return
            
            # Build unified schema
            unified_schema = build_unified_schema(config)
            
            # Build source_config dict
            source_configs = {}
            file_paths = []
            db_config = None
            
            for source in config["sources"]:
                source_name = source["name"]
                source_type = source["type"]
                source_cfg = source["config"]
                source_configs[source_name] = source_cfg
                
                if source_type == "csv":
                    file_path = source_cfg.get("file_path")
                    if file_path and os.path.exists(file_path):
                        file_paths.append(file_path)
                elif source_type == "postgres" and db_config is None:
                    db_config = source_cfg
            
            # Generate multi-source pipeline
            auto_execute = "no execute" not in cleaned_text.lower() and "skip execute" not in cleaned_text.lower()
            run_async(handle_multi_source_pipeline_generation(
                say, description, unified_schema, source_configs, file_paths, db_config, auto_execute
            ))
        except Exception as e:
            say(f"Error generating multi-source pipeline: {str(e)}")
    
    elif "generate pipeline" in cleaned_text or "create pipeline" in cleaned_text:
        # Extract natural language description
        desc_start = cleaned_text.find(":") + 1 if ":" in cleaned_text else len(cleaned_text)
        description = cleaned_text[desc_start:].strip()
        
        if not description:
            say("Please provide a description of the pipeline you want to generate.\nExample: `generate pipeline: Filter rows where age > 25`")
            return
        
        if csv_file_path:
            # Generate pipeline from CSV
            try:
                # Check if user wants to skip execution
                auto_execute = "no execute" not in cleaned_text.lower() and "skip execute" not in cleaned_text.lower()
                run_async(handle_pipeline_generation(say, description, csv_file_path, auto_execute=auto_execute))
            except Exception as e:
                say(f"Error generating pipeline: {str(e)}")
            finally:
                if csv_file_path and os.path.exists(csv_file_path):
                    os.remove(csv_file_path)
        else:
            say("Please upload a CSV file (or share one in this channel) to generate a pipeline from it.")
    
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


async def handle_pipeline_generation(say, description: str, csv_file_path: str, auto_execute: bool = True):
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
                
                # Format execution results
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
                        result_json = json.dumps(execution_result["result_data"], indent=2)
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
            
            except Exception as e:
                say(f"*Execution Error:* {str(e)}")
    
    except Exception as e:
        say(f"Error generating pipeline: {str(e)}")
        raise


async def handle_multi_source_pipeline_generation(
    say, description: str, unified_schema: Dict[str, Any], 
    source_configs: Dict[str, Any], file_paths: list, db_config: Dict[str, Any], 
    auto_execute: bool = True
):
    """Handle multi-source pipeline generation request"""
    try:
        # Generate pipeline with unified schema
        # Note: We pass unified_schema which has "sources" key, so pipeline generator should detect it
        pipeline = await generate_pipeline(
            natural_language=description,
            source_type="csv",  # Base type, but schema indicates multi-source
            schema=unified_schema,
            source_config=source_configs,
            transformations=None
        )
        
        # Format response
        response_parts = [
            f"*Multi-Source Pipeline Generated* :rocket:",
            f"\n*Description:* {pipeline.get('description', 'N/A')}",
            f"\n*Language:* {pipeline.get('language', 'python')}",
            f"\n*Sources:* {len(unified_schema['sources'])} source(s)",
            f"\n*Relationships:* {len(unified_schema.get('relationships', []))} relationship(s)",
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
                    file_paths=file_paths if file_paths else None,
                    db_config=db_config,
                    timeout=60
                )
                
                # Format execution results
                exec_parts = [f"\n*Execution Results* :white_check_mark:"]
                
                if execution_result["status"] == "success":
                    exec_parts.append(f"*Status:* Success ({execution_result['execution_time']}s)")
                    if execution_result.get("output"):
                        output = execution_result["output"]
                        if len(output) > 1500:
                            output = output[:1500] + "\n... (truncated)"
                        exec_parts.append(f"*Output:*\n{format_code_block(output, 'text')}")
                    if execution_result.get("result_data"):
                        result_json = json.dumps(execution_result["result_data"], indent=2)
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
            
            except Exception as e:
                say(f"*Execution Error:* {str(e)}")
    
    except Exception as e:
        say(f"Error generating multi-source pipeline: {str(e)}")
        raise

# TODO: respond in thread
def handle_schema_inference(say, csv_file_path: str):
    """Handle schema inference request"""
    try:
        schema = infer_schema_csv({"file_path": csv_file_path})
        response = format_schema_response(schema)
        say(response)
    except Exception as e:
        say(f"Error inferring schema: {str(e)}")
        raise


@app.event("app_mention")
def handle_app_mention_events(event, say):
    """Handle app_mention events (when bot is mentioned in a channel)"""
    # Convert app_mention event to message-like format for existing handler
    message = {
        "text": event.get("text", ""),
        "user": event.get("user"),
        "channel": event.get("channel"),
        "bot_id": None,  # Not a bot message
        "files": event.get("files", [])
    }
    # Use the existing message handler logic
    handle_message(message, say)


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

