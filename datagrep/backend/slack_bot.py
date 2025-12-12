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

from services.schema_inference import infer_schema_csv
from services.pipeline_generator import generate_pipeline

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

• `@datagrep generate pipeline: <description>` - Generate a data pipeline from natural language
• `@datagrep infer schema: <file>` - Infer schema from uploaded CSV file
• `@datagrep help` - Show this help message

*Examples:*
• `@datagrep generate pipeline: Filter rows where age > 25 and group by department`
• Upload a CSV file and mention @datagrep with "infer schema"
• `@datagrep generate pipeline: Calculate average salary by department from employees.csv`

*Note:* For CSV files, upload the file first, then reference it in your request.
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
    if "generate pipeline" in cleaned_text or "create pipeline" in cleaned_text:
        # Extract natural language description
        desc_start = cleaned_text.find(":") + 1 if ":" in cleaned_text else len(cleaned_text)
        description = cleaned_text[desc_start:].strip()
        
        if not description:
            say("Please provide a description of the pipeline you want to generate.\nExample: `generate pipeline: Filter rows where age > 25`")
            return
        
        if csv_file_path:
            # Generate pipeline from CSV
            try:
                run_async(handle_pipeline_generation(say, description, csv_file_path))
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
                run_async(handle_pipeline_generation(say, cleaned_text, csv_file_path))
            except Exception as e:
                say(f"Error: {str(e)}")
            finally:
                if csv_file_path and os.path.exists(csv_file_path):
                    os.remove(csv_file_path)
        else:
            say("I didn't understand that. Type `help` for available commands.")


async def handle_pipeline_generation(say, description: str, csv_file_path: str):
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
    
    except Exception as e:
        say(f"Error generating pipeline: {str(e)}")
        raise


def handle_schema_inference(say, csv_file_path: str):
    """Handle schema inference request"""
    try:
        schema = infer_schema_csv({"file_path": csv_file_path})
        response = format_schema_response(schema)
        say(response)
    except Exception as e:
        say(f"Error inferring schema: {str(e)}")
        raise


# Note: app_mention events are handled by the message handler above
# which checks for bot mentions. We don't need a separate handler here.


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


def main():
    """Start the Slack bot"""
    handler = SocketModeHandler(app, os.environ.get("SLACK_APP_TOKEN"))
    print("Starting Datagrep Slack Bot...")
    handler.start()


if __name__ == "__main__":
    main()

