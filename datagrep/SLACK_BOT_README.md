# Datagrep Slack Bot - Proof of Concept

A Slack bot that integrates with the Datagrep API to provide data pipeline generation capabilities directly within Slack.

## Features

- 🤖 **Natural Language Pipeline Generation**: Describe your data pipeline needs in plain English
- 📊 **Schema Inference**: Automatically infer schemas from CSV files shared in Slack
- 📁 **File Upload Support**: Upload CSV files directly to Slack and process them
- 💬 **Conversational Interface**: Interact with the bot via mentions or direct messages

## Prerequisites

1. **Slack Workspace**: You need a Slack workspace with admin permissions
2. **Slack App Creation**: Create a new Slack app at [api.slack.com/apps](https://api.slack.com/apps)
3. **Python Environment**: Python 3.8+ with pip
4. **Datagrep Backend**: The main Datagrep API should be running (or use the integrated services)

## Setup Instructions

### 1. Create a Slack App

1. Go to [api.slack.com/apps](https://api.slack.com/apps) and click "Create New App"
2. Choose "From scratch"
3. Name your app (e.g., "Datagrep Bot") and select your workspace
4. Click "Create App"

### 2. Configure OAuth & Permissions

1. In your app settings, go to **OAuth & Permissions** under "Features"
2. Under "Scopes" → "Bot Token Scopes", add the following scopes:
   - `app_mentions:read` - To listen for @mentions
   - `chat:write` - To send messages
   - `files:read` - To read uploaded files
   - `channels:history` - To read channel messages (if needed)
   - `im:history` - To read direct messages
   - `im:write` - To send direct messages

3. Scroll up and click "Install to Workspace"
4. Authorize the app and copy the **Bot User OAuth Token** (starts with `xoxb-`)

### 3. Enable Socket Mode

1. Go to **Socket Mode** under "Features" in your app settings
2. Toggle "Enable Socket Mode" to ON
3. Click "Generate" to create an App-Level Token
4. Name it (e.g., "datagrep-socket-token") and add scope: `connections:write`
5. Copy the token (starts with `xapp-`)

### 4. Subscribe to Events (Optional but Recommended)

1. Go to **Event Subscriptions** under "Features"
2. Toggle "Enable Events" to ON
3. Subscribe to bot events:
   - `app_mention` - When users mention the bot
   - `message.channels` - Messages in channels (if bot is in channel)
   - `message.im` - Direct messages
   - `file_shared` - When files are shared

### 5. Install Dependencies

```bash
cd backend
pip install -r requirements.txt
```

### 6. Configure Environment Variables

Copy the example env file and add your tokens:

```bash
cp env.example .env
```

Edit `.env` and add:

```env
# Slack Bot Configuration
SLACK_BOT_TOKEN=xoxb-your-bot-token-here
SLACK_APP_TOKEN=xapp-your-app-token-here
DATAGREP_API_URL=http://localhost:8000  # Optional, defaults to localhost:8000

# OpenAI API Key (required for pipeline generation)
OPENAI_API_KEY=your_openai_api_key_here
```

### 7. Run the Bot

```bash
cd backend
python slack_bot.py
```

You should see: `Starting Datagrep Slack Bot...`

### 8. Invite the Bot to Your Channel

1. Go to your Slack channel
2. Type `/invite @Datagrep Bot` (or whatever you named your bot)
3. The bot should join the channel

## Usage

### Generate a Pipeline

Upload a CSV file and mention the bot:

```
@datagrep generate pipeline: Filter rows where age > 25 and group by department
```

Or in a direct message:
```
generate pipeline: Calculate average salary by department from the CSV
```

### Infer Schema

Upload a CSV file and ask for schema:
```
@datagrep infer schema
```

Or:
```
@datagrep schema
```

### Get Help

```
@datagrep help
```

Or just type `help` in a direct message.

## How It Works

1. **Message Handling**: The bot listens for messages where it's mentioned or receives direct messages
2. **File Processing**: When a CSV file is uploaded, the bot downloads it temporarily
3. **Schema Inference**: If schema inference is requested, it uses the `schema_inference` service
4. **Pipeline Generation**: When a pipeline is requested, it:
   - Infers the schema from the CSV
   - Uses OpenAI GPT-4 to generate pipeline code based on natural language
   - Returns formatted code, steps, and dependencies
5. **Cleanup**: Temporary files are automatically deleted after processing

## Example Conversations

### Example 1: Basic Pipeline Generation

**User:** `@datagrep generate pipeline: Filter employees where salary > 50000`

**Bot:** 
```
Pipeline Generated 🚀

Description: Filter employees with salary greater than 50000
Language: python

Generated Code:
```python
import pandas as pd

df = pd.read_csv('employees.csv')
filtered_df = df[df['salary'] > 50000]
filtered_df.to_csv('output.csv', index=False)
```

Steps:
  • Read CSV file
  • Filter rows by salary
  • Save results

Dependencies: `pandas`
```

### Example 2: Schema Inference

**User:** (uploads employees.csv) `@datagrep infer schema`

**Bot:**
```
Schema Information:

Columns:
  • name (object) [nullable]
  • age (int64)
  • salary (float64)
  • department (object)

Total rows: 100

Sample rows:
```json
[
  {
    "name": "John Doe",
    "age": 30,
    "salary": 75000,
    "department": "Engineering"
  }
]
```
```

## Architecture

The Slack bot integrates directly with the Datagrep services:

```
Slack Message
    ↓
slack_bot.py (handles Slack events)
    ↓
services/
    ├── schema_inference.py (infers CSV/PostgreSQL schemas)
    └── pipeline_generator.py (generates pipelines via OpenAI)
```

## Troubleshooting

### Bot doesn't respond

- Check that the bot is invited to the channel
- Verify `SLACK_BOT_TOKEN` and `SLACK_APP_TOKEN` are set correctly
- Ensure Socket Mode is enabled in Slack app settings
- Check bot logs for errors

### File upload issues

- Verify `files:read` scope is added
- Check that the uploaded file is a CSV
- Ensure the bot has permission to download files from your workspace

### Pipeline generation fails

- Verify `OPENAI_API_KEY` is set correctly
- Check that the CSV file is valid
- Review error messages in bot logs

## Limitations (PoC)

This is a Proof of Concept with the following limitations:

- Only supports CSV files (PostgreSQL support can be added)
- Temporary file handling (files are deleted after processing)
- No persistent storage of generated pipelines
- Basic error handling
- No user authentication/authorization

## Future Enhancements

- [ ] Support for PostgreSQL database connections
- [ ] Persistent storage of generated pipelines
- [ ] Pipeline execution and testing
- [ ] Multi-file support
- [ ] Interactive pipeline editing
- [ ] Integration with Slack's AI features (split view, suggested prompts)
- [ ] User authentication and workspace management

## Resources

- [Slack Bolt for Python](https://slack.dev/bolt-python/)
- [Slack API Documentation](https://api.slack.com/)
- [Slack Socket Mode](https://api.slack.com/apis/connections/socket)
- [Datagrep Main README](../README.md)

## License

This is part of the Datagrep Senior Design project for the University of Cincinnati.

