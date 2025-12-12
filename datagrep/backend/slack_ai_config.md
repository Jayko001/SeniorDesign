# Slack AI Features Configuration Guide

## Split View (Already Enabled ✅)

You've already enabled split view in the Slack app settings. This provides:
- **Top bar entry point** - Users can access your AI assistant from Slack's top bar
- **Side-by-side view** - AI chat appears alongside Slack conversations
- **Chat and History tabs** - Replaces the Messages tab in app home

## Suggested Prompts Configuration

### Option 1: Fixed Prompts (Recommended for PoC)

1. Go to your Slack app settings: https://api.slack.com/apps
2. Navigate to **Agents & AI Apps** → **Suggested Prompts**
3. Select **Fixed**
4. Add up to 4 prompt suggestions:

```
Prompt 1: "Generate pipeline: Calculate average salary by department"
Description: "Create a pipeline that groups data by department and calculates average salary"

Prompt 2: "Infer schema from CSV file"
Description: "Analyze the structure and columns of a CSV file"

Prompt 3: "Generate pipeline: Filter rows where age > 25"
Description: "Create a pipeline to filter data based on conditions"

Prompt 4: "Generate pipeline: Join two data sources"
Description: "Create a pipeline to combine data from multiple sources"
```

### Option 2: Dynamic Prompts (Advanced)

For dynamic prompts that change based on context, you would need to implement an endpoint that responds to Slack's prompt suggestion requests. This requires:

1. Setting up an endpoint that Slack can call
2. Implementing logic to generate contextual prompts
3. Configuring the endpoint URL in Slack app settings

For the PoC, **Fixed prompts are recommended** as they're simpler and don't require additional infrastructure.

## Events You Can Handle

The bot code now handles these AI-related events:

- `assistant_thread_started` - Fires when a user opens the AI chat interface
- `message` - Handles all messages including those in the AI chat
- `file_shared` - Detects and processes CSV file uploads

## Testing the Split View

1. **Access the AI Assistant:**
   - Click on your app in Slack's top bar
   - Or open your app's home and click the "Chat" tab

2. **Test Suggested Prompts:**
   - When you open the chat, you should see the suggested prompts (if configured)
   - Click on a prompt to send it

3. **Test Normal Usage:**
   - Type messages normally
   - Upload CSV files
   - Ask for schema inference or pipeline generation

## Additional Features to Consider

### Loading States
You can show loading indicators while processing:
- The bot already sends "Processing your request... ⏳" messages
- You can enhance this with more detailed status updates

### App Threads
The split view supports threads for better conversation organization:
- Each conversation in the AI chat is automatically threaded
- The History tab shows past conversations

### Context Management
For advanced features, you can:
- Store conversation context
- Reference previous messages in the thread
- Maintain file context across messages

## Resources

- [Split View Documentation](https://docs.slack.dev/surfaces/split-view)
- [Developing Apps with AI Features](https://docs.slack.dev/ai/developing-apps-with-ai-features)
- [Slack API Reference](https://api.slack.com/reference)

