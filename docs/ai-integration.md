# AI Integration Guide


## MCP Hosts
| Claude | Cursor | VS Code |
|:------:|:-------:|:-------:|


## 🖥️ Claude Desktop Integration

> [!TIP]
> You can add the configuration for the MCP server in Claude for Desktop for AI-assisted log analysis.

### 🛠️ Setup Instructions
1. To get Claude for Desktop and how to add an MCP server, access [this link](https://modelcontextprotocol.io/quickstart/user). 
2. Add the following code to your MCP client configuration. 
> [!NOTE]
> The GCP Cost Optimization MCP server uses the default GCP project from your ADC by default. Specify a value in `GCP_PROJECT_ID` if you want to use a different project. Similarly, adjust the `GCP_REGION` configuration values as per your setup.
```json
{
  "mcpServers": {
    "gcp-cost-optimization": {
      "command": "uvx",
      "args": [
        "rohithay.dataops-mcp-server@latest",
        "--allow-write",
        "--allow-sensitive-data-access"
      ],
      "env": {
        "GCP_PROJECT_ID": "your-gcp-project",
        "GCP_REGION": "us-central1",
        "ENABLE_MULTI_AGENT": "true"
      },
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

**Using service account credentials**
```json
{
  "mcpServers": {
    "gcp-cost-optimization": {
      "command": "uvx",
      "args": ["rohithay.dataops-mcp-server@latest"],
      "env": {
        "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/service-account.json",
        "GCP_PROJECT_ID": "your-gcp-project",
        "GCP_REGION": "us-central1"
      },
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

### 🧪 Testing the configuration
Every time you start Claude Desktop, it will attempt to load any configured MCP Servers. You should see output indicating that the MCP Server has been discovered and initialized.

If you're running into issues, check out the [troubleshooting guide](./troubleshooting.md) or open a GitHub Issue. 


## 🔋 AI Assistant Capabilities

With the enhanced tool support, AI assistants can now:


## 💬 AI Prompt Templates

The server provides specialized prompts that AI assistants can use:
