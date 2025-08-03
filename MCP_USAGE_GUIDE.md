# DataOps MCP Server Usage Guide

## 🎯 What is this MCP Server?

This **Model Context Protocol (MCP) Server** provides AI assistants like Claude Desktop with BigQuery cost analysis tools. It allows you to:

- **Analyze BigQuery costs** and usage patterns
- **Optimize expensive queries** before running them
- **Forecast future costs** based on historical data
- **Identify cost drivers** and optimization opportunities

## 🚀 How to Use the MCP Server

### **Method 1: Command Line Interface (CLI)**

The easiest way to use the tools directly:

#### **1. Health Check**
```bash
uv run python src/client.py health
```
**What it does:** Verifies BigQuery access and permissions

#### **2. Cost Analysis**
```bash
# Basic cost analysis (last 7 days)
uv run python src/client.py costs

# Extended analysis (30 days with predictions)
uv run python src/client.py costs --days 30 --predictions

# Detailed analysis with grouping
uv run python src/client.py costs --days 14 --group-by date,user --details
```
**What it does:** Analyzes historical BigQuery costs, shows trends, and identifies optimization opportunities

#### **3. Query Cost Analysis**
```bash
# Analyze a simple query
uv run python src/client.py query "SELECT COUNT(*) FROM dataset.table" --optimize

# Analyze a complex query with optimization suggestions
uv run python src/client.py query "$(cat your_query.sql)" --optimize
```
**What it does:** Estimates query cost before execution and provides optimization suggestions

### **Method 2: Claude Desktop Integration**

To use with Claude Desktop AI assistant:

#### **1. Add to Claude Desktop Config**
Edit your `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "dataops-cost-analyzer": {
      "command": "uv",
      "args": ["run", "python", "src/main.py"],
      "cwd": "/Users/ryarlagadda/repos/dataops-mcp-server",
      "env": {
        "GCP_PROJECT_ID": "gcp-wow-wiq-tsr-dev"
      }
    }
  }
}
```

#### **2. Natural Language Commands in Claude**
Once configured, you can ask Claude:
- *"Analyze my BigQuery costs for the last 30 days"*
- *"Find queries that cost more than $50"*
- *"Optimize this SQL query for cost efficiency"*
- *"What are my biggest cost drivers in BigQuery?"*
- *"Forecast my BigQuery costs for next week"*

### **Method 3: Python Integration**

Use directly in your Python scripts:

```python
import asyncio
import sys
import os

# Add to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src', 'dataops-mcp-server'))

from tools.bigquery_tools import GetBigQueryCostsTool
from tools.cost_analysis_tools import AnalyzeQueryCostTool

async def main():
    # Cost analysis
    cost_tool = GetBigQueryCostsTool(project_id="gcp-wow-wiq-tsr-dev")
    cost_result = await cost_tool.execute(days=7, include_predictions=True)
    
    # Query analysis  
    query_tool = AnalyzeQueryCostTool(project_id="gcp-wow-wiq-tsr-dev")
    query_result = await query_tool.execute(
        sql="SELECT * FROM big_table WHERE date > '2024-01-01'",
        include_optimization=True
    )
    
    print("Cost Analysis:", cost_result)
    print("Query Analysis:", query_result)

asyncio.run(main())
```

## 📊 Available Tools

### **1. GetBigQueryCostsTool**
- **Purpose:** Historical cost analysis and forecasting
- **Parameters:**
  - `days` (1-90): Analysis period
  - `include_predictions`: Add cost forecasting
  - `group_by`: Group by date, user, dataset, query_type
  - `include_query_details`: Individual query breakdowns

### **2. AnalyzeQueryCostTool** 
- **Purpose:** Pre-execution query cost estimation
- **Parameters:**
  - `sql`: SQL query to analyze
  - `include_optimization`: Get optimization suggestions
  - `optimization_model`: Analysis method

### **3. Health Check**
- **Purpose:** Verify BigQuery connectivity and permissions
- **Returns:** Boolean health status

## 🔧 Setup Requirements

### **1. Google Cloud Authentication**
```bash
# Option A: Service account key
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# Option B: User authentication
gcloud auth application-default login
gcloud config set project gcp-wow-wiq-tsr-dev
```

### **2. Required Permissions**
Your account/service account needs:
- `BigQuery Data Viewer`
- `BigQuery Job User` 
- `BigQuery Resource Viewer`

### **3. Python Dependencies**
```bash
uv add google-cloud-bigquery
```

## 📈 Example Output

### **Cost Analysis Result:**
```json
{
  "success": true,
  "project_id": "gcp-wow-wiq-tsr-dev",
  "cost_analysis": {
    "cost_summary": {
      "total_cost_usd": 2821.21,
      "total_queries": 4351,
      "average_cost_per_query": 0.6484
    },
    "trends": {
      "trend_direction": "increasing",
      "monthly_projection": 30000.50
    },
    "optimization_opportunities": [
      {
        "type": "high_average_query_cost",
        "potential_monthly_savings": 9000.15,
        "priority": "high"
      }
    ]
  }
}
```

### **Query Analysis Result:**
```json
{
  "success": true,
  "analysis": {
    "estimated_cost_usd": 8.56,
    "bytes_to_process": 1402200000000,
    "risk_assessment": {
      "risk_level": "CRITICAL"
    },
    "optimization_suggestions": [
      "Implement incremental processing",
      "Add partition filters",
      "Consider materialized views"
    ]
  }
}
```

## 🎯 Common Use Cases

### **1. Daily Cost Monitoring**
```bash
# Check yesterday's costs
uv run python src/client.py costs --days 1

# Weekly cost review
uv run python src/client.py costs --days 7 --predictions
```

### **2. Pre-deployment Query Validation**
```bash
# Before running expensive dbt models
uv run python src/client.py query "$(cat models/expensive_model.sql)" --optimize
```

### **3. Cost Investigation**
```bash
# Find cost drivers by user
uv run python src/client.py costs --days 30 --group-by user --details

# Find expensive datasets
uv run python src/client.py costs --days 14 --group-by dataset
```

### **4. Automated Monitoring**
```bash
#!/bin/bash
# Daily cost check script
COST=$(uv run python src/client.py costs --days 1 | jq -r '.cost_analysis.cost_summary.total_cost_usd')
if (( $(echo "$COST > 1000" | bc -l) )); then
    echo "⚠️  High daily cost: \$$COST"
    # Send alert
fi
```

## 🔍 Troubleshooting

### **Common Issues:**

1. **"No module named 'google'"**
   ```bash
   uv add google-cloud-bigquery
   ```

2. **"Health check failed"**
   ```bash
   gcloud auth application-default login
   gcloud config set project gcp-wow-wiq-tsr-dev
   ```

3. **"Permission denied"**
   - Check IAM roles in Google Cloud Console
   - Ensure BigQuery API is enabled

4. **"No data found"**
   - Project needs BigQuery usage history
   - Check if queries are in US region

### **Debug Commands:**
```bash
# Test authentication
gcloud auth list

# Test BigQuery access
gcloud bq ls

# Check project
gcloud config get-value project
```

## 🎉 Next Steps

1. **Start with health check:** `uv run python src/client.py health`
2. **Run cost analysis:** `uv run python src/client.py costs --days 7`
3. **Test query analysis:** `uv run python src/client.py query "SELECT 1" --optimize`
4. **Configure Claude Desktop** for natural language queries
5. **Integrate into your dbt workflows** for pre-deployment cost checks

The MCP server is now ready to help you optimize your BigQuery costs! 🚀
