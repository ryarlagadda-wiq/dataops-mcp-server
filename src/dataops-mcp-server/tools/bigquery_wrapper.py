#!/usr/bin/env python3
"""
BigQuery Tools Wrapper - Provides direct access to FastMCP functions
"""

import os
import json
from datetime import datetime, timedelta
from google.cloud import bigquery

def setup_client(project_id: str):
    """Setup BigQuery client with project ID"""
    os.environ['GOOGLE_CLOUD_PROJECT'] = project_id
    return bigquery.Client(project=project_id)

def get_daily_costs_direct(project_id: str, days: int = 7) -> str:
    """
    Get daily BigQuery costs for the specified number of days.
    """
    if not 1 <= days <= 30:
        return json.dumps({"error": "Days must be between 1 and 30"})
    
    try:
        bq_client = setup_client(project_id)
        
        query = f"""
        SELECT 
            DATE(creation_time) as date,
            COUNT(*) as query_count,
            SUM(total_bytes_processed) / POW(10, 12) * 6.25 as cost_usd,
            ROUND(AVG(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)), 2) as avg_duration_ms
        FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            AND job_type = 'QUERY'
            AND state = 'DONE'
            AND error_result IS NULL
            AND total_bytes_processed IS NOT NULL
        GROUP BY DATE(creation_time)
        ORDER BY date DESC
        """
        
        results = list(bq_client.query(query))
        
        daily_costs = []
        total_cost = 0
        
        for row in results:
            cost_entry = {
                "date": row.date.isoformat(),
                "query_count": int(row.query_count),
                "cost_usd": float(row.cost_usd or 0),
                "avg_duration_ms": float(row.avg_duration_ms or 0)
            }
            daily_costs.append(cost_entry)
            total_cost += cost_entry["cost_usd"]
        
        return json.dumps({
            "success": True,
            "project_id": project_id,
            "period_days": days,
            "total_cost_usd": round(total_cost, 2),
            "daily_costs": daily_costs
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e),
            "suggestion": "Check project permissions and ensure BigQuery has been used recently"
        })

def health_check_direct(project_id: str) -> str:
    """
    Check if the BigQuery cost analyzer can access the project successfully.
    """
    try:
        bq_client = setup_client(project_id)
        
        # Test basic BigQuery access
        test_query = "SELECT 1 as test"
        job_config = bigquery.QueryJobConfig(dry_run=True)
        bq_client.query(test_query, job_config=job_config)
        
        # Test INFORMATION_SCHEMA access
        schema_query = f"""
        SELECT COUNT(*) as job_count
        FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(dry_run=True)
        bq_client.query(schema_query, job_config=job_config)
        
        return json.dumps({
            "success": True,
            "status": "healthy",
            "project_id": project_id,
            "message": "BigQuery access confirmed",
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "status": "unhealthy",
            "error": str(e),
            "suggestions": [
                "Check GOOGLE_APPLICATION_CREDENTIALS environment variable",
                "Verify BigQuery API is enabled",
                "Ensure proper IAM permissions (bigquery.jobs.listAll)"
            ]
        })

def get_cost_summary_direct(project_id: str, days: int = 7) -> str:
    """
    Get a comprehensive cost summary for BigQuery usage.
    """
    if not 1 <= days <= 30:
        return json.dumps({"error": "Days must be between 1 and 30"})
    
    try:
        bq_client = setup_client(project_id)
        
        # Get basic stats
        summary_query = f"""
        SELECT 
            COUNT(*) as total_queries,
            SUM(total_bytes_processed) / POW(10, 12) * 6.25 as total_cost_usd,
            AVG(total_bytes_processed) / POW(10, 12) * 6.25 as avg_cost_per_query,
            MAX(total_bytes_processed) / POW(10, 12) * 6.25 as max_query_cost,
            COUNT(DISTINCT user_email) as unique_users,
            COUNT(DISTINCT DATE(creation_time)) as active_days
        FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            AND job_type = 'QUERY'
            AND state = 'DONE'
            AND error_result IS NULL
            AND total_bytes_processed IS NOT NULL
        """
        
        result = list(bq_client.query(summary_query))[0]
        
        total_cost = float(result.total_cost_usd or 0)
        avg_cost = float(result.avg_cost_per_query or 0)
        max_cost = float(result.max_query_cost or 0)
        
        # Generate insights
        insights = []
        
        if avg_cost > 1.0:
            insights.append(f"High average query cost: ${avg_cost:.2f} - consider query optimization")
        
        if max_cost > 10.0:
            insights.append(f"Very expensive single query detected: ${max_cost:.2f}")
        
        if result.active_days < days:
            insights.append(f"BigQuery not used every day ({result.active_days}/{days} active days)")
        
        # Calculate projections
        daily_avg = total_cost / max(result.active_days, 1)
        weekly_projection = daily_avg * 7
        monthly_projection = daily_avg * 30
        
        return json.dumps({
            "success": True,
            "project_id": project_id,
            "analysis_period": {
                "days": days,
                "start_date": (datetime.now() - timedelta(days=days)).date().isoformat(),
                "end_date": datetime.now().date().isoformat()
            },
            "summary": {
                "total_cost_usd": round(total_cost, 2),
                "total_queries": int(result.total_queries),
                "avg_cost_per_query": round(avg_cost, 4),
                "max_query_cost": round(max_cost, 2),
                "unique_users": int(result.unique_users or 0),
                "active_days": int(result.active_days)
            },
            "projections": {
                "daily_average": round(daily_avg, 2),
                "weekly_projection": round(weekly_projection, 2),
                "monthly_projection": round(monthly_projection, 2)
            },
            "insights": insights,
            "generated_at": datetime.now().isoformat()
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e)
        })
