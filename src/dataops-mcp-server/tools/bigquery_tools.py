#!/usr/bin/env python3

"""
Simple BigQuery Cost Analysis MCP Server
Provides basic BigQuery cost analysis tools for AI assistants.
"""

import os
import json
from datetime import datetime, timedelta
from typing import Optional

from fastmcp import FastMCP
from google.cloud import bigquery

# Initialize MCP server
mcp = FastMCP("BigQuery Cost Analyzer")

# Initialize BigQuery client
project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
if not project_id:
    raise ValueError("GOOGLE_CLOUD_PROJECT environment variable is required")

bq_client = bigquery.Client(project=project_id)

@mcp.tool()
def get_daily_costs(days: int = 7) -> str:
    """
    Get daily BigQuery costs for the specified number of days.
    
    Args:
        days: Number of days to analyze (1-30, default: 7)
    
    Returns:
        JSON string with daily cost breakdown
    """
    if not 1 <= days <= 30:
        return json.dumps({"error": "Days must be between 1 and 30"})
    
    try:
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

@mcp.tool()
def get_top_users(days: int = 7, limit: int = 10) -> str:
    """
    Get top BigQuery users by cost over the specified period.
    
    Args:
        days: Number of days to analyze (1-30, default: 7)
        limit: Maximum number of users to return (1-50, default: 10)
    
    Returns:
        JSON string with top users by cost
    """
    if not 1 <= days <= 30:
        return json.dumps({"error": "Days must be between 1 and 30"})
    
    if not 1 <= limit <= 50:
        return json.dumps({"error": "Limit must be between 1 and 50"})
    
    try:
        query = f"""
        SELECT 
            user_email,
            COUNT(*) as query_count,
            SUM(total_bytes_processed) / POW(10, 12) * 6.25 as cost_usd,
            ROUND(AVG(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)), 2) as avg_duration_ms
        FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            AND job_type = 'QUERY'
            AND state = 'DONE'
            AND error_result IS NULL
            AND total_bytes_processed IS NOT NULL
            AND user_email IS NOT NULL
        GROUP BY user_email
        ORDER BY cost_usd DESC
        LIMIT {limit}
        """
        
        results = list(bq_client.query(query))
        
        top_users = []
        total_cost = 0
        
        for row in results:
            user_entry = {
                "user_email": row.user_email,
                "query_count": int(row.query_count),
                "cost_usd": round(float(row.cost_usd or 0), 2),
                "avg_duration_ms": float(row.avg_duration_ms or 0)
            }
            top_users.append(user_entry)
            total_cost += user_entry["cost_usd"]
        
        return json.dumps({
            "success": True,
            "project_id": project_id,
            "period_days": days,
            "total_analyzed_cost": round(total_cost, 2),
            "top_users": top_users
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e)
        })

@mcp.tool()
def get_cost_summary(days: int = 7) -> str:
    """
    Get a comprehensive cost summary for BigQuery usage.
    
    Args:
        days: Number of days to analyze (1-30, default: 7)
    
    Returns:
        JSON string with cost summary and insights
    """
    if not 1 <= days <= 30:
        return json.dumps({"error": "Days must be between 1 and 30"})
    
    try:
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

@mcp.tool()
def get_service_account_analysis(
    days: int = 7, 
    service_account_filter: str = "", 
    include_query_text: bool = False,
    min_cost_threshold: float = 0.0
) -> str:
    """
    Get detailed cost analysis filtered by service account with comprehensive debugging info.
    
    Args:
        days: Number of days to analyze (1-30, default: 7)
        service_account_filter: Filter for service account email (empty for all, or partial match)
        include_query_text: Include actual SQL query text in results
        min_cost_threshold: Only include queries above this cost threshold
    
    Returns:
        JSON string with detailed service account cost analysis
    """
    if not 1 <= days <= 30:
        return json.dumps({"error": "Days must be between 1 and 30"})
    
    try:
        # Build the main analysis query
        query_text_field = "LEFT(query, 500) as query_preview," if include_query_text else ""
        service_account_condition = ""
        
        if service_account_filter:
            service_account_condition = f"AND user_email LIKE '%{service_account_filter}%'"
        
        query = f"""
        WITH query_analysis AS (
            SELECT 
                job_id,
                user_email,
                project_id as job_project,
                creation_time,
                start_time,
                end_time,
                {query_text_field}
                job_type,
                statement_type,
                priority,
                total_bytes_processed,
                total_bytes_billed,
                total_slot_ms,
                total_bytes_processed / POW(10, 12) * 6.25 as cost_usd,
                TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) as duration_ms,
                COALESCE(total_slot_ms / TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 0) as avg_slots,
                destination_table.dataset_id as target_dataset,
                destination_table.table_id as target_table,
                labels,
                reservation_id,
                parent_job_id,
                cache_hit,
                CASE 
                    WHEN user_email LIKE '%gserviceaccount.com' THEN 'Service Account'
                    WHEN user_email LIKE '%@gmail.com' OR user_email LIKE '%@company.com' THEN 'User Account'
                    ELSE 'Unknown'
                END as account_type,
                CASE
                    WHEN total_bytes_processed / POW(10, 12) * 6.25 > 50 THEN 'CRITICAL'
                    WHEN total_bytes_processed / POW(10, 12) * 6.25 > 10 THEN 'HIGH'
                    WHEN total_bytes_processed / POW(10, 12) * 6.25 > 1 THEN 'MEDIUM'
                    ELSE 'LOW'
                END as cost_tier
            FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
                AND job_type = 'QUERY'
                AND state = 'DONE'
                AND error_result IS NULL
                AND total_bytes_processed IS NOT NULL
                {service_account_condition}
                AND total_bytes_processed / POW(10, 12) * 6.25 >= {min_cost_threshold}
        ),
        user_summary AS (
            SELECT 
                user_email,
                account_type,
                COUNT(*) as query_count,
                SUM(cost_usd) as total_cost_usd,
                AVG(cost_usd) as avg_cost_per_query,
                MAX(cost_usd) as max_query_cost,
                SUM(total_bytes_processed) as total_bytes_processed,
                AVG(avg_slots) as avg_slots_used,
                SUM(total_slot_ms) as total_slot_ms,
                AVG(duration_ms) as avg_duration_ms,
                COUNT(CASE WHEN cache_hit THEN 1 END) as cache_hits,
                COUNT(DISTINCT target_dataset) as datasets_accessed,
                STRING_AGG(DISTINCT cost_tier) as cost_tiers_used
            FROM query_analysis
            GROUP BY user_email, account_type
        ),
        daily_breakdown AS (
            SELECT 
                DATE(creation_time) as date,
                user_email,
                account_type,
                COUNT(*) as daily_queries,
                SUM(cost_usd) as daily_cost,
                AVG(avg_slots) as daily_avg_slots
            FROM query_analysis
            GROUP BY DATE(creation_time), user_email, account_type
        )
        SELECT 
            'user_summary' as result_type,
            TO_JSON_STRING(user_summary) as data
        FROM user_summary
        WHERE total_cost_usd > 0
        ORDER BY total_cost_usd DESC
        """
        
        results = list(bq_client.query(query))
        
        # Parse user summaries
        user_summaries = []
        for row in results:
            if row.result_type == 'user_summary':
                user_data = json.loads(row.data)
                user_summaries.append(user_data)
        
        # Get daily breakdown
        daily_query = f"""
        SELECT 
            DATE(creation_time) as date,
            user_email,
            CASE 
                WHEN user_email LIKE '%gserviceaccount.com' THEN 'Service Account'
                WHEN user_email LIKE '%@gmail.com' OR user_email LIKE '%@company.com' THEN 'User Account'
                ELSE 'Unknown'
            END as account_type,
            COUNT(*) as daily_queries,
            SUM(total_bytes_processed / POW(10, 12) * 6.25) as daily_cost,
            AVG(COALESCE(total_slot_ms / TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 0)) as daily_avg_slots
        FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            AND job_type = 'QUERY'
            AND state = 'DONE'
            AND error_result IS NULL
            AND total_bytes_processed IS NOT NULL
            {service_account_condition}
            AND total_bytes_processed / POW(10, 12) * 6.25 >= {min_cost_threshold}
        GROUP BY DATE(creation_time), user_email, account_type
        ORDER BY date DESC, daily_cost DESC
        """
        
        daily_results = list(bq_client.query(daily_query))
        daily_breakdown = []
        
        for row in daily_results:
            daily_entry = {
                "date": row.date.isoformat(),
                "user_email": row.user_email,
                "account_type": row.account_type,
                "daily_queries": int(row.daily_queries),
                "daily_cost_usd": round(float(row.daily_cost), 4),
                "daily_avg_slots": round(float(row.daily_avg_slots or 0), 2)
            }
            daily_breakdown.append(daily_entry)
        
        # Get expensive queries details if requested
        expensive_queries = []
        if include_query_text or min_cost_threshold > 0:
            expensive_query = f"""
            SELECT 
                job_id,
                user_email,
                creation_time,
                total_bytes_processed / POW(10, 12) * 6.25 as cost_usd,
                TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) as duration_ms,
                COALESCE(total_slot_ms / TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 0) as avg_slots,
                destination_table.dataset_id as target_dataset,
                destination_table.table_id as target_table,
                cache_hit,
                LEFT(query, 1000) as query_preview
            FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
                AND job_type = 'QUERY'
                AND state = 'DONE'
                AND error_result IS NULL
                AND total_bytes_processed IS NOT NULL
                {service_account_condition}
                AND total_bytes_processed / POW(10, 12) * 6.25 >= GREATEST({min_cost_threshold}, 5.0)
            ORDER BY cost_usd DESC
            LIMIT 20
            """
            
            expensive_results = list(bq_client.query(expensive_query))
            
            for row in expensive_results:
                expensive_entry = {
                    "job_id": row.job_id,
                    "user_email": row.user_email,
                    "creation_time": row.creation_time.isoformat(),
                    "cost_usd": round(float(row.cost_usd), 4),
                    "duration_ms": int(row.duration_ms or 0),
                    "avg_slots": round(float(row.avg_slots or 0), 2),
                    "target_dataset": row.target_dataset,
                    "target_table": row.target_table,
                    "cache_hit": bool(row.cache_hit),
                }
                
                if include_query_text:
                    expensive_entry["query_preview"] = row.query_preview
                
                expensive_queries.append(expensive_entry)
        
        # Calculate overall statistics
        total_cost = sum(float(user.get("total_cost_usd", 0)) for user in user_summaries)
        total_queries = sum(int(user.get("query_count", 0)) for user in user_summaries)
        service_account_users = [u for u in user_summaries if u.get("account_type") == "Service Account"]
        user_account_users = [u for u in user_summaries if u.get("account_type") == "User Account"]
        
        # Generate insights
        insights = []
        
        if service_account_users:
            sa_cost = sum(float(u.get("total_cost_usd", 0)) for u in service_account_users)
            sa_percentage = (sa_cost / total_cost * 100) if total_cost > 0 else 0
            insights.append(f"Service accounts contribute {sa_percentage:.1f}% of total cost (${sa_cost:.2f})")
        
        if expensive_queries:
            insights.append(f"Found {len(expensive_queries)} expensive queries (>${min_cost_threshold if min_cost_threshold > 0 else 5.0})")
        
        # Top cost drivers
        if user_summaries:
            top_user = max(user_summaries, key=lambda x: float(x.get("total_cost_usd", 0)))
            insights.append(f"Top cost driver: {top_user.get('user_email')} (${top_user.get('total_cost_usd', 0):.2f})")
        
        return json.dumps({
            "success": True,
            "project_id": project_id,
            "analysis_period": {
                "days": days,
                "start_date": (datetime.now() - timedelta(days=days)).date().isoformat(),
                "end_date": datetime.now().date().isoformat(),
                "service_account_filter": service_account_filter or "all",
                "min_cost_threshold": min_cost_threshold
            },
            "overall_statistics": {
                "total_cost_usd": round(total_cost, 2),
                "total_queries": total_queries,
                "unique_users": len(user_summaries),
                "service_account_users": len(service_account_users),
                "user_account_users": len(user_account_users),
                "avg_cost_per_query": round(total_cost / max(total_queries, 1), 4)
            },
            "user_analysis": user_summaries,
            "daily_breakdown": daily_breakdown[:50],  # Limit to 50 entries
            "expensive_queries": expensive_queries,
            "insights": insights,
            "debugging_info": {
                "query_included_text": include_query_text,
                "cost_threshold_applied": min_cost_threshold,
                "service_account_filter_applied": bool(service_account_filter),
                "expensive_queries_count": len(expensive_queries),
                "daily_breakdown_entries": len(daily_breakdown)
            },
            "generated_at": datetime.now().isoformat()
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "suggestion": "Check project permissions and service account filter syntax"
        })

@mcp.tool()
def health_check() -> str:
    """
    Check if the BigQuery cost analyzer can access the project successfully.
    
    Returns:
        JSON string with health status
    """
    try:
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

if __name__ == "__main__":
    mcp.run()