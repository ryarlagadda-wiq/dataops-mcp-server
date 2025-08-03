#!/usr/bin/env python3
"""
BigQuery Tools Wrapper - Provides direct access to FastMCP functions
"""

import os
import json
import hashlib
from datetime import datetime, timedelta
from google.cloud import bigquery

def setup_client(project_id: str):
    """Setup BigQuery client with project ID"""
    os.environ['GOOGLE_CLOUD_PROJECT'] = project_id
    return bigquery.Client(project=project_id)

def bigquery_cost_analyzer(
    project_id: str,
    days: int = 7, 
    service_account_filter: str = "", 
    include_query_text: bool = False,
    min_cost_threshold: float = 0.0
) -> str:
    """
    Get detailed cost analysis with comprehensive debugging info.
    
    Args:
        project_id: GCP project ID
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
        bq_client = setup_client(project_id)
        
        # Build the main analysis query
        query_text_field = "LEFT(query, 500) as query_preview," if include_query_text else ""
        service_account_condition = ""
        
        if service_account_filter:
            service_account_condition = f"AND user_email LIKE '%{service_account_filter}%'"
        
        # User summary query
        user_summary_query = f"""
        SELECT 
            user_email,
            CASE 
                WHEN user_email LIKE '%gserviceaccount.com' THEN 'Service Account'
                WHEN user_email LIKE '%@woolworths.com.au' THEN 'User Account'
                ELSE 'Unknown'
            END as account_type,
            COUNT(*) as query_count,
            SUM(total_bytes_processed / POW(10, 12) * 6.25) as total_cost_usd,
            AVG(total_bytes_processed / POW(10, 12) * 6.25) as avg_cost_per_query,
            MAX(total_bytes_processed / POW(10, 12) * 6.25) as max_query_cost,
            SUM(total_bytes_processed) as total_bytes_processed,
            AVG(COALESCE(total_slot_ms / TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 0)) as avg_slots_used,
            SUM(total_slot_ms) as total_slot_ms,
            AVG(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)) as avg_duration_ms,
            COUNT(CASE WHEN cache_hit THEN 1 END) as cache_hits,
            COUNT(DISTINCT destination_table.dataset_id) as datasets_accessed
        FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            AND job_type = 'QUERY'
            AND state = 'DONE'
            AND error_result IS NULL
            AND total_bytes_processed IS NOT NULL
            {service_account_condition}
            AND total_bytes_processed / POW(10, 12) * 6.25 >= {min_cost_threshold}
        GROUP BY user_email, account_type
        HAVING total_cost_usd > 0
        ORDER BY total_cost_usd DESC
        """
        
        user_results = list(bq_client.query(user_summary_query))
        user_summaries = []
        
        for row in user_results:
            user_entry = {
                "user_email": row.user_email,
                "account_type": row.account_type,
                "query_count": int(row.query_count),
                "total_cost_usd": round(float(row.total_cost_usd), 4),
                "avg_cost_per_query": round(float(row.avg_cost_per_query), 4),
                "max_query_cost": round(float(row.max_query_cost), 4),
                "total_bytes_processed": int(row.total_bytes_processed),
                "avg_slots_used": round(float(row.avg_slots_used or 0), 2),
                "total_slot_ms": int(row.total_slot_ms or 0),
                "avg_duration_ms": round(float(row.avg_duration_ms or 0), 2),
                "cache_hits": int(row.cache_hits or 0),
                "datasets_accessed": int(row.datasets_accessed or 0)
            }
            user_summaries.append(user_entry)
        
        # Get expensive queries details
        expensive_queries = []
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
            statement_type,
            reservation_id,
            {query_text_field if include_query_text else "'Query text not included' as query_preview,"}
            parent_job_id
        FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            AND job_type = 'QUERY'
            AND state = 'DONE'
            AND error_result IS NULL
            AND total_bytes_processed IS NOT NULL
            {service_account_condition}
            AND total_bytes_processed / POW(10, 12) * 6.25 >= GREATEST({min_cost_threshold}, 1.0)
        ORDER BY cost_usd DESC
        LIMIT 50
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
                "statement_type": row.statement_type,
                "reservation_id": row.reservation_id,
                "parent_job_id": row.parent_job_id
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
            
            # Top service account
            if service_account_users:
                top_sa = max(service_account_users, key=lambda x: float(x.get("total_cost_usd", 0)))
                insights.append(f"Top service account: {top_sa.get('user_email')} (${top_sa.get('total_cost_usd', 0):.2f})")
        
        if expensive_queries:
            insights.append(f"Found {len(expensive_queries)} expensive queries (>${min_cost_threshold if min_cost_threshold > 0 else 1.0})")
            
            # Cache hit analysis
            cache_hits = sum(1 for q in expensive_queries if q.get('cache_hit'))
            cache_rate = (cache_hits / len(expensive_queries) * 100) if expensive_queries else 0
            insights.append(f"Cache hit rate for expensive queries: {cache_rate:.1f}%")
        
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
                "avg_cost_per_query": round(total_cost / max(total_queries, 1), 4),
                "total_slot_ms": sum(int(u.get("total_slot_ms", 0)) for u in user_summaries)
            },
            "usage_analysis": user_summaries,
            "expensive_queries": expensive_queries,
            "insights": insights,
            "debugging_info": {
                "query_included_text": include_query_text,
                "cost_threshold_applied": min_cost_threshold,
                "service_account_filter_applied": bool(service_account_filter),
                "expensive_queries_count": len(expensive_queries),
                "user_summaries_count": len(user_summaries)
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

def get_daily_costs(project_id: str, days: int = 7) -> str:
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

def health_check(project_id: str) -> str:
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

def get_cost_summary(project_id: str, days: int = 7) -> str:
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


# Cost Optimizer Tool Wrappers
def analyze_expensive_queries_direct(
    project_id: str,
    days: int = 7,
    min_cost_threshold: float = 10.0,
    categorize_by: str = "cost_driver"
) -> str:
    """
    Wrapper for analyze_expensive_queries to work with CLI.
    
    Args:
        project_id: GCP project ID
        days: Number of days to analyze (1-30, default: 7)
        min_cost_threshold: Minimum cost to be considered expensive (default: 10.0)
        categorize_by: Categorization method (cost_driver, usage_pattern, optimization_opportunity)
    
    Returns:
        JSON string with categorized expensive queries and optimization suggestions
    """
    if not 1 <= days <= 30:
        return json.dumps({"error": "Days must be between 1 and 30"})
    
    try:
        bq_client = setup_client(project_id)
        
        # Get expensive queries with detailed metadata
        query = f"""
        WITH expensive_queries AS (
            SELECT 
                job_id,
                user_email,
                creation_time,
                LEFT(query, 2000) as query_text,
                statement_type,
                total_bytes_processed,
                total_bytes_processed / POW(10, 12) * 6.25 as cost_usd,
                TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) as duration_ms,
                COALESCE(total_slot_ms / TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 0) as avg_slots,
                destination_table.dataset_id as target_dataset,
                destination_table.table_id as target_table,
                cache_hit,
                reservation_id,
                
                -- Cost driver classification
                CASE 
                    WHEN total_bytes_processed / POW(10, 12) > 10 THEN 'DATA_VOLUME_HEAVY'
                    WHEN COALESCE(total_slot_ms / TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 0) > 2000 THEN 'COMPUTE_INTENSIVE'
                    WHEN total_bytes_processed / POW(10, 12) > 1 AND COALESCE(total_slot_ms / TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 0) > 500 THEN 'MIXED_HEAVY'
                    ELSE 'MODERATE_USAGE'
                END as cost_driver_type,
                
                -- Usage pattern classification  
                CASE 
                    WHEN job_id LIKE '%airflow%' OR job_id LIKE '%scheduled%' THEN 'ETL_SCHEDULED'
                    WHEN user_email LIKE '%gserviceaccount.com' THEN 'SERVICE_ACCOUNT'
                    WHEN query LIKE '%LIMIT%' AND query LIKE '%ORDER BY%' THEN 'EXPLORATORY'
                    WHEN statement_type IN ('CREATE_TABLE_AS_SELECT', 'INSERT') THEN 'DATA_PIPELINE'
                    ELSE 'AD_HOC_ANALYSIS'
                END as usage_pattern,
                
                -- Optimization opportunity classification
                CASE 
                    WHEN query LIKE '%SELECT *%' AND total_bytes_processed / POW(10, 12) > 1 THEN 'COLUMN_PRUNING'
                    WHEN query NOT LIKE '%WHERE%' AND total_bytes_processed / POW(10, 12) > 5 THEN 'MISSING_FILTERS'
                    WHEN query LIKE '%JOIN%' AND (query LIKE '%SELECT *%' OR NOT query LIKE '%WHERE%') THEN 'JOIN_OPTIMIZATION'
                    WHEN NOT cache_hit AND query LIKE '%GROUP BY%' THEN 'CACHING_OPPORTUNITY'
                    WHEN query LIKE '%ORDER BY%' AND NOT query LIKE '%LIMIT%' THEN 'RESULT_LIMITING'
                    ELSE 'GENERAL_OPTIMIZATION'
                END as optimization_category
                
            FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
                AND job_type = 'QUERY'
                AND state = 'DONE'
                AND error_result IS NULL
                AND total_bytes_processed IS NOT NULL
                AND total_bytes_processed / POW(10, 12) * 6.25 >= {min_cost_threshold}
                AND query IS NOT NULL
        )
        SELECT * FROM expensive_queries
        ORDER BY cost_usd DESC
        LIMIT 100
        """
        
        results = list(bq_client.query(query))
        
        # Process and categorize results
        categorized_queries = {}
        total_cost = 0
        
        for row in results:
            total_cost += row.cost_usd
            
            # Determine categorization key
            if categorize_by == "cost_driver":
                category_key = row.cost_driver_type
            elif categorize_by == "usage_pattern":
                category_key = row.usage_pattern
            else:
                category_key = row.optimization_category
            
            if category_key not in categorized_queries:
                categorized_queries[category_key] = {
                    "queries": [],
                    "total_cost": 0,
                    "query_count": 0
                }
            
            query_entry = {
                "job_id": row.job_id,
                "user_email": row.user_email,
                "creation_time": row.creation_time.isoformat(),
                "cost_usd": round(row.cost_usd, 2),
                "duration_ms": row.duration_ms,
                "avg_slots": round(row.avg_slots, 2),
                "cost_driver_type": row.cost_driver_type,
                "usage_pattern": row.usage_pattern,
                "optimization_category": row.optimization_category,
                "target_table": f"{row.target_dataset}.{row.target_table}" if row.target_dataset else None,
                "cache_hit": row.cache_hit,
                "query_preview": row.query_text[:500] + "..." if len(row.query_text) > 500 else row.query_text
            }
            
            categorized_queries[category_key]["queries"].append(query_entry)
            categorized_queries[category_key]["total_cost"] += row.cost_usd
            categorized_queries[category_key]["query_count"] += 1
        
        # Calculate averages and generate recommendations
        optimization_recommendations = []
        for category, data in categorized_queries.items():
            data["avg_cost"] = round(data["total_cost"] / data["query_count"], 2)
            data["total_cost"] = round(data["total_cost"], 2)
            
            # Generate category-specific recommendations
            recommendations = _generate_optimization_recommendations(category, data, categorize_by)
            optimization_recommendations.extend(recommendations)
        
        return json.dumps({
            "success": True,
            "analysis_period": {
                "days": days,
                "min_cost_threshold": min_cost_threshold,
                "categorize_by": categorize_by
            },
            "summary": {
                "total_expensive_queries": len(results),
                "total_cost_analyzed": round(total_cost, 2),
                "categories_found": len(categorized_queries),
                "avg_cost_per_query": round(total_cost / len(results), 2) if results else 0
            },
            "categorized_queries": categorized_queries,
            "optimization_recommendations": optimization_recommendations,
            "generated_at": datetime.now().isoformat()
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        })


def detect_optimization_patterns_direct(
    project_id: str,
    days: int = 7,
    min_cost_threshold: float = 5.0
) -> str:
    """
    Wrapper for detect_optimization_patterns to work with CLI.
    
    Args:
        project_id: GCP project ID
        days: Number of days to analyze (1-30, default: 7)
        min_cost_threshold: Minimum cost to analyze (default: 5.0)
    
    Returns:
        JSON string with detected patterns and specific optimization suggestions
    """
    if not 1 <= days <= 30:
        return json.dumps({"error": "Days must be between 1 and 30"})
    
    try:
        bq_client = setup_client(project_id)
        
        query = f"""
        SELECT 
            job_id,
            user_email,
            creation_time,
            query,
            total_bytes_processed / POW(10, 12) * 6.25 as cost_usd,
            total_bytes_processed / POW(10, 12) as tb_processed,
            TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) as duration_ms,
            COALESCE(total_slot_ms / TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 0) as avg_slots,
            cache_hit,
            statement_type,
            destination_table.dataset_id as target_dataset,
            destination_table.table_id as target_table
        FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            AND job_type = 'QUERY'
            AND state = 'DONE'
            AND error_result IS NULL
            AND total_bytes_processed IS NOT NULL
            AND total_bytes_processed / POW(10, 12) * 6.25 >= {min_cost_threshold}
            AND query IS NOT NULL
        ORDER BY cost_usd DESC
        LIMIT 200
        """
        
        results = list(bq_client.query(query))
        
        patterns_detected = {
            "select_star_large_scan": [],
            "missing_partition_filter": [],
            "unfiltered_aggregation": [],
            "order_without_limit": [],
            "complex_joins": [],
            "no_cache_utilization": []
        }
        
        total_potential_savings = 0
        
        for row in results:
            query_text = row.query.upper() if row.query else ""
            cost = row.cost_usd
            
            # Pattern 1: SELECT * with large data scan
            if "SELECT *" in query_text and row.tb_processed > 1:
                patterns_detected["select_star_large_scan"].append({
                    "job_id": row.job_id,
                    "user_email": row.user_email,
                    "cost_usd": cost,
                    "tb_processed": row.tb_processed,
                    "potential_savings_usd": cost * 0.4,
                    "recommendation": "Replace SELECT * with specific column names",
                    "implementation": "List only required columns to reduce data processing"
                })
                total_potential_savings += cost * 0.4
            
            # Pattern 2: Missing partition filters
            if row.tb_processed > 5 and not any(filter_word in query_text for filter_word in 
                ['_PARTITIONTIME', '_PARTITIONDATE', 'DATE(', 'PARTITION']):
                patterns_detected["missing_partition_filter"].append({
                    "job_id": row.job_id,
                    "user_email": row.user_email,
                    "cost_usd": cost,
                    "tb_processed": row.tb_processed,
                    "potential_savings_usd": cost * 0.6,
                    "recommendation": "Add partition filters to limit data scan",
                    "implementation": "Add WHERE _PARTITIONDATE >= 'YYYY-MM-DD' clause"
                })
                total_potential_savings += cost * 0.6
            
            # Pattern 3: Unfiltered aggregations
            if "GROUP BY" in query_text and "WHERE" not in query_text and row.tb_processed > 2:
                patterns_detected["unfiltered_aggregation"].append({
                    "job_id": row.job_id,
                    "user_email": row.user_email,
                    "cost_usd": cost,
                    "potential_savings_usd": cost * 0.5,
                    "recommendation": "Add WHERE clause to filter data before aggregation",
                    "implementation": "Filter data early to reduce aggregation workload"
                })
                total_potential_savings += cost * 0.5
            
            # Pattern 4: ORDER BY without LIMIT
            if "ORDER BY" in query_text and "LIMIT" not in query_text and row.tb_processed > 1:
                patterns_detected["order_without_limit"].append({
                    "job_id": row.job_id,
                    "user_email": row.user_email,
                    "cost_usd": cost,
                    "potential_savings_usd": cost * 0.25,
                    "recommendation": "Add LIMIT clause to ORDER BY queries",
                    "implementation": "Use LIMIT to avoid sorting entire result set"
                })
                total_potential_savings += cost * 0.25
            
            # Pattern 5: Complex JOINs
            join_count = query_text.count("JOIN")
            if join_count >= 3:
                patterns_detected["complex_joins"].append({
                    "job_id": row.job_id,
                    "user_email": row.user_email,
                    "cost_usd": cost,
                    "join_count": join_count,
                    "potential_savings_usd": cost * 0.3,
                    "recommendation": f"Optimize {join_count} JOINs - consider denormalization",
                    "implementation": "Review JOIN order, consider materialized views"
                })
                total_potential_savings += cost * 0.3
            
            # Pattern 6: No cache utilization
            if not row.cache_hit and cost > 10:
                patterns_detected["no_cache_utilization"].append({
                    "job_id": row.job_id,
                    "user_email": row.user_email,
                    "cost_usd": cost,
                    "potential_savings_usd": cost * 0.8,
                    "recommendation": "Enable query result caching",
                    "implementation": "Use deterministic queries that can be cached"
                })
                total_potential_savings += cost * 0.8
        
        # Calculate pattern summaries
        pattern_summary = {}
        for pattern_name, pattern_queries in patterns_detected.items():
            if pattern_queries:
                pattern_summary[pattern_name] = {
                    "occurrences": len(pattern_queries),
                    "total_cost": sum(q["cost_usd"] for q in pattern_queries),
                    "total_potential_savings": sum(q["potential_savings_usd"] for q in pattern_queries),
                    "avg_cost_per_occurrence": sum(q["cost_usd"] for q in pattern_queries) / len(pattern_queries)
                }
        
        return json.dumps({
            "success": True,
            "analysis_period": {
                "days": days,
                "min_cost_threshold": min_cost_threshold,
                "queries_analyzed": len(results)
            },
            "summary": {
                "patterns_detected": len([p for p in patterns_detected.values() if p]),
                "total_potential_savings_usd": round(total_potential_savings, 2),
                "top_opportunity": max(pattern_summary.items(), 
                                    key=lambda x: x[1]["total_potential_savings"])[0] if pattern_summary else None
            },
            "pattern_summary": pattern_summary,
            "detailed_patterns": patterns_detected,
            "generated_at": datetime.now().isoformat()
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        })


def _generate_optimization_recommendations(category: str, data: dict, categorize_by: str) -> list:
    """
    Generate optimization recommendations based on category analysis.
    
    Args:
        category: The category name (e.g., 'DATA_VOLUME_HEAVY', 'MISSING_FILTERS', etc.)
        data: Dictionary containing category data with keys: queries, total_cost, query_count, avg_cost
        categorize_by: The categorization method ('cost_driver', 'usage_pattern', 'optimization_opportunity')
    
    Returns:
        List of recommendation dictionaries with priority, description, implementation, and potential_savings
    """
    recommendations = []
    total_cost = data.get("total_cost", 0)
    query_count = data.get("query_count", 0)
    avg_cost = data.get("avg_cost", 0)
    
    if categorize_by == "cost_driver":
        if category == "DATA_VOLUME_HEAVY":
            recommendations.append({
                "priority": "HIGH",
                "category": category,
                "title": "Optimize Large Data Scans",
                "description": f"These {query_count} queries process massive amounts of data (avg ${avg_cost:.2f}/query)",
                "implementation": [
                    "Add partition filters to limit data scanning",
                    "Replace SELECT * with specific column names",
                    "Use LIMIT clauses for exploratory queries",
                    "Consider materialized views for repeated patterns"
                ],
                "potential_savings_usd": round(total_cost * 0.6, 2),
                "effort": "Medium",
                "timeline": "2-4 weeks"
            })
        
        elif category == "COMPUTE_INTENSIVE":
            recommendations.append({
                "priority": "MEDIUM",
                "category": category,
                "title": "Reduce Compute Complexity",
                "description": f"These {query_count} queries use excessive compute resources (high slot usage)",
                "implementation": [
                    "Optimize complex JOINs and subqueries",
                    "Use approximate aggregation functions where possible",
                    "Consider breaking complex queries into steps",
                    "Enable query result caching"
                ],
                "potential_savings_usd": round(total_cost * 0.3, 2),
                "effort": "High",
                "timeline": "4-6 weeks"
            })
        
        elif category == "MIXED_HEAVY":
            recommendations.append({
                "priority": "HIGH",
                "category": category,
                "title": "Comprehensive Query Optimization",
                "description": f"These {query_count} queries have both high data volume and compute usage",
                "implementation": [
                    "Apply both data volume and compute optimizations",
                    "Review query architecture and design",
                    "Consider denormalization strategies",
                    "Implement incremental processing patterns"
                ],
                "potential_savings_usd": round(total_cost * 0.5, 2),
                "effort": "High",
                "timeline": "6-8 weeks"
            })
    
    elif categorize_by == "usage_pattern":
        if category == "SERVICE_ACCOUNT":
            recommendations.append({
                "priority": "CRITICAL",
                "category": category,
                "title": "Optimize Automated Service Account Queries",
                "description": f"Service account queries cost ${total_cost:.2f} - these run repeatedly and compound costs",
                "implementation": [
                    "Review ETL job efficiency and scheduling",
                    "Implement incremental data processing",
                    "Add query result caching for repeated patterns",
                    "Optimize dbt models and transformations"
                ],
                "potential_savings_usd": round(total_cost * 0.7, 2),
                "effort": "Medium",
                "timeline": "2-3 weeks"
            })
        
        elif category == "ETL_SCHEDULED":
            recommendations.append({
                "priority": "HIGH",
                "category": category,
                "title": "Optimize Scheduled ETL Jobs",
                "description": f"Scheduled jobs consuming ${total_cost:.2f} with {query_count} executions",
                "implementation": [
                    "Optimize job scheduling to avoid peak hours",
                    "Implement incremental loads instead of full refreshes",
                    "Use clustering and partitioning effectively",
                    "Review job dependencies and parallelization"
                ],
                "potential_savings_usd": round(total_cost * 0.5, 2),
                "effort": "Medium",
                "timeline": "3-4 weeks"
            })
        
        elif category == "AD_HOC_ANALYSIS":
            recommendations.append({
                "priority": "MEDIUM",
                "category": category,
                "title": "Improve Ad-hoc Query Efficiency",
                "description": f"Ad-hoc analysis queries costing ${total_cost:.2f} - optimize for exploration",
                "implementation": [
                    "Create sample datasets for exploration",
                    "Provide query templates and best practices",
                    "Implement query cost warnings",
                    "Use BI tools with built-in optimizations"
                ],
                "potential_savings_usd": round(total_cost * 0.4, 2),
                "effort": "Low",
                "timeline": "1-2 weeks"
            })
    
    elif categorize_by == "optimization_opportunity":
        if category == "MISSING_FILTERS":
            recommendations.append({
                "priority": "CRITICAL",
                "category": category,
                "title": "Add Partition and Where Filters",
                "description": f"${total_cost:.2f} wasted on unfiltered data scans across {query_count} queries",
                "implementation": [
                    "Add WHERE clauses with partition columns (_PARTITIONDATE, _PARTITIONTIME)",
                    "Implement date range filters on all time-series queries",
                    "Use query validators to enforce filter requirements",
                    "Create filtered views for common access patterns"
                ],
                "potential_savings_usd": round(total_cost * 0.8, 2),
                "effort": "Low",
                "timeline": "1 week"
            })
        
        elif category == "COLUMN_PRUNING":
            recommendations.append({
                "priority": "HIGH",
                "category": category,
                "title": "Replace SELECT * with Specific Columns",
                "description": f"${total_cost:.2f} spent on unnecessary column processing in {query_count} queries",
                "implementation": [
                    "Identify required columns for each query",
                    "Replace SELECT * with explicit column lists",
                    "Create views with commonly used column sets",
                    "Use SELECT EXCEPT for near-complete column sets"
                ],
                "potential_savings_usd": round(total_cost * 0.6, 2),
                "effort": "Low",
                "timeline": "1-2 weeks"
            })
        
        elif category == "CACHING_OPPORTUNITY":
            recommendations.append({
                "priority": "MEDIUM",
                "category": category,
                "title": "Enable Query Result Caching",
                "description": f"${total_cost:.2f} could be saved with caching on {query_count} repeated queries",
                "implementation": [
                    "Enable query result caching in BigQuery settings",
                    "Make queries deterministic (remove NOW(), RAND(), etc.)",
                    "Use consistent table names and avoid dynamic SQL",
                    "Implement cache-friendly query patterns"
                ],
                "potential_savings_usd": round(total_cost * 0.9, 2),
                "effort": "Low",
                "timeline": "Few days"
            })
        
        elif category == "JOIN_OPTIMIZATION":
            recommendations.append({
                "priority": "MEDIUM",
                "category": category,
                "title": "Optimize Complex Joins",
                "description": f"${total_cost:.2f} on complex join operations in {query_count} queries",
                "implementation": [
                    "Review join order and use smaller tables first",
                    "Add filters before joins to reduce data volume",
                    "Consider denormalization for frequently joined tables",
                    "Use ARRAY and STRUCT types to reduce joins"
                ],
                "potential_savings_usd": round(total_cost * 0.4, 2),
                "effort": "Medium",
                "timeline": "2-3 weeks"
            })
        
        elif category == "RESULT_LIMITING":
            recommendations.append({
                "priority": "LOW",
                "category": category,
                "title": "Add LIMIT Clauses to Large Result Sets",
                "description": f"${total_cost:.2f} on queries returning large result sets without limits",
                "implementation": [
                    "Add LIMIT clauses to exploratory queries",
                    "Use QUALIFY for window function results",
                    "Implement pagination for large datasets",
                    "Use TABLESAMPLE for data exploration"
                ],
                "potential_savings_usd": round(total_cost * 0.3, 2),
                "effort": "Low",
                "timeline": "1 week"
            })
    
    # Add general recommendations if no specific category matched
    if not recommendations:
        recommendations.append({
            "priority": "MEDIUM",
            "category": category,
            "title": "General Query Optimization",
            "description": f"Review and optimize {query_count} queries costing ${total_cost:.2f}",
            "implementation": [
                "Analyze query execution plans",
                "Apply standard BigQuery optimization practices",
                "Monitor query performance over time",
                "Implement cost alerts and governance"
            ],
            "potential_savings_usd": round(total_cost * 0.2, 2),
            "effort": "Medium",
            "timeline": "2-4 weeks"
        })
    
    return recommendations


def create_cost_forecast_direct(
    days_historical: int = 30,
    days_forecast: int = 30,
    growth_assumptions: str = "current_trend"
) -> str:
    """
    Create cost forecast based on historical usage patterns.
    """
    try:
        # Use default project and client
        project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'gcp-wow-wiq-tsr-dev')
        bq_client = setup_client(project_id)
        
        # Get historical daily costs
        query = f"""
        WITH daily_costs AS (
            SELECT 
                DATE(creation_time) as date,
                SUM(total_bytes_processed / POW(10, 12) * 6.25) as daily_cost_usd,
                COUNT(*) as query_count,
                AVG(total_bytes_processed / POW(10, 12) * 6.25) as avg_cost_per_query
            FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_historical} DAY)
                AND job_type = 'QUERY'
                AND state = 'DONE'
                AND error_result IS NULL
                AND total_bytes_processed IS NOT NULL
            GROUP BY DATE(creation_time)
        ),
        forecast_base AS (
            SELECT 
                AVG(daily_cost_usd) as avg_daily_cost,
                STDDEV(daily_cost_usd) as cost_stddev,
                AVG(query_count) as avg_daily_queries,
                MAX(daily_cost_usd) as max_daily_cost,
                MIN(daily_cost_usd) as min_daily_cost,
                COUNT(*) as days_analyzed
            FROM daily_costs
        )
        SELECT * FROM forecast_base
        """
        
        results = list(bq_client.query(query))
        if not results:
            return json.dumps({"error": "No historical data found"})
        
        base_stats = results[0]
        avg_daily_cost = float(base_stats.avg_daily_cost or 0)
        
        # Apply growth assumptions
        growth_multipliers = {
            "current_trend": 1.0,
            "conservative": 1.1,  # 10% growth
            "aggressive": 1.3     # 30% growth
        }
        
        growth_factor = growth_multipliers.get(growth_assumptions, 1.0)
        
        # Generate forecast
        forecast_data = []
        for day in range(1, days_forecast + 1):
            # Simple linear growth model
            daily_forecast = avg_daily_cost * growth_factor * (1 + (day * 0.001))  # 0.1% daily growth
            forecast_data.append({
                "day": day,
                "forecast_date": (datetime.now() + timedelta(days=day)).strftime("%Y-%m-%d"),
                "forecasted_cost_usd": round(daily_forecast, 2)
            })
        
        total_forecast = sum(d["forecasted_cost_usd"] for d in forecast_data)
        monthly_forecast = total_forecast if days_forecast >= 30 else total_forecast * (30 / days_forecast)
        
        return json.dumps({
            "success": True,
            "forecast_period": {
                "historical_days": days_historical,
                "forecast_days": days_forecast,
                "growth_assumptions": growth_assumptions,
                "growth_factor": growth_factor
            },
            "historical_analysis": {
                "avg_daily_cost": round(avg_daily_cost, 2),
                "cost_stddev": round(float(base_stats.cost_stddev or 0), 2),
                "avg_daily_queries": int(base_stats.avg_daily_queries or 0),
                "max_daily_cost": round(float(base_stats.max_daily_cost or 0), 2),
                "min_daily_cost": round(float(base_stats.min_daily_cost or 0), 2),
                "days_analyzed": int(base_stats.days_analyzed or 0)
            },
            "forecast_summary": {
                "total_forecast_usd": round(total_forecast, 2),
                "monthly_estimate_usd": round(monthly_forecast, 2),
                "avg_daily_forecast": round(total_forecast / days_forecast, 2)
            },
            "daily_forecasts": forecast_data[:10],  # First 10 days for brevity
            "budget_recommendations": [
                f"Set monthly budget alert at ${round(monthly_forecast * 1.1, 2)} (10% buffer)",
                f"Expect daily costs around ${round(avg_daily_cost * growth_factor, 2)}",
                f"Plan for peak costs up to ${round(float(base_stats.max_daily_cost or 0) * growth_factor, 2)}/day"
            ],
            "generated_at": datetime.now().isoformat()
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        })


def analyze_table_hotspots_direct(
    days: int = 7,
    min_access_cost: float = 5.0
) -> str:
    """
    Analyze which tables are most expensive to access and suggest optimizations.
    """
    try:
        # Use default project and client
        project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'gcp-wow-wiq-tsr-dev')
        bq_client = setup_client(project_id)
        
        query = f"""
        WITH table_access_costs AS (
            SELECT 
                job_id,
                user_email,
                creation_time,
                total_bytes_processed / POW(10, 12) * 6.25 as cost_usd,
                total_bytes_processed / POW(10, 12) as tb_processed,
                TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) as duration_ms,
                
                -- Extract table references from query
                REGEXP_EXTRACT_ALL(query, r'`([^`]+\.[^`]+\.[^`]+)`') as table_refs_full,
                REGEXP_EXTRACT_ALL(query, r'FROM\\s+`?([^\\s`]+)`?') as table_refs_from,
                REGEXP_EXTRACT_ALL(query, r'JOIN\\s+`?([^\\s`]+)`?') as table_refs_join
                
            FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
                AND job_type = 'QUERY'
                AND state = 'DONE'
                AND error_result IS NULL
                AND total_bytes_processed IS NOT NULL
                AND total_bytes_processed / POW(10, 12) * 6.25 >= {min_access_cost}
                AND query IS NOT NULL
        ),
        table_costs AS (
            SELECT 
                table_ref,
                COUNT(*) as access_count,
                SUM(cost_usd) as total_cost_usd,
                AVG(cost_usd) as avg_cost_per_access,
                SUM(tb_processed) as total_tb_processed,
                AVG(duration_ms) as avg_duration_ms,
                APPROX_TOP_COUNT(user_email, 3) as top_users
            FROM table_access_costs,
            UNNEST(ARRAY_CONCAT(table_refs_full, table_refs_from, table_refs_join)) as table_ref
            WHERE table_ref IS NOT NULL AND table_ref != ''
            GROUP BY table_ref
        )
        SELECT 
            table_ref,
            access_count,
            total_cost_usd,
            avg_cost_per_access,
            total_tb_processed,
            avg_duration_ms,
            top_users
        FROM table_costs
        WHERE total_cost_usd > {min_access_cost}
        ORDER BY total_cost_usd DESC
        LIMIT 20
        """
        
        results = list(bq_client.query(query))
        
        table_hotspots = []
        total_hotspot_cost = 0
        
        for row in results:
            table_info = {
                "table_name": row.table_ref,
                "access_count": row.access_count,
                "total_cost_usd": round(row.total_cost_usd, 2),
                "avg_cost_per_access": round(row.avg_cost_per_access, 2),
                "total_tb_processed": round(row.total_tb_processed, 2),
                "avg_duration_ms": round(row.avg_duration_ms, 0),
                "top_users": [str(user) for user in (row.top_users or [])[:3]],
                "optimization_priority": "HIGH" if row.total_cost_usd > 100 else "MEDIUM" if row.total_cost_usd > 50 else "LOW"
            }
            table_hotspots.append(table_info)
            total_hotspot_cost += row.total_cost_usd
        
        # Generate recommendations for top tables
        recommendations = []
        for table in table_hotspots[:5]:  # Top 5 tables
            table_recs = []
            
            if table["avg_cost_per_access"] > 10:
                table_recs.append("Add partition filters to reduce data scanning")
                table_recs.append("Consider using SELECT with specific columns instead of SELECT *")
            
            if table["access_count"] > 10:
                table_recs.append("Consider creating materialized views for frequent access patterns")
                table_recs.append("Implement query result caching")
            
            if table["avg_duration_ms"] > 30000:  # > 30 seconds
                table_recs.append("Optimize table clustering for frequently filtered columns")
                table_recs.append("Review and optimize JOIN operations on this table")
            
            recommendations.append({
                "table": table["table_name"],
                "priority": table["optimization_priority"],
                "potential_savings_usd": round(table["total_cost_usd"] * 0.4, 2),  # Conservative 40% savings
                "recommendations": table_recs or ["Review access patterns and optimize queries"]
            })
        
        return json.dumps({
            "success": True,
            "analysis_period": {
                "days": days,
                "min_access_cost": min_access_cost
            },
            "summary": {
                "total_hotspot_tables": len(table_hotspots),
                "total_hotspot_cost": round(total_hotspot_cost, 2),
                "avg_cost_per_table": round(total_hotspot_cost / len(table_hotspots), 2) if table_hotspots else 0,
                "top_table": table_hotspots[0]["table_name"] if table_hotspots else None,
                "top_table_cost": table_hotspots[0]["total_cost_usd"] if table_hotspots else 0
            },
            "table_hotspots": table_hotspots,
            "optimization_recommendations": recommendations,
            "generated_at": datetime.now().isoformat()
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        })


def generate_materialized_view_recommendations_direct(
    days: int = 14,
    min_repetition_count: int = 3,
    min_cost_per_execution: float = 5.0
) -> str:
    """
    Analyze query patterns to recommend materialized views for cost optimization.
    """
    try:
        # Use default project and client
        project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'gcp-wow-wiq-tsr-dev')
        bq_client = setup_client(project_id)
        
        query = f"""
        WITH query_patterns AS (
            SELECT 
                -- Normalize queries by removing literals and variables
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(query, r"'[^']*'", "'<STRING>'"),
                        r"\\b\\d+\\b", "<NUMBER>"
                    ),
                    r"TIMESTAMP\\('[^']*'\\)", "TIMESTAMP('<DATE>')"
                ) as normalized_query,
                
                user_email,
                total_bytes_processed / POW(10, 12) * 6.25 as cost_usd,
                total_bytes_processed / POW(10, 12) as tb_processed,
                TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) as duration_ms,
                creation_time
                
            FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
                AND job_type = 'QUERY'
                AND state = 'DONE'
                AND error_result IS NULL
                AND total_bytes_processed IS NOT NULL
                AND total_bytes_processed / POW(10, 12) * 6.25 >= {min_cost_per_execution}
                AND query IS NOT NULL
                AND LENGTH(query) > 100  -- Filter out simple queries
        ),
        pattern_analysis AS (
            SELECT 
                normalized_query,
                COUNT(*) as execution_count,
                SUM(cost_usd) as total_cost_usd,
                AVG(cost_usd) as avg_cost_per_execution,
                SUM(tb_processed) as total_tb_processed,
                AVG(duration_ms) as avg_duration_ms,
                APPROX_TOP_COUNT(user_email, 3) as frequent_users,
                MIN(creation_time) as first_execution,
                MAX(creation_time) as last_execution
            FROM query_patterns
            GROUP BY normalized_query
            HAVING COUNT(*) >= {min_repetition_count}
        )
        SELECT 
            normalized_query,
            execution_count,
            total_cost_usd,
            avg_cost_per_execution,
            total_tb_processed,
            avg_duration_ms,
            frequent_users,
            first_execution,
            last_execution
        FROM pattern_analysis
        ORDER BY total_cost_usd DESC
        LIMIT 10
        """
        
        results = list(bq_client.query(query))
        
        materialized_view_candidates = []
        total_potential_savings = 0
        
        for row in results:
            # Calculate potential savings (materialized views can save 60-90% of costs for repeated queries)
            savings_pct = 0.75  # Conservative 75% savings estimate
            potential_savings = row.total_cost_usd * savings_pct
            total_potential_savings += potential_savings
            
            # Calculate execution frequency
            execution_frequency = row.execution_count / days
            
            candidate = {
                "query_pattern": row.normalized_query[:500] + "..." if len(row.normalized_query) > 500 else row.normalized_query,
                "execution_count": row.execution_count,
                "total_cost_usd": round(row.total_cost_usd, 2),
                "avg_cost_per_execution": round(row.avg_cost_per_execution, 2),
                "total_tb_processed": round(row.total_tb_processed, 2),
                "avg_duration_ms": round(row.avg_duration_ms, 0),
                "frequent_users": [user.value for user in row.frequent_users],
                "execution_frequency_per_day": round(execution_frequency, 1),
                "potential_savings_usd": round(potential_savings, 2),
                "roi_score": round(potential_savings / (row.avg_cost_per_execution * 0.1), 1),  # Rough ROI estimate
                "priority": "HIGH" if potential_savings > 100 else "MEDIUM" if potential_savings > 50 else "LOW"
            }
            materialized_view_candidates.append(candidate)
        
        # Generate specific recommendations
        recommendations = []
        for candidate in materialized_view_candidates[:5]:  # Top 5 candidates
            mv_name = f"mv_pattern_{hash(candidate['query_pattern'][:100]) % 10000}"
            
            recommendations.append({
                "materialized_view_name": mv_name,
                "priority": candidate["priority"],
                "potential_savings_usd": candidate["potential_savings_usd"],
                "execution_frequency": candidate["execution_frequency_per_day"],
                "implementation_steps": [
                    f"Create materialized view: {mv_name}",
                    "Update queries to use the materialized view",
                    "Set up automatic refresh schedule",
                    "Monitor usage and cost savings"
                ],
                "refresh_strategy": "DAILY" if candidate["execution_frequency_per_day"] > 1 else "HOURLY",
                "estimated_creation_cost": round(candidate["avg_cost_per_execution"] * 0.1, 2),
                "break_even_executions": max(1, int(candidate["avg_cost_per_execution"] * 0.1 / (candidate["avg_cost_per_execution"] * 0.75)))
            })
        
        return json.dumps({
            "success": True,
            "analysis_period": {
                "days": days,
                "min_repetition_count": min_repetition_count,
                "min_cost_per_execution": min_cost_per_execution
            },
            "summary": {
                "total_candidates": len(materialized_view_candidates),
                "total_potential_savings": round(total_potential_savings, 2),
                "avg_savings_per_view": round(total_potential_savings / len(materialized_view_candidates), 2) if materialized_view_candidates else 0,
                "high_priority_views": len([c for c in materialized_view_candidates if c["priority"] == "HIGH"])
            },
            "materialized_view_candidates": materialized_view_candidates,
            "implementation_recommendations": recommendations,
            "generated_at": datetime.now().isoformat()
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        })


def create_optimization_report_direct(
    days: int = 7,
    report_type: str = "executive"
) -> str:
    """
    Generate comprehensive optimization report for different audiences.
    """
    try:
        # Use default project and client
        project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'gcp-wow-wiq-tsr-dev')
        bq_client = setup_client(project_id)
        
        # Get comprehensive data for report
        summary_query = f"""
        WITH cost_analysis AS (
            SELECT 
                COUNT(*) as total_queries,
                COUNT(DISTINCT user_email) as unique_users,
                SUM(total_bytes_processed / POW(10, 12) * 6.25) as total_cost_usd,
                AVG(total_bytes_processed / POW(10, 12) * 6.25) as avg_cost_per_query,
                SUM(total_bytes_processed) / POW(10, 12) as total_tb_processed,
                
                -- Cost patterns
                SUM(CASE WHEN total_bytes_processed / POW(10, 12) * 6.25 > 50 THEN 1 ELSE 0 END) as expensive_queries,
                SUM(CASE WHEN query LIKE '%SELECT *%' THEN 1 ELSE 0 END) as select_star_queries,
                SUM(CASE WHEN query NOT LIKE '%WHERE%' THEN 1 ELSE 0 END) as unfiltered_queries,
                SUM(CASE WHEN NOT cache_hit THEN 1 ELSE 0 END) as uncached_queries,
                
                -- User patterns
                SUM(CASE WHEN user_email LIKE '%gserviceaccount.com' THEN total_bytes_processed / POW(10, 12) * 6.25 ELSE 0 END) as service_account_cost,
                SUM(CASE WHEN user_email NOT LIKE '%gserviceaccount.com' THEN total_bytes_processed / POW(10, 12) * 6.25 ELSE 0 END) as human_user_cost
                
            FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
                AND job_type = 'QUERY'
                AND state = 'DONE'
                AND error_result IS NULL
                AND total_bytes_processed IS NOT NULL
        )
        SELECT * FROM cost_analysis
        """
        
        results = list(bq_client.query(summary_query))
        if not results:
            return json.dumps({"error": "No data found for analysis period"})
        
        summary = results[0]
        
        # Calculate optimization potential
        select_star_savings = (summary.select_star_queries / summary.total_queries) * summary.total_cost_usd * 0.4
        filter_savings = (summary.unfiltered_queries / summary.total_queries) * summary.total_cost_usd * 0.6
        cache_savings = (summary.uncached_queries / summary.total_queries) * summary.total_cost_usd * 0.2
        
        total_potential_savings = select_star_savings + filter_savings + cache_savings
        
        # Generate report based on audience
        if report_type == "executive":
            report_content = {
                "report_title": f"BigQuery Cost Optimization Report - {days} Day Executive Summary",
                "key_metrics": {
                    "total_cost_usd": round(summary.total_cost_usd, 2),
                    "monthly_projection_usd": round(summary.total_cost_usd * (30 / days), 2),
                    "total_queries": summary.total_queries,
                    "unique_users": summary.unique_users,
                    "data_processed_tb": round(summary.total_tb_processed, 2)
                },
                "optimization_opportunities": {
                    "immediate_savings_potential": round(total_potential_savings, 2),
                    "monthly_savings_projection": round(total_potential_savings * (30 / days), 2),
                    "roi_percentage": round((total_potential_savings / summary.total_cost_usd) * 100, 1),
                    "top_opportunities": [
                        f"Query filtering optimization: ${round(filter_savings, 2)} potential savings",
                        f"Column selection optimization: ${round(select_star_savings, 2)} potential savings",
                        f"Query caching improvements: ${round(cache_savings, 2)} potential savings"
                    ]
                },
                "recommendations": [
                    "Implement query optimization guidelines across teams",
                    "Set up cost monitoring and alerts",
                    "Train users on BigQuery best practices",
                    "Consider automated query optimization tools"
                ]
            }
            
        elif report_type == "technical":
            report_content = {
                "report_title": f"BigQuery Technical Optimization Analysis - {days} Days",
                "technical_metrics": {
                    "total_cost_usd": round(summary.total_cost_usd, 2),
                    "avg_cost_per_query": round(summary.avg_cost_per_query, 4),
                    "total_data_processed_tb": round(summary.total_tb_processed, 2),
                    "expensive_queries_count": summary.expensive_queries,
                    "expensive_queries_pct": round((summary.expensive_queries / summary.total_queries) * 100, 1)
                },
                "optimization_analysis": {
                    "select_star_issues": {
                        "count": summary.select_star_queries,
                        "percentage": round((summary.select_star_queries / summary.total_queries) * 100, 1),
                        "potential_savings": round(select_star_savings, 2)
                    },
                    "filtering_issues": {
                        "count": summary.unfiltered_queries,
                        "percentage": round((summary.unfiltered_queries / summary.total_queries) * 100, 1),
                        "potential_savings": round(filter_savings, 2)
                    },
                    "caching_issues": {
                        "count": summary.uncached_queries,
                        "percentage": round((summary.uncached_queries / summary.total_queries) * 100, 1),
                        "potential_savings": round(cache_savings, 2)
                    }
                },
                "implementation_priorities": [
                    "HIGH: Implement partition filtering on expensive queries",
                    "HIGH: Replace SELECT * with specific columns",
                    "MEDIUM: Enable query result caching",
                    "MEDIUM: Optimize JOIN operations",
                    "LOW: Implement LIMIT clauses where appropriate"
                ]
            }
            
        else:  # stakeholder
            report_content = {
                "report_title": f"BigQuery Cost & Usage Report - {days} Day Stakeholder Summary",
                "business_impact": {
                    "current_monthly_cost": round(summary.total_cost_usd * (30 / days), 2),
                    "cost_per_user": round(summary.total_cost_usd / summary.unique_users, 2),
                    "queries_per_day": round(summary.total_queries / days, 0),
                    "service_vs_human_cost": {
                        "automated_processes": round(summary.service_account_cost, 2),
                        "human_users": round(summary.human_user_cost, 2)
                    }
                },
                "cost_optimization_summary": {
                    "optimization_potential": round(total_potential_savings, 2),
                    "percentage_improvement": round((total_potential_savings / summary.total_cost_usd) * 100, 1),
                    "monthly_savings_target": round(total_potential_savings * (30 / days), 2)
                },
                "action_items": [
                    "Review and approve optimization initiatives",
                    "Allocate resources for query optimization training",
                    "Implement cost governance policies",
                    "Set up regular cost review meetings"
                ]
            }
        
        report_content.update({
            "report_metadata": {
                "analysis_period": f"{days} days",
                "report_type": report_type,
                "generated_at": datetime.now().isoformat(),
                "project_id": project_id
            }
        })
        
        return json.dumps({
            "success": True,
            "report": report_content,
            "raw_metrics": {
                "total_queries": summary.total_queries,
                "total_cost_usd": round(summary.total_cost_usd, 2),
                "total_potential_savings": round(total_potential_savings, 2)
            }
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        })
