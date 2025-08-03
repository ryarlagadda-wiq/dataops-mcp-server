#!/usr/bin/env python3
"""
Unified BigQuery Client - Shared component for all MCP servers
Provides common BigQuery operations and utilities.
"""

import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from google.cloud import bigquery


@dataclass
class QueryConfig:
    """Standard configuration for BigQuery queries."""
    days: int = 7
    project_id: str = ""
    limit: int = 10
    min_cost_threshold: float = 0.0
    service_account_filter: str = ""
    include_query_text: bool = False
    
    def __post_init__(self):
        if not self.project_id:
            self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "")
        if not self.project_id:
            raise ValueError("GOOGLE_CLOUD_PROJECT environment variable required")
        if not (1 <= self.days <= 90):
            raise ValueError("Days must be between 1 and 90")
        if not (1 <= self.limit <= 500):
            raise ValueError("Limit must be between 1 and 500")


@dataclass  
class CostMetrics:
    """Standard cost metrics structure."""
    total_cost: float
    query_count: int
    avg_cost: float
    max_cost: float
    unique_users: int = 0
    active_days: int = 0
    cache_hit_rate: float = 0.0


class UnifiedBigQueryClient:
    """
    Unified BigQuery client with common operations.
    Shared across all MCP servers for consistency.
    """
    
    def __init__(self, project_id: Optional[str] = None):
        self.project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        if not self.project_id:
            raise ValueError("GOOGLE_CLOUD_PROJECT environment variable required")
        
        self.client = bigquery.Client(project=self.project_id)
        
        # Standard SQL fragments
        self.COST_CALCULATION = "total_bytes_processed / POW(10, 12) * 6.25"
        self.BASE_JOB_FILTERS = """
            job_type = 'QUERY'
            AND state = 'DONE'
            AND error_result IS NULL
            AND total_bytes_processed IS NOT NULL
        """
    
    def execute_query(self, query: str, timeout: int = 60) -> List[Any]:
        """Execute BigQuery query with timeout and error handling."""
        try:
            job_config = bigquery.QueryJobConfig(
                use_query_cache=True,
                maximum_bytes_billed=100 * 1024**4  # 100 TB limit
            )
            query_job = self.client.query(query, job_config=job_config)
            return list(query_job.result(timeout=timeout))
        except Exception as e:
            raise Exception(f"BigQuery execution error: {str(e)}")
    
    def dry_run_query(self, query: str) -> Dict[str, Any]:
        """Perform dry run to validate query and estimate cost."""
        try:
            job_config = bigquery.QueryJobConfig(dry_run=True)
            query_job = self.client.query(query, job_config=job_config)
            
            return {
                "valid": True,
                "bytes_processed": query_job.total_bytes_processed,
                "estimated_cost_usd": (query_job.total_bytes_processed / (1024**4)) * 6.25
            }
        except Exception as e:
            return {
                "valid": False,
                "error": str(e)
            }
    
    def build_time_filter(self, days: int) -> str:
        """Build time-based filter clause."""
        return f"creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)"
    
    def build_base_where_clause(self, config: QueryConfig) -> str:
        """Build standard WHERE clause for job analysis."""
        conditions = [
            self.build_time_filter(config.days),
            self.BASE_JOB_FILTERS
        ]
        
        if config.min_cost_threshold > 0:
            conditions.append(f"{self.COST_CALCULATION} >= {config.min_cost_threshold}")
        
        if config.service_account_filter:
            conditions.append(f"user_email LIKE '%{config.service_account_filter}%'")
        
        return "WHERE " + " AND ".join(conditions)
    
    def build_daily_costs_query(self, config: QueryConfig) -> str:
        """Build optimized daily costs query."""
        return f"""
        SELECT 
            DATE(creation_time) as date,
            COUNT(*) as query_count,
            SUM({self.COST_CALCULATION}) as cost_usd,
            ROUND(AVG(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)), 2) as avg_duration_ms,
            COUNT(DISTINCT user_email) as unique_users,
            COUNT(CASE WHEN cache_hit THEN 1 END) as cache_hits
        FROM `{self.project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        {self.build_base_where_clause(config)}
        GROUP BY DATE(creation_time)
        ORDER BY date DESC
        """
    
    def build_top_users_query(self, config: QueryConfig) -> str:
        """Build optimized top users query."""
        base_where = self.build_base_where_clause(config)
        base_where += " AND user_email IS NOT NULL"
        
        return f"""
        SELECT 
            user_email,
            CASE 
                WHEN user_email LIKE '%gserviceaccount.com' THEN 'Service Account'
                WHEN user_email LIKE '%@%.%' THEN 'User Account'
                ELSE 'Unknown'
            END as account_type,
            COUNT(*) as query_count,
            SUM({self.COST_CALCULATION}) as cost_usd,
            ROUND(AVG({self.COST_CALCULATION}), 4) as avg_cost_per_query,
            ROUND(MAX({self.COST_CALCULATION}), 2) as max_query_cost,
            ROUND(AVG(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)), 2) as avg_duration_ms,
            COUNT(CASE WHEN cache_hit THEN 1 END) as cache_hits
        FROM `{self.project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        {base_where}
        GROUP BY user_email, account_type
        ORDER BY cost_usd DESC
        LIMIT {config.limit}
        """
    
    def build_cost_summary_query(self, config: QueryConfig) -> str:
        """Build comprehensive cost summary query."""
        return f"""
        SELECT 
            COUNT(*) as total_queries,
            SUM({self.COST_CALCULATION}) as total_cost_usd,
            AVG({self.COST_CALCULATION}) as avg_cost_per_query,
            MAX({self.COST_CALCULATION}) as max_query_cost,
            COUNT(DISTINCT user_email) as unique_users,
            COUNT(DISTINCT DATE(creation_time)) as active_days,
            COUNT(CASE WHEN cache_hit THEN 1 END) as cache_hits,
            COUNT(DISTINCT destination_table.dataset_id) as datasets_accessed,
            AVG(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)) as avg_duration_ms,
            SUM(total_bytes_processed) as total_bytes_processed
        FROM `{self.project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        {self.build_base_where_clause(config)}
        """
    
    def process_daily_costs_results(self, results: List[Any]) -> Dict[str, Any]:
        """Process daily costs query results into standard format."""
        daily_costs = []
        total_cost = 0
        total_queries = 0
        
        for row in results:
            cost_entry = {
                "date": row.date.isoformat(),
                "query_count": int(row.query_count),
                "cost_usd": round(float(row.cost_usd or 0), 2),
                "avg_duration_ms": float(row.avg_duration_ms or 0),
                "unique_users": int(row.unique_users or 0),
                "cache_hits": int(row.cache_hits or 0),
                "cache_hit_rate": round((int(row.cache_hits or 0) / max(int(row.query_count), 1)) * 100, 1)
            }
            daily_costs.append(cost_entry)
            total_cost += cost_entry["cost_usd"]
            total_queries += cost_entry["query_count"]
        
        return {
            "daily_costs": daily_costs,
            "summary": {
                "total_cost_usd": round(total_cost, 2),
                "total_queries": total_queries,
                "avg_cost_per_day": round(total_cost / max(len(daily_costs), 1), 2),
                "avg_queries_per_day": round(total_queries / max(len(daily_costs), 1), 0)
            }
        }
    
    def process_top_users_results(self, results: List[Any]) -> Dict[str, Any]:
        """Process top users query results into standard format."""
        top_users = []
        total_analyzed_cost = 0
        
        for row in results:
            user_entry = {
                "user_email": row.user_email,
                "account_type": row.account_type,
                "query_count": int(row.query_count),
                "cost_usd": round(float(row.cost_usd or 0), 2),
                "avg_cost_per_query": float(row.avg_cost_per_query or 0),
                "max_query_cost": float(row.max_query_cost or 0),
                "avg_duration_ms": float(row.avg_duration_ms or 0),
                "cache_hits": int(row.cache_hits or 0),
                "cache_hit_rate": round((int(row.cache_hits or 0) / max(int(row.query_count), 1)) * 100, 1)
            }
            top_users.append(user_entry)
            total_analyzed_cost += user_entry["cost_usd"]
        
        return {
            "top_users": top_users,
            "summary": {
                "total_analyzed_cost": round(total_analyzed_cost, 2),
                "users_analyzed": len(top_users),
                "service_accounts": len([u for u in top_users if u["account_type"] == "Service Account"]),
                "user_accounts": len([u for u in top_users if u["account_type"] == "User Account"])
            }
        }
    
    def generate_cost_insights(self, metrics: CostMetrics, config: QueryConfig) -> List[str]:
        """Generate intelligent cost insights."""
        insights = []
        
        if metrics.avg_cost > 1.0:
            insights.append(f"High average query cost: ${metrics.avg_cost:.2f} - consider query optimization")
        
        if metrics.max_cost > 10.0:
            insights.append(f"Very expensive single query detected: ${metrics.max_cost:.2f}")
        
        if metrics.active_days < config.days:
            insights.append(f"BigQuery not used every day ({metrics.active_days}/{config.days} active days)")
        
        if metrics.cache_hit_rate < 10:
            insights.append(f"Low cache hit rate: {metrics.cache_hit_rate:.1f}% - queries may not be reusable")
        elif metrics.cache_hit_rate > 50:
            insights.append(f"Excellent cache utilization: {metrics.cache_hit_rate:.1f}% cache hit rate")
        
        if metrics.query_count > 10000 and metrics.avg_cost < 0.01:
            insights.append("High query volume with low individual costs - good query efficiency")
        
        return insights
    
    def calculate_cost_projections(self, total_cost: float, active_days: int) -> Dict[str, float]:
        """Calculate cost projections based on current usage."""
        daily_avg = total_cost / max(active_days, 1)
        return {
            "daily_average": round(daily_avg, 2),
            "weekly_projection": round(daily_avg * 7, 2),
            "monthly_projection": round(daily_avg * 30, 2),
            "quarterly_projection": round(daily_avg * 90, 2),
            "annual_projection": round(daily_avg * 365, 2)
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check for BigQuery connectivity."""
        try:
            checks = []
            
            # Test 1: Basic BigQuery access
            test_query = "SELECT 1 as test"
            job_config = bigquery.QueryJobConfig(dry_run=True)
            self.client.query(test_query, job_config=job_config)
            checks.append({"name": "basic_access", "status": "passed", "message": "BigQuery API accessible"})
            
            # Test 2: INFORMATION_SCHEMA access
            schema_query = f"""
            SELECT COUNT(*) as job_count
            FROM `{self.project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
            LIMIT 1
            """
            job_config = bigquery.QueryJobConfig(dry_run=True)
            self.client.query(schema_query, job_config=job_config)
            checks.append({"name": "information_schema", "status": "passed", "message": "INFORMATION_SCHEMA access confirmed"})
            
            # Test 3: Recent job data availability
            recent_jobs_query = f"""
            SELECT COUNT(*) as recent_jobs
            FROM `{self.project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
            """
            results = self.execute_query(recent_jobs_query)
            recent_jobs = results[0].recent_jobs if results else 0
            checks.append({
                "name": "recent_data", 
                "status": "passed" if recent_jobs > 0 else "warning",
                "message": f"Found {recent_jobs} jobs in last 7 days"
            })
            
            return {
                "success": True,
                "status": "healthy",
                "project_id": self.project_id,
                "message": "All BigQuery health checks passed",
                "timestamp": datetime.now().isoformat(),
                "checks": checks
            }
            
        except Exception as e:
            return {
                "success": False,
                "status": "unhealthy",
                "project_id": self.project_id,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "suggestions": [
                    "Check GOOGLE_APPLICATION_CREDENTIALS environment variable",
                    "Verify BigQuery API is enabled for the project",
                    "Ensure IAM permissions: bigquery.jobs.listAll, bigquery.jobs.create",
                    "Confirm project ID is correct and accessible"
                ]
            }


def create_standard_response(success: bool, data: Dict[str, Any], 
                           project_id: str, error: str = None) -> str:
    """Create standardized JSON response format."""
    response = {
        "success": success,
        "project_id": project_id,
        "timestamp": datetime.now().isoformat()
    }
    
    if success:
        response.update(data)
    else:
        response["error"] = error
        response["suggestion"] = "Check project permissions and query parameters"
    
    return json.dumps(response, indent=2)


# Singleton instance for reuse across servers
_client_instance = None

def get_bigquery_client(project_id: Optional[str] = None) -> UnifiedBigQueryClient:
    """Get or create singleton BigQuery client instance."""
    global _client_instance
    if _client_instance is None or (project_id and _client_instance.project_id != project_id):
        _client_instance = UnifiedBigQueryClient(project_id)
    return _client_instance
