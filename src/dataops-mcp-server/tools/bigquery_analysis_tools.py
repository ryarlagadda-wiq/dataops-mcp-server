#!/usr/bin/env python3

"""
Refactored BigQuery Cost Analysis MCP Server
Modular, faster, and simplified implementation.
"""

import os
import json
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass
from functools import lru_cache, wraps
from concurrent.futures import ThreadPoolExecutor

from fastmcp import FastMCP
from google.cloud import bigquery


# Data Classes for Type Safety
@dataclass
class QueryConfig:
    """Configuration for BigQuery queries."""
    days: int
    project_id: str
    service_account_filter: str = ""
    include_query_text: bool = False
    min_cost_threshold: float = 0.0
    limit: int = 10
    
    def __post_init__(self):
        """Validate configuration parameters."""
        if not (1 <= self.days <= 30):
            raise ValueError("Days must be between 1 and 30")
        if not (1 <= self.limit <= 50):
            raise ValueError("Limit must be between 1 and 50")
        if self.min_cost_threshold < 0:
            raise ValueError("Cost threshold must be non-negative")
        if not self.project_id:
            raise ValueError("Project ID cannot be empty")


@dataclass
class CostMetrics:
    """Standard cost metrics structure."""
    total_cost: float
    query_count: int
    avg_cost: float
    max_cost: float
    unique_users: int = 0
    active_days: int = 0


class BigQueryAnalyzer:
    """Core BigQuery analysis functionality."""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        self.executor = ThreadPoolExecutor(max_workers=3)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.cleanup()
    
    def cleanup(self):
        """Clean up resources."""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)
    
    # SQL Query Templates
    BASE_WHERE_CLAUSE = """
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            AND job_type = 'QUERY'
            AND state = 'DONE'
            AND error_result IS NULL
            AND total_bytes_processed IS NOT NULL
    """
    
    COST_CALCULATION = "total_bytes_processed / POW(10, 12) * 6.25"
    
    @lru_cache(maxsize=32)
    def _build_base_query(self, days: int, additional_filters: str = "") -> str:
        """Build base query with caching."""
        base_where = self.BASE_WHERE_CLAUSE.format(days=days)
        if additional_filters:
            base_where += f" AND {additional_filters}"
        return base_where
    
    def _add_service_account_filter(self, config: QueryConfig) -> str:
        """Generate service account filter clause."""
        if config.service_account_filter:
            return f"user_email LIKE '%{config.service_account_filter}%'"
        return ""
    
    def _add_cost_threshold_filter(self, config: QueryConfig) -> str:
        """Generate cost threshold filter clause."""
        if config.min_cost_threshold > 0:
            return f"{self.COST_CALCULATION} >= {config.min_cost_threshold}"
        return ""
    
    def _execute_query(self, query: str) -> List[Any]:
        """Execute BigQuery query."""
        return list(self.client.query(query))
    
    async def _execute_query_async(self, query: str) -> List[Any]:
        """Execute BigQuery asynchronously using thread executor."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self._execute_query, query)
    
    def _build_daily_costs_query(self, config: QueryConfig) -> str:
        """Build optimized daily costs query."""
        filters = []
        
        if sa_filter := self._add_service_account_filter(config):
            filters.append(sa_filter)
        
        additional_filters = " AND ".join(filters)
        base_where = self._build_base_query(config.days, additional_filters)
        
        return f"""
        SELECT 
            DATE(creation_time) as date,
            COUNT(*) as query_count,
            SUM({self.COST_CALCULATION}) as cost_usd,
            ROUND(AVG(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)), 2) as avg_duration_ms
        FROM `{self.project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        {base_where}
        GROUP BY DATE(creation_time)
        ORDER BY date DESC
        """
    
    def _build_top_users_query(self, config: QueryConfig) -> str:
        """Build optimized top users query."""
        base_where = self._build_base_query(config.days, "user_email IS NOT NULL")
        
        return f"""
        SELECT 
            user_email,
            COUNT(*) as query_count,
            SUM({self.COST_CALCULATION}) as cost_usd,
            ROUND(AVG(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)), 2) as avg_duration_ms
        FROM `{self.project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        {base_where}
        GROUP BY user_email
        ORDER BY cost_usd DESC
        LIMIT {config.limit}
        """
    
    def _build_cost_summary_query(self, config: QueryConfig) -> str:
        """Build optimized cost summary query."""
        base_where = self._build_base_query(config.days)
        
        return f"""
        SELECT 
            COUNT(*) as total_queries,
            SUM({self.COST_CALCULATION}) as total_cost_usd,
            AVG({self.COST_CALCULATION}) as avg_cost_per_query,
            MAX({self.COST_CALCULATION}) as max_query_cost,
            COUNT(DISTINCT user_email) as unique_users,
            COUNT(DISTINCT DATE(creation_time)) as active_days
        FROM `{self.project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        {base_where}
        """
    
    def _process_daily_costs_results(self, results: List[Any]) -> Tuple[List[Dict], float]:
        """Process daily costs query results."""
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
        
        return daily_costs, total_cost
    
    def _process_results(self, results: List[Any], 
                               field_mapping: Dict[str, str]) -> Tuple[List[Dict], float]:
        processed_results = []
        total_cost = 0
        
        for row in results:
            entry = {}
            for output_field, input_field in field_mapping.items():
                value = getattr(row, input_field, 0)
                
                # Apply type conversions and formatting
                if output_field.endswith('_usd') or output_field == 'cost_usd':
                    value = round(float(value or 0), 2)
                    total_cost += value
                elif output_field.endswith('_count') or output_field in ['query_count']:
                    value = int(value or 0)
                elif output_field.endswith('_ms'):
                    value = float(value or 0)
                elif output_field == 'date':
                    value = value.isoformat() if hasattr(value, 'isoformat') else str(value)
                else:
                    value = str(value) if value is not None else ""
                
                entry[output_field] = value
            
            processed_results.append(entry)
        
        return processed_results, total_cost
    
    def _process_top_users_results(self, results: List[Any]) -> Tuple[List[Dict], float]:
        """Process top users query results."""
        field_mapping = {
            "user_email": "user_email",
            "query_count": "query_count",
            "cost_usd": "cost_usd", 
            "avg_duration_ms": "avg_duration_ms"
        }
        return self._process_results_generic(results, field_mapping)
    
    def _generate_cost_insights(self, metrics: CostMetrics, config: QueryConfig) -> List[str]:
        """Generate intelligent cost insights."""
        insights = []
        
        if metrics.avg_cost > 1.0:
            insights.append(f"High average query cost: ${metrics.avg_cost:.2f} - consider query optimization")
        
        if metrics.max_cost > 10.0:
            insights.append(f"Very expensive single query detected: ${metrics.max_cost:.2f}")
        
        if metrics.active_days < config.days:
            insights.append(f"BigQuery not used every day ({metrics.active_days}/{config.days} active days)")
        
        return insights
    
    def _calculate_projections(self, total_cost: float, active_days: int) -> Dict[str, float]:
        """Calculate cost projections."""
        daily_avg = total_cost / max(active_days, 1)
        return {
            "daily_average": round(daily_avg, 2),
            "weekly_projection": round(daily_avg * 7, 2),
            "monthly_projection": round(daily_avg * 30, 2)
        }
    
    # Public API Methods
    async def get_daily_costs_async(self, config: QueryConfig) -> Dict[str, Any]:
        """Get daily costs asynchronously."""
        query = self._build_daily_costs_query(config)
        results = await self._execute_query_async(query)
        daily_costs, total_cost = self._process_daily_costs_results(results)
        
        return {
            "success": True,
            "project_id": self.project_id,
            "period_days": config.days,
            "total_cost_usd": round(total_cost, 2),
            "daily_costs": daily_costs
        }
    
    def get_daily_costs_sync(self, config: QueryConfig) -> Dict[str, Any]:
        """Get daily costs synchronously."""
        query = self._build_daily_costs_query(config)
        results = self._execute_query(query)
        daily_costs, total_cost = self._process_daily_costs_results(results)
        
        return {
            "success": True,
            "project_id": self.project_id,
            "period_days": config.days,
            "total_cost_usd": round(total_cost, 2),
            "daily_costs": daily_costs
        }
    
    async def get_top_users_async(self, config: QueryConfig) -> Dict[str, Any]:
        """Get top users asynchronously."""
        query = self._build_top_users_query(config)
        results = await self._execute_query_async(query)
        top_users, total_cost = self._process_top_users_results(results)
        
        return {
            "success": True,
            "project_id": self.project_id,
            "period_days": config.days,
            "total_analyzed_cost": round(total_cost, 2),
            "top_users": top_users
        }
    
    def get_top_users_sync(self, config: QueryConfig) -> Dict[str, Any]:
        """Get top users synchronously."""
        query = self._build_top_users_query(config)
        results = self._execute_query(query)
        top_users, total_cost = self._process_top_users_results(results)
        
        return {
            "success": True,
            "project_id": self.project_id,
            "period_days": config.days,
            "total_analyzed_cost": round(total_cost, 2),
            "top_users": top_users
        }
    
    def get_cost_summary_sync(self, config: QueryConfig) -> Dict[str, Any]:
        """Get comprehensive cost summary."""
        query = self._build_cost_summary_query(config)
        results = self._execute_query(query)
        result = results[0]
        
        metrics = CostMetrics(
            total_cost=float(result.total_cost_usd or 0),
            query_count=int(result.total_queries),
            avg_cost=float(result.avg_cost_per_query or 0),
            max_cost=float(result.max_query_cost or 0),
            unique_users=int(result.unique_users or 0),
            active_days=int(result.active_days)
        )
        
        insights = self._generate_cost_insights(metrics, config)
        projections = self._calculate_projections(metrics.total_cost, metrics.active_days)
        
        return {
            "success": True,
            "project_id": self.project_id,
            "analysis_period": {
                "days": config.days,
                "start_date": (datetime.now() - timedelta(days=config.days)).date().isoformat(),
                "end_date": datetime.now().date().isoformat()
            },
            "summary": {
                "total_cost_usd": round(metrics.total_cost, 2),
                "total_queries": metrics.query_count,
                "avg_cost_per_query": round(metrics.avg_cost, 4),
                "max_query_cost": round(metrics.max_cost, 2),
                "unique_users": metrics.unique_users,
                "active_days": metrics.active_days
            },
            "projections": projections,
            "insights": insights,
            "generated_at": datetime.now().isoformat()
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Check BigQuery connectivity and permissions."""
        try:
            # Test basic BigQuery access
            test_query = "SELECT 1 as test"
            job_config = bigquery.QueryJobConfig(dry_run=True)
            self.client.query(test_query, job_config=job_config)
            
            # Test INFORMATION_SCHEMA access
            schema_query = f"""
            SELECT COUNT(*) as job_count
            FROM `{self.project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
            LIMIT 1
            """
            job_config = bigquery.QueryJobConfig(dry_run=True)
            self.client.query(schema_query, job_config=job_config)
            
            return {
                "success": True,
                "status": "healthy",
                "project_id": self.project_id,
                "message": "BigQuery access confirmed",
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                "success": False,
                "status": "unhealthy",
                "error": str(e),
                "suggestions": [
                    "Check GOOGLE_APPLICATION_CREDENTIALS environment variable",
                    "Verify BigQuery API is enabled",
                    "Ensure proper IAM permissions (bigquery.jobs.listAll)"
                ]
            }


# Error Handling Decorator
def handle_bigquery_errors(func):
    """Decorator for consistent error handling."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            return json.dumps({
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "suggestion": "Check project permissions and query parameters"
            })
    return wrapper


def handle_bigquery_errors_async(func):
    """Async decorator for consistent error handling."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            return json.dumps({
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "suggestion": "Check project permissions and query parameters"
            })
    return wrapper


# Initialize components
project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
if not project_id:
    raise ValueError("GOOGLE_CLOUD_PROJECT environment variable is required")

analyzer = BigQueryAnalyzer(project_id)
mcp = FastMCP("BigQuery Cost Analyzer")


# MCP Tool Functions
@mcp.tool()
@handle_bigquery_errors
def get_daily_costs(days: int = 7) -> str:
    """Get daily BigQuery costs for the specified number of days."""
    config = QueryConfig(days=days, project_id=project_id)
    result = analyzer.get_daily_costs_sync(config)
    return json.dumps(result, indent=2)


@mcp.tool()
@handle_bigquery_errors_async
async def get_daily_costs_async(days: int = 7) -> str:
    """Get daily BigQuery costs asynchronously."""
    config = QueryConfig(days=days, project_id=project_id)
    result = await analyzer.get_daily_costs_async(config)
    return json.dumps(result, indent=2)


@mcp.tool()
@handle_bigquery_errors
def get_top_users(days: int = 7, limit: int = 10) -> str:
    """Get top BigQuery users by cost over the specified period."""
    config = QueryConfig(days=days, project_id=project_id, limit=limit)
    result = analyzer.get_top_users_sync(config)
    return json.dumps(result, indent=2)


@mcp.tool()
@handle_bigquery_errors_async
async def get_top_users_async(days: int = 7, limit: int = 10) -> str:
    """Get top BigQuery users asynchronously."""
    config = QueryConfig(days=days, project_id=project_id, limit=limit)
    result = await analyzer.get_top_users_async(config)
    return json.dumps(result, indent=2)


@mcp.tool()
@handle_bigquery_errors
def get_cost_summary(days: int = 7) -> str:
    """Get a comprehensive cost summary for BigQuery usage."""
    config = QueryConfig(days=days, project_id=project_id)
    result = analyzer.get_cost_summary_sync(config)
    return json.dumps(result, indent=2)


@mcp.tool()
@handle_bigquery_errors
def health_check() -> str:
    """Check if the BigQuery cost analyzer can access the project successfully."""
    result = analyzer.health_check()
    return json.dumps(result, indent=2)


if __name__ == "__main__":
    try:
        mcp.run()
    finally:
        analyzer.cleanup()
