#!/usr/bin/env python3

"""
Advanced BigQuery Optimization Tools - Add these to your existing MCP server
"""

import re
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

# Add these tools to your existing MCP server

@mcp.tool()
def analyze_expensive_queries(
    days: int = 7,
    min_cost_threshold: float = 10.0,
    categorize_by: str = "cost_driver"  # cost_driver | usage_pattern | optimization_opportunity
) -> str:
    """
    Analyze and categorize expensive queries with optimization recommendations.
    
    Args:
        days: Number of days to analyze (1-30, default: 7)
        min_cost_threshold: Minimum cost to be considered expensive (default: 10.0)
        categorize_by: Categorization method (cost_driver, usage_pattern, optimization_opportunity)
    
    Returns:
        JSON string with categorized expensive queries and optimization suggestions
    """
    if not 1 <= days <= 30:
        return json.dumps({"error": "Days must be between 1 and 30"})
    
    try:
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
        optimization_recommendations = []
        
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
                    "query_count": 0,
                    "avg_cost": 0
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

@mcp.tool()
def detect_optimization_patterns(
    days: int = 7,
    min_cost_threshold: float = 5.0
) -> str:
    """
    Detect specific optimization patterns in expensive queries with actionable recommendations.
    
    Args:
        days: Number of days to analyze (1-30, default: 7)
        min_cost_threshold: Minimum cost to analyze (default: 5.0)
    
    Returns:
        JSON string with detected patterns and specific optimization suggestions
    """
    if not 1 <= days <= 30:
        return json.dumps({"error": "Days must be between 1 and 30"})
    
    try:
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
            "no_cache_utilization": [],
            "repeated_expensive_queries": [],
            "inefficient_string_operations": []
        }
        
        total_potential_savings = 0
        
        for row in results:
            query_text = row.query.upper() if row.query else ""
            cost = row.cost_usd
            
            optimizations = []
            
            # Pattern 1: SELECT * with large data scan
            if "SELECT *" in query_text and row.tb_processed > 1:
                patterns_detected["select_star_large_scan"].append({
                    "job_id": row.job_id,
                    "user_email": row.user_email,
                    "cost_usd": cost,
                    "tb_processed": row.tb_processed,
                    "potential_savings_usd": cost * 0.4,  # 40% savings potential
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
                    "potential_savings_usd": cost * 0.6,  # 60% savings potential
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
                    "potential_savings_usd": cost * 0.5,  # 50% savings potential
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
                    "potential_savings_usd": cost * 0.25,  # 25% savings potential
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
                    "potential_savings_usd": cost * 0.3,  # 30% savings potential
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
                    "potential_savings_usd": cost * 0.8,  # 80% savings if cached
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

@mcp.tool()
def analyze_query_pre_execution(
    sql: str,
    include_optimization_suggestions: bool = True
) -> str:
    """
    Analyze query cost and performance before execution using dry-run.
    
    Args:
        sql: SQL query to analyze
        include_optimization_suggestions: Include optimization recommendations
    
    Returns:
        JSON string with cost estimate and optimization suggestions
    """
    if not sql or not sql.strip():
        return json.dumps({"error": "SQL query cannot be empty"})
    
    try:
        # Perform dry-run analysis
        job_config = bigquery.QueryJobConfig(
            dry_run=True,
            use_query_cache=False
        )
        
        job = bq_client.query(sql, job_config=job_config)
        
        # Calculate cost estimate
        bytes_processed = job.total_bytes_processed or 0
        tb_processed = bytes_processed / (1024 ** 4)
        estimated_cost = tb_processed * 6.25
        
        # Determine cost tier
        if estimated_cost >= 50:
            cost_tier = "CRITICAL"
        elif estimated_cost >= 10:
            cost_tier = "HIGH"
        elif estimated_cost >= 1:
            cost_tier = "MEDIUM"
        else:
            cost_tier = "LOW"
        
        # Analyze query complexity
        sql_upper = sql.upper()
        complexity_factors = {
            "query_length": len(sql),
            "join_count": sql_upper.count("JOIN"),
            "subquery_count": sql_upper.count("SELECT") - 1,
            "window_function_count": sql_upper.count("OVER("),
            "case_statement_count": sql_upper.count("CASE"),
            "union_count": sql_upper.count("UNION")
        }
        
        complexity_score = min(1.0, (
            complexity_factors["join_count"] * 0.1 +
            complexity_factors["subquery_count"] * 0.05 +
            complexity_factors["window_function_count"] * 0.15 +
            complexity_factors["case_statement_count"] * 0.05 +
            complexity_factors["union_count"] * 0.1 +
            min(len(sql) / 5000, 0.2)
        ))
        
        # Generate optimization suggestions
        optimization_suggestions = []
        potential_savings = 0
        
        if include_optimization_suggestions:
            # Check for common patterns
            if "SELECT *" in sql_upper:
                optimization_suggestions.append({
                    "pattern": "SELECT_STAR",
                    "description": "Replace SELECT * with specific column names",
                    "potential_savings_percent": 40,
                    "potential_savings_usd": estimated_cost * 0.4,
                    "implementation": "List only required columns to reduce data processing"
                })
                potential_savings += estimated_cost * 0.4
            
            if not any(filter_word in sql_upper for filter_word in 
                      ['WHERE', '_PARTITIONTIME', '_PARTITIONDATE', 'DATE(']):
                optimization_suggestions.append({
                    "pattern": "MISSING_FILTERS",
                    "description": "Add WHERE clause or partition filters",
                    "potential_savings_percent": 60,
                    "potential_savings_usd": estimated_cost * 0.6,
                    "implementation": "Add filters to reduce data scanning"
                })
                potential_savings += estimated_cost * 0.6
            
            if "ORDER BY" in sql_upper and "LIMIT" not in sql_upper:
                optimization_suggestions.append({
                    "pattern": "ORDER_WITHOUT_LIMIT",
                    "description": "Add LIMIT clause to ORDER BY",
                    "potential_savings_percent": 25,
                    "potential_savings_usd": estimated_cost * 0.25,
                    "implementation": "Use LIMIT to avoid sorting entire result set"
                })
                potential_savings += estimated_cost * 0.25
            
            join_count = sql_upper.count("JOIN")
            if join_count >= 3:
                optimization_suggestions.append({
                    "pattern": "COMPLEX_JOINS",
                    "description": f"Optimize {join_count} JOINs",
                    "potential_savings_percent": 30,
                    "potential_savings_usd": estimated_cost * 0.3,
                    "implementation": "Consider denormalization or materialized views"
                })
                potential_savings += estimated_cost * 0.3
        
        # Risk assessment
        risk_factors = []
        risk_score = 0
        
        if estimated_cost > 100:
            risk_factors.append("Very high cost (>$100)")
            risk_score += 4
        elif estimated_cost > 25:
            risk_factors.append("High cost (>$25)")
            risk_score += 3
        elif estimated_cost > 5:
            risk_factors.append("Medium cost (>$5)")
            risk_score += 2
        
        if tb_processed > 10:
            risk_factors.append("Large data volume (>10TB)")
            risk_score += 3
        elif tb_processed > 1:
            risk_factors.append("Moderate data volume (>1TB)")
            risk_score += 1
        
        if complexity_score > 0.7:
            risk_factors.append("High query complexity")
            risk_score += 2
        
        # Determine risk level
        if risk_score >= 8:
            risk_level = "CRITICAL"
        elif risk_score >= 5:
            risk_level = "HIGH"
        elif risk_score >= 3:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"
        
        # Execution recommendation
        if risk_level == "CRITICAL":
            execution_recommendation = "DO NOT EXECUTE - requires optimization and approval"
        elif risk_level == "HIGH":
            execution_recommendation = "High risk - strongly recommend optimization first"
        elif risk_level == "MEDIUM":
            execution_recommendation = "Proceed with caution - monitor execution"
        else:
            execution_recommendation = "Safe to execute - minimal risk"
        
        return json.dumps({
            "success": True,
            "query_analysis": {
                "query_hash": hash(sql.strip()) % 10000000,
                "query_length": len(sql),
                "estimated_cost_usd": round(estimated_cost, 4),
                "cost_tier": cost_tier,
                "bytes_to_process": bytes_processed,
                "tb_to_process": round(tb_processed, 4)
            },
            "complexity_analysis": {
                "complexity_score": round(complexity_score, 3),
                "complexity_factors": complexity_factors
            },
            "risk_assessment": {
                "risk_level": risk_level,
                "risk_score": risk_score,
                "risk_factors": risk_factors,
                "execution_recommendation": execution_recommendation
            },
            "optimization_suggestions": optimization_suggestions,
            "potential_total_savings": {
                "savings_usd": round(potential_savings, 2),
                "savings_percent": round((potential_savings / estimated_cost * 100), 1) if estimated_cost > 0 else 0,
                "optimized_cost_usd": round(max(0, estimated_cost - potential_savings), 2)
            },
            "generated_at": datetime.now().isoformat()
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "suggestion": "Check SQL syntax and table access permissions"
        })

@mcp.tool()
def create_cost_forecast(
    days_historical: int = 30,
    days_forecast: int = 30,
    growth_assumptions: str = "current_trend"  # current_trend | conservative | aggressive
) -> str:
    """
    Create cost forecast based on historical usage patterns.
    
    Args:
        days_historical: Days of historical data to analyze (7-90, default: 30)
        days_forecast: Days to forecast ahead (1-365, default: 30)
        growth_assumptions: Growth assumption model
    
    Returns:
        JSON string with cost forecast and budget recommendations
    """
    if not 7 <= days_historical <= 90:
        return json.dumps({"error": "Historical days must be between 7 and 90"})
    
    if not 1 <= days_forecast <= 365:
        return json.dumps({"error": "Forecast days must be between 1 and 365"})
    
    try:
        # Get historical daily costs
        query = f"""
        WITH daily_costs AS (
            SELECT 
                DATE(creation_time) as date,
                COUNT(*) as query_count,
                SUM(total_bytes_processed) / POW(10, 12) * 6.25 as daily_cost_usd,
                COUNT(DISTINCT user_email) as active_users,
                SUM(CASE WHEN user_email LIKE '%gserviceaccount.com' THEN 
                    total_bytes_processed / POW(10, 12) * 6.25 ELSE 0 END) as service_account_cost,
                SUM(CASE WHEN user_email NOT LIKE '%gserviceaccount.com' THEN 
                    total_bytes_processed / POW(10, 12) * 6.25 ELSE 0 END) as user_cost
            FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_historical} DAY)
                AND job_type = 'QUERY'
                AND state = 'DONE'
                AND error_result IS NULL
                AND total_bytes_processed IS NOT NULL
            GROUP BY DATE(creation_time)
        ),
        weekly_trends AS (
            SELECT 
                EXTRACT(WEEK FROM date) as week_num,
                AVG(daily_cost_usd) as avg_weekly_cost,
                AVG(query_count) as avg_weekly_queries,
                AVG(active_users) as avg_weekly_users
            FROM daily_costs
            GROUP BY EXTRACT(WEEK FROM date)
            ORDER BY week_num
        )
        SELECT 
            dc.*,
            wt.avg_weekly_cost,
            -- Calculate growth trends
            LAG(daily_cost_usd, 7) OVER (ORDER BY date) as cost_week_ago,
            LAG(daily_cost_usd, 1) OVER (ORDER BY date) as cost_day_ago
        FROM daily_costs dc
        LEFT JOIN weekly_trends wt ON EXTRACT(WEEK FROM dc.date) = wt.week_num
        ORDER BY date
        """
        
        results = list(bq_client.query(query))
        
        if not results:
            return json.dumps({
                "success": False,
                "error": "No historical data found for forecasting"
            })
        
        # Calculate trends and growth rates
        daily_costs = [row.daily_cost_usd for row in results]
        total_historical_cost = sum(daily_costs)
        avg_daily_cost = total_historical_cost / len(daily_costs)
        
        # Calculate growth rate
        recent_costs = daily_costs[-7:] if len(daily_costs) >= 7 else daily_costs
        early_costs = daily_costs[:7] if len(daily_costs) >= 14 else daily_costs[:len(daily_costs)//2]
        
        recent_avg = sum(recent_costs) / len(recent_costs)
        early_avg = sum(early_costs) / len(early_costs)
        
        if early_avg > 0:
            growth_rate = (recent_avg - early_avg) / early_avg
        else:
            growth_rate = 0
        
        # Apply growth assumptions
        if growth_assumptions == "conservative":
            adjusted_growth_rate = max(0, growth_rate * 0.5)  # 50% of observed growth
        elif growth_assumptions == "aggressive":
            adjusted_growth_rate = growth_rate * 1.5  # 150% of observed growth
        else:  # current_trend
            adjusted_growth_rate = growth_rate
        
        # Generate forecast
        forecast_data = []
        current_base_cost = recent_avg
        
        for day in range(1, days_forecast + 1):
            # Apply growth rate (compound daily growth)
            daily_growth_rate = adjusted_growth_rate / 30  # Convert monthly to daily
            projected_cost = current_base_cost * (1 + daily_growth_rate) ** day
            
            # Add some seasonality (higher on weekdays)
            forecast_date = datetime.now().date() + timedelta(days=day)
            weekday_multiplier = 1.2 if forecast_date.weekday() < 5 else 0.8
            projected_cost *= weekday_multiplier
            
            forecast_data.append({
                "date": forecast_date.isoformat(),
                "projected_cost_usd": round(projected_cost, 2),
                "confidence": "high" if day <= 7 else "medium" if day <= 30 else "low"
            })
        
        # Calculate forecast summary
        total_forecast_cost = sum(day["projected_cost_usd"] for day in forecast_data)
        avg_forecast_daily = total_forecast_cost / days_forecast
        
        # Generate budget recommendations
        budget_recommendations = []
        
        if adjusted_growth_rate > 0.1:  # >10% growth
            budget_recommendations.append({
                "priority": "HIGH",
                "recommendation": f"Cost growing at {adjusted_growth_rate*100:.1f}% - implement cost controls",
                "action": "Set up daily budget alerts and optimize expensive queries"
            })
        
        if avg_forecast_daily > avg_daily_cost * 1.5:
            budget_recommendations.append({
                "priority": "MEDIUM",
                "recommendation": f"Forecast shows 50%+ cost increase",
                "action": "Review query patterns and implement optimization strategies"
            })
        
        weekly_forecast_cost = sum(day["projected_cost_usd"] for day in forecast_data[:7])
        monthly_forecast_cost = sum(day["projected_cost_usd"] for day in forecast_data[:30])
        
        return json.dumps({
            "success": True,
            "historical_analysis": {
                "days_analyzed": len(results),
                "total_historical_cost": round(total_historical_cost, 2),
                "avg_daily_cost": round(avg_daily_cost, 2),
                "growth_rate_observed": round(growth_rate * 100, 2),
                "growth_rate_applied": round(adjusted_growth_rate * 100, 2)
            },
            "forecast_summary": {
                "forecast_period_days": days_forecast,
                "total_forecast_cost": round(total_forecast_cost, 2),
                "avg_daily_forecast": round(avg_forecast_daily, 2),
                "weekly_forecast": round(weekly_forecast_cost, 2),
                "monthly_forecast": round(monthly_forecast_cost, 2),
                "growth_assumptions": growth_assumptions
            },
            "forecast_data": forecast_data,
            "budget_recommendations": budget_recommendations,
            "cost_comparison": {
                "current_vs_forecast_daily": {
                    "current": round(avg_daily_cost, 2),
                    "forecast": round(avg_forecast_daily, 2),
                    "change_percent": round((avg_forecast_daily - avg_daily_cost) / avg_daily_cost * 100, 1) if avg_daily_cost > 0 else 0
                }
            },
            "generated_at": datetime.now().isoformat()
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        })

@mcp.tool()
def analyze_table_hotspots(
    days: int = 7,
    min_access_cost: float = 5.0
) -> str:
    """
    Analyze which tables are most expensive to access and suggest optimizations.
    
    Args:
        days: Number of days to analyze (1-30, default: 7)
        min_access_cost: Minimum cost threshold for table access analysis
    
    Returns:
        JSON string with table hotspot analysis and optimization recommendations
    """
    if not 1 <= days <= 30:
        return json.dumps({"error": "Days must be between 1 and 30"})
    
    try:
        query = f"""
        WITH table_access_patterns AS (
            SELECT 
                job_id,
                user_email,
                creation_time,
                query,
                total_bytes_processed / POW(10, 12) * 6.25 as cost_usd,
                total_bytes_processed / POW(10, 12) as tb_processed,
                TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) as duration_ms,
                -- Extract table references from query
                ARRAY(
                    SELECT TRIM(table_ref)
                    FROM UNNEST(REGEXP_EXTRACT_ALL(query, r'FROM\s+`([^`]+)`|JOIN\s+`([^`]+)`')) AS table_ref
                    WHERE table_ref IS NOT NULL AND table_ref != ''
                ) as referenced_tables
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
                table_name,
                COUNT(*) as access_count,
                COUNT(DISTINCT user_email) as unique_users,
                SUM(cost_usd) as total_access_cost,
                AVG(cost_usd) as avg_cost_per_access,
                MAX(cost_usd) as max_single_access_cost,
                SUM(tb_processed) as total_tb_processed,
                AVG(duration_ms) as avg_duration_ms,
                -- Detect access patterns
                COUNT(CASE WHEN UPPER(query) LIKE '%SELECT *%' THEN 1 END) as select_star_count,
                COUNT(CASE WHEN UPPER(query) NOT LIKE '%WHERE%' THEN 1 END) as unfiltered_access_count,
                COUNT(CASE WHEN UPPER(query) LIKE '%JOIN%' THEN 1 END) as join_access_count
            FROM table_access_patterns,
            UNNEST(referenced_tables) as table_name
            WHERE table_name IS NOT NULL
            GROUP BY table_name
        )
        SELECT 
            table_name,
            access_count,
            unique_users,
            total_access_cost,
            avg_cost_per_access,
            max_single_access_cost,
            total_tb_processed,
            avg_duration_ms,
            select_star_count,
            unfiltered_access_count,
            join_access_count,
            -- Calculate optimization scores
            CASE 
                WHEN select_star_count > access_count * 0.5 THEN 'CRITICAL'
                WHEN select_star_count > access_count * 0.2 THEN 'HIGH'
                ELSE 'MEDIUM'
            END as column_pruning_priority,
            CASE 
                WHEN unfiltered_access_count > access_count * 0.7 THEN 'CRITICAL'
                WHEN unfiltered_access_count > access_count * 0.3 THEN 'HIGH'
                ELSE 'MEDIUM'
            END as filtering_priority
        FROM table_costs
        WHERE total_access_cost >= {min_access_cost}
        ORDER BY total_access_cost DESC
        LIMIT 50
        """
        
        results = list(bq_client.query(query))
        
        table_hotspots = []
        total_hotspot_cost = 0
        optimization_recommendations = []
        
        for row in results:
            total_hotspot_cost += row.total_access_cost
            
            # Generate table-specific recommendations
            table_recommendations = []
            
            if row.select_star_count > 0:
                savings_potential = row.total_access_cost * 0.4 * (row.select_star_count / row.access_count)
                table_recommendations.append({
                    "type": "COLUMN_PRUNING",
                    "priority": row.column_pruning_priority,
                    "description": f"Replace SELECT * in {row.select_star_count} queries",
                    "potential_savings_usd": round(savings_potential, 2),
                    "implementation": f"CREATE VIEW with specific columns for {row.table_name}"
                })
            
            if row.unfiltered_access_count > 0:
                savings_potential = row.total_access_cost * 0.6 * (row.unfiltered_access_count / row.access_count)
                table_recommendations.append({
                    "type": "PARTITION_FILTERING", 
                    "priority": row.filtering_priority,
                    "description": f"Add filters to {row.unfiltered_access_count} unfiltered queries",
                    "potential_savings_usd": round(savings_potential, 2),
                    "implementation": f"Partition {row.table_name} by date/time and enforce filter usage"
                })
            
            if row.join_access_count > row.access_count * 0.5:
                table_recommendations.append({
                    "type": "DENORMALIZATION",
                    "priority": "MEDIUM",
                    "description": f"Frequently joined in {row.join_access_count} queries",
                    "potential_savings_usd": round(row.total_access_cost * 0.25, 2),
                    "implementation": f"Consider denormalizing {row.table_name} with frequently joined tables"
                })
            
            table_hotspots.append({
                "table_name": row.table_name,
                "access_metrics": {
                    "access_count": row.access_count,
                    "unique_users": row.unique_users,
                    "total_access_cost_usd": round(row.total_access_cost, 2),
                    "avg_cost_per_access_usd": round(row.avg_cost_per_access, 2),
                    "max_single_access_cost_usd": round(row.max_single_access_cost, 2),
                    "total_tb_processed": round(row.total_tb_processed, 2),
                    "avg_duration_ms": round(row.avg_duration_ms, 2)
                },
                "access_patterns": {
                    "select_star_usage": f"{row.select_star_count}/{row.access_count}",
                    "unfiltered_access": f"{row.unfiltered_access_count}/{row.access_count}",
                    "join_usage": f"{row.join_access_count}/{row.access_count}"
                },
                "optimization_recommendations": table_recommendations
            })
        
        return json.dumps({
            "success": True,
            "analysis_period": {
                "days": days,
                "min_access_cost": min_access_cost
            },
            "summary": {
                "tables_analyzed": len(results),
                "total_hotspot_cost_usd": round(total_hotspot_cost, 2),
                "avg_cost_per_table": round(total_hotspot_cost / len(results), 2) if results else 0
            },
            "table_hotspots": table_hotspots,
            "generated_at": datetime.now().isoformat()
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        })

@mcp.tool()
def generate_materialized_view_recommendations(
    days: int = 14,
    min_repetition_count: int = 3,
    min_cost_per_execution: float = 5.0
) -> str:
    """
    Analyze query patterns to recommend materialized views for cost optimization.
    
    Args:
        days: Number of days to analyze for repeated patterns (7-30, default: 14)
        min_repetition_count: Minimum times a pattern must repeat (2-10, default: 3)
        min_cost_per_execution: Minimum cost per execution to consider (default: 5.0)
    
    Returns:
        JSON string with materialized view recommendations
    """
    if not 7 <= days <= 30:
        return json.dumps({"error": "Days must be between 7 and 30"})
    
    if not 2 <= min_repetition_count <= 10:
        return json.dumps({"error": "Repetition count must be between 2 and 10"})
    
    try:
        query = f"""
        WITH query_patterns AS (
            SELECT 
                -- Normalize queries by removing literals and whitespace for pattern matching
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(UPPER(query), r"'[^']*'", "'LITERAL'"),
                        r'\\b\\d+\\b', 'NUMBER'
                    ),
                    r'\\s+', ' '
                ) as normalized_query,
                user_email,
                total_bytes_processed / POW(10, 12) * 6.25 as cost_usd,
                TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) as duration_ms,
                query as original_query,
                creation_time,
                destination_table.dataset_id as target_dataset,
                destination_table.table_id as target_table
            FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
                AND job_type = 'QUERY'
                AND state = 'DONE'
                AND error_result IS NULL
                AND total_bytes_processed IS NOT NULL
                AND total_bytes_processed / POW(10, 12) * 6.25 >= {min_cost_per_execution}
                AND query IS NOT NULL
                AND UPPER(query) LIKE '%SELECT%'
                AND UPPER(query) LIKE '%FROM%'
        ),
        repeated_patterns AS (
            SELECT 
                normalized_query,
                COUNT(*) as execution_count,
                COUNT(DISTINCT user_email) as unique_users,
                SUM(cost_usd) as total_cost,
                AVG(cost_usd) as avg_cost_per_execution,
                AVG(duration_ms) as avg_duration_ms,
                MAX(cost_usd) as max_cost_per_execution,
                STRING_AGG(DISTINCT user_email LIMIT 3) as sample_users,
                ANY_VALUE(original_query) as sample_query,
                -- Detect pattern characteristics
                CASE 
                    WHEN normalized_query LIKE '%GROUP BY%' THEN TRUE
                    ELSE FALSE
                END as has_aggregation,
                CASE 
                    WHEN normalized_query LIKE '%JOIN%' THEN TRUE
                    ELSE FALSE
                END as has_joins,
                CASE 
                    WHEN normalized_query LIKE '%ORDER BY%' THEN TRUE
                    ELSE FALSE
                END as has_ordering,
                CASE 
                    WHEN normalized_query LIKE '%WINDOW%' OR normalized_query LIKE '%OVER(%' THEN TRUE
                    ELSE FALSE
                END as has_window_functions
            FROM query_patterns
            GROUP BY normalized_query
            HAVING COUNT(*) >= {min_repetition_count}
                AND SUM(cost_usd) >= {min_cost_per_execution * min_repetition_count}
        )
        SELECT *
        FROM repeated_patterns
        ORDER BY total_cost DESC
        LIMIT 20
        """
        
        results = list(bq_client.query(query))
        
        materialized_view_recommendations = []
        total_potential_savings = 0
        
        for row in results:
            # Calculate potential savings (materialized views can save 70-90% of cost for repeated queries)
            baseline_savings = row.total_cost * 0.8  # 80% baseline savings
            
            # Adjust savings based on pattern characteristics
            if row.has_aggregation:
                savings_multiplier = 1.1  # Aggregations benefit more
            elif row.has_joins:
                savings_multiplier = 1.05  # Joins benefit moderately
            else:
                savings_multiplier = 1.0
            
            potential_savings = baseline_savings * savings_multiplier
            total_potential_savings += potential_savings
            
            # Generate refresh strategy recommendation
            if row.execution_count > days * 2:  # More than 2x per day
                refresh_strategy = "AUTOMATIC_HOURLY"
                refresh_cost_estimate = potential_savings * 0.1  # 10% of savings for refresh
            elif row.execution_count > days:  # Daily or more
                refresh_strategy = "AUTOMATIC_DAILY"
                refresh_cost_estimate = potential_savings * 0.05  # 5% of savings for refresh
            else:
                refresh_strategy = "MANUAL_WEEKLY"
                refresh_cost_estimate = potential_savings * 0.02  # 2% of savings for refresh
            
            net_savings = potential_savings - refresh_cost_estimate
            
            # Generate materialized view SQL
            sample_query = row.sample_query
            mv_name = f"mv_{hash(row.normalized_query) % 100000}"
            
            # Extract main tables from query for partitioning suggestion
            table_matches = re.findall(r'FROM\s+`([^`]+)`', sample_query.upper())
            main_table = table_matches[0] if table_matches else "unknown_table"
            
            mv_sql = f"""
CREATE MATERIALIZED VIEW `{project_id}.optimized_views.{mv_name}`
PARTITION BY DATE(created_date)  -- Adjust partition column as needed
CLUSTER BY primary_key_column    -- Adjust cluster column as needed
OPTIONS (
  enable_refresh = true,
  refresh_interval_minutes = {'60' if refresh_strategy == 'AUTOMATIC_HOURLY' else '1440'}
)
AS
{sample_query}
            """.strip()
            
            materialized_view_recommendations.append({
                "pattern_id": mv_name,
                "pattern_summary": {
                    "execution_count": row.execution_count,
                    "unique_users": row.unique_users,
                    "total_cost_usd": round(row.total_cost, 2),
                    "avg_cost_per_execution_usd": round(row.avg_cost_per_execution, 2),
                    "avg_duration_ms": round(row.avg_duration_ms, 2)
                },
                "pattern_characteristics": {
                    "has_aggregation": row.has_aggregation,
                    "has_joins": row.has_joins,
                    "has_ordering": row.has_ordering,
                    "has_window_functions": row.has_window_functions
                },
                "optimization_potential": {
                    "potential_savings_usd": round(potential_savings, 2),
                    "refresh_cost_estimate_usd": round(refresh_cost_estimate, 2),
                    "net_savings_usd": round(net_savings, 2),
                    "savings_percentage": round(net_savings / row.total_cost * 100, 1),
                    "roi_multiple": round(net_savings / refresh_cost_estimate, 1) if refresh_cost_estimate > 0 else float('inf')
                },
                "implementation": {
                    "recommended_name": mv_name,
                    "refresh_strategy": refresh_strategy,
                    "main_source_table": main_table,
                    "sample_users": row.sample_users,
                    "materialized_view_sql": mv_sql
                },
                "sample_query": sample_query[:1000] + "..." if len(sample_query) > 1000 else sample_query
            })
        
        # Sort by net savings
        materialized_view_recommendations.sort(key=lambda x: x["optimization_potential"]["net_savings_usd"], reverse=True)
        
        return json.dumps({
            "success": True,
            "analysis_period": {
                "days": days,
                "min_repetition_count": min_repetition_count,
                "min_cost_per_execution": min_cost_per_execution
            },
            "summary": {
                "patterns_analyzed": len(results),
                "total_potential_savings_usd": round(total_potential_savings, 2),
                "recommended_views": len(materialized_view_recommendations),
                "top_opportunity_savings": round(materialized_view_recommendations[0]["optimization_potential"]["net_savings_usd"], 2) if materialized_view_recommendations else 0
            },
            "materialized_view_recommendations": materialized_view_recommendations,
            "implementation_priority": [
                {
                    "priority": "IMMEDIATE",
                    "patterns": [mv["pattern_id"] for mv in materialized_view_recommendations[:3]],
                    "total_savings": sum(mv["optimization_potential"]["net_savings_usd"] for mv in materialized_view_recommendations[:3])
                },
                {
                    "priority": "HIGH",
                    "patterns": [mv["pattern_id"] for mv in materialized_view_recommendations[3:8]],
                    "total_savings": sum(mv["optimization_potential"]["net_savings_usd"] for mv in materialized_view_recommendations[3:8])
                }
            ],
            "generated_at": datetime.now().isoformat()
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        })

@mcp.tool()
def create_optimization_report(
    days: int = 7,
    report_type: str = "executive"  # executive | technical | stakeholder
) -> str:
    """
    Generate comprehensive optimization report for different audiences.
    
    Args:
        days: Number of days to analyze (1-30, default: 7)
        report_type: Type of report to generate (executive, technical, stakeholder)
    
    Returns:
        JSON string with formatted optimization report
    """
    if not 1 <= days <= 30:
        return json.dumps({"error": "Days must be between 1 and 30"})
    
    if report_type not in ["executive", "technical", "stakeholder"]:
        return json.dumps({"error": "Report type must be executive, technical, or stakeholder"})
    
    try:
        # Get comprehensive data for report
        summary_query = f"""
        WITH cost_analysis AS (
            SELECT 
                COUNT(*) as total_queries,
                COUNT(DISTINCT user_email) as unique_users,
                SUM(total_bytes_processed / POW(10, 12) * 6.25) as total_cost_usd,
                AVG(total_bytes_processed / POW(10, 12) * 6.25) as avg_cost_per_query,
                MAX(total_bytes_processed / POW(10, 12) * 6.25) as max_query_cost,
                SUM(total_bytes_processed) / POW(10, 12) as total_tb_processed,
                COUNT(CASE WHEN cache_hit THEN 1 END) as cache_hits,
                COUNT(CASE WHEN user_email LIKE '%gserviceaccount.com' THEN 1 END) as service_account_queries,
                SUM(CASE WHEN user_email LIKE '%gserviceaccount.com' THEN 
                    total_bytes_processed / POW(10, 12) * 6.25 ELSE 0 END) as service_account_cost,
                COUNT(CASE WHEN total_bytes_processed / POW(10, 12) * 6.25 > 50 THEN 1 END) as critical_cost_queries,
                COUNT(CASE WHEN total_bytes_processed / POW(10, 12) * 6.25 > 10 THEN 1 END) as high_cost_queries,
                COUNT(CASE WHEN query LIKE '%SELECT *%' THEN 1 END) as select_star_queries
            FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
                AND job_type = 'QUERY'
                AND state = 'DONE'
                AND error_result IS NULL
                AND total_bytes_processed IS NOT NULL
        ),
        daily_trends AS (
            SELECT 
                DATE(creation_time) as date,
                SUM(total_bytes_processed / POW(10, 12) * 6.25) as daily_cost
            FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
                AND job_type = 'QUERY'
                AND state = 'DONE'
                AND error_result IS NULL
                AND total_bytes_processed IS NOT NULL
            GROUP BY DATE(creation_time)
            ORDER BY date
        )
        SELECT 
            ca.*,
            ARRAY_AGG(dt.daily_cost ORDER BY dt.date) as daily_cost_trend
        FROM cost_analysis ca
        CROSS JOIN daily_trends dt
        GROUP BY ca.total_queries, ca.unique_users, ca.total_cost_usd, ca.avg_cost_per_query, 
                 ca.max_query_cost, ca.total_tb_processed, ca.cache_hits, ca.service_account_queries,
                 ca.service_account_cost, ca.critical_cost_queries, ca.high_cost_queries, ca.select_star_queries
        """
        
        result = list(bq_client.query(summary_query))[0]
        
        # Calculate key metrics
        cache_hit_rate = (result.cache_hits / result.total_queries * 100) if result.total_queries > 0 else 0
        service_account_percentage = (result.service_account_cost / result.total_cost_usd * 100) if result.total_cost_usd > 0 else 0
        high_cost_query_percentage = (result.high_cost_queries / result.total_queries * 100) if result.total_queries > 0 else 0
        
        # Calculate optimization opportunities
        select_star_savings = result.total_cost_usd * 0.4 * (result.select_star_queries / result.total_queries) if result.total_queries > 0 else 0
        cache_optimization_savings = result.total_cost_usd * 0.7 * (1 - cache_hit_rate / 100)
        partition_filter_savings = result.total_cost_usd * 0.5  # Estimate based on typical patterns
        
        total_optimization_potential = select_star_savings + cache_optimization_savings + partition_filter_savings
        
        # Generate report based on type
        if report_type == "executive":
            report_content = {
                "executive_summary": {
                    "period": f"Last {days} days",
                    "total_spend_usd": round(result.total_cost_usd, 2),
                    "daily_average_usd": round(result.total_cost_usd / days, 2),
                    "monthly_projection_usd": round(result.total_cost_usd / days * 30, 2),
                    "optimization_opportunity_usd": round(total_optimization_potential, 2),
                    "potential_monthly_savings_usd": round(total_optimization_potential / days * 30, 2)
                },
                "key_findings": [
                    f"Service accounts drive {service_account_percentage:.1f}% of total costs",
                    f"Cache hit rate is only {cache_hit_rate:.1f}% - major opportunity",
                    f"{high_cost_query_percentage:.1f}% of queries exceed $10 cost threshold",
                    f"Potential monthly savings of ${total_optimization_potential / days * 30:.0f} identified"
                ],
                "immediate_actions": [
                    {
                        "action": "Enable query result caching",
                        "impact": f"${cache_optimization_savings:.0f} monthly savings",
                        "effort": "Low",
                        "timeline": "1 week"
                    },
                    {
                        "action": "Implement partition filtering on ETL jobs",
                        "impact": f"${partition_filter_savings * 0.6:.0f} monthly savings",
                        "effort": "Medium", 
                        "timeline": "2-3 weeks"
                    },
                    {
                        "action": "Replace SELECT * with specific columns",
                        "impact": f"${select_star_savings:.0f} monthly savings",
                        "effort": "Medium",
                        "timeline": "1 month"
                    }
                ],
                "cost_trend": list(result.daily_cost_trend)
            }
        
        elif report_type == "technical":
            report_content = {
                "technical_analysis": {
                    "query_statistics": {
                        "total_queries": result.total_queries,
                        "unique_users": result.unique_users,
                        "total_tb_processed": round(result.total_tb_processed, 2),
                        "avg_cost_per_query": round(result.avg_cost_per_query, 4),
                        "max_query_cost": round(result.max_query_cost, 2)
                    },
                    "performance_metrics": {
                        "cache_hit_rate_percent": round(cache_hit_rate, 2),
                        "high_cost_queries": result.high_cost_queries,
                        "critical_cost_queries": result.critical_cost_queries,
                        "select_star_usage": result.select_star_queries
                    }
                },
                "optimization_patterns": [
                    {
                        "pattern": "Column Pruning (SELECT * replacement)",
                        "occurrences": result.select_star_queries,
                        "estimated_savings_usd": round(select_star_savings, 2),
                        "implementation": "Replace SELECT * with explicit column lists"
                    },
                    {
                        "pattern": "Query Result Caching",
                        "current_hit_rate": f"{cache_hit_rate:.1f}%",
                        "estimated_savings_usd": round(cache_optimization_savings, 2),
                        "implementation": "Enable use_query_cache=true for deterministic queries"
                    },
                    {
                        "pattern": "Partition Filtering",
                        "estimated_queries_affected": round(result.total_queries * 0.6),
                        "estimated_savings_usd": round(partition_filter_savings, 2),
                        "implementation": "Add WHERE _PARTITIONDATE >= date_filter clauses"
                    }
                ],
                "code_examples": {
                    "partition_filtering": """
-- Before (expensive full scan)
SELECT * FROM `project.dataset.large_table`
WHERE event_date >= '2024-01-01'

-- After (partition-filtered)
SELECT col1, col2, col3 FROM `project.dataset.large_table`
WHERE _PARTITIONDATE >= '2024-01-01'
  AND event_date >= '2024-01-01'
                    """,
                    "materialized_view": """
-- Create materialized view for expensive aggregation
CREATE MATERIALIZED VIEW `project.dataset.daily_sales_summary`
PARTITION BY DATE(sale_date)
AS
SELECT 
  DATE(sale_date) as sale_date,
  store_id,
  SUM(amount) as total_sales,
  COUNT(*) as transaction_count
FROM `project.dataset.sales_raw`
GROUP BY DATE(sale_date), store_id
                    """
                }
            }
        
        else:  # stakeholder
            report_content = {
                "stakeholder_summary": {
                    "business_impact": {
                        "current_monthly_spend": round(result.total_cost_usd / days * 30, 2),
                        "optimization_opportunity": round(total_optimization_potential / days * 30, 2),
                        "percentage_savings_possible": round(total_optimization_potential / result.total_cost_usd * 100, 1),
                        "payback_period": "Immediate - most optimizations have instant effect"
                    },
                    "cost_drivers": [
                        {
                            "driver": "Service Account ETL Jobs",
                            "percentage": round(service_account_percentage, 1),
                            "monthly_cost": round(result.service_account_cost / days * 30, 2),
                            "optimization_potential": "High - can reduce by 60% with incremental processing"
                        },
                        {
                            "driver": "Ad-hoc User Queries", 
                            "percentage": round(100 - service_account_percentage, 1),
                            "monthly_cost": round((result.total_cost_usd - result.service_account_cost) / days * 30, 2),
                            "optimization_potential": "Medium - training and query governance needed"
                        }
                    ],
                    "recommendations_by_priority": [
                        {
                            "priority": "Immediate (Week 1)",
                            "actions": ["Enable query caching", "Add cost alerts"],
                            "investment_required": "Minimal - configuration changes only",
                            "expected_savings": f"${cache_optimization_savings / days * 30 * 0.5:.0f}/month"
                        },
                        {
                            "priority": "Short-term (Month 1)",
                            "actions": ["Optimize ETL jobs", "Implement partition filtering"],
                            "investment_required": "1-2 engineering weeks",
                            "expected_savings": f"${partition_filter_savings / days * 30 * 0.7:.0f}/month"
                        },
                        {
                            "priority": "Medium-term (Months 2-3)",
                            "actions": ["Deploy materialized views", "Restructure data architecture"],
                            "investment_required": "3-4 engineering weeks",
                            "expected_savings": f"${select_star_savings / days * 30:.0f}/month"
                        }
                    ]
                }
            }
        
        return json.dumps({
            "success": True,
            "report_metadata": {
                "report_type": report_type,
                "analysis_period_days": days,
                "generated_at": datetime.now().isoformat(),
                "project_id": project_id
            },
            "report_content": report_content
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        })

def _generate_optimization_recommendations(category: str, data: Dict, categorize_by: str) -> List[Dict]:
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