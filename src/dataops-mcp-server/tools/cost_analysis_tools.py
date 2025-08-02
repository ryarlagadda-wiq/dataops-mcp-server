#!/usr/bin/env python3

"""
Query Cost Analysis Tool
Predicts BigQuery query costs before execution and provides optimization suggestions.
"""

import json
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)

@dataclass
class QueryCostAnalysis:
    """Query cost analysis result"""
    estimated_cost_usd: float
    bytes_to_process: int
    cost_tier: str
    optimization_suggestions: List[str]
    risk_assessment: Dict[str, Any]
    performance_predictions: Dict[str, Any]
    query_complexity_score: float

class AnalyzeQueryCostTool:
    """Tool for analyzing individual query costs before execution"""
    
    def __init__(self, project_id: str, region: str = "us-central1"):
        self.project_id = project_id
        self.region = region
        self.bq_client = bigquery.Client(project=project_id)
        
        # Cost tier thresholds
        self.cost_tiers = {
            "LOW": (0, 1.0),         # $0 - $1
            "MEDIUM": (1.0, 10.0),   # $1 - $10
            "HIGH": (10.0, 50.0),    # $10 - $50
            "CRITICAL": (50.0, float('inf'))  # $50+
        }
        
        # BigQuery pricing
        self.price_per_tb = 6.25
        
        # Optimization patterns
        self.optimization_patterns = {
            "select_star": {
                "pattern": r"SELECT\s+\*",
                "suggestion": "Replace SELECT * with specific columns to reduce data processing",
                "potential_savings": 0.3
            },
            "no_partition_filter": {
                "keywords": ["_PARTITIONTIME", "_PARTITIONDATE"],
                "suggestion": "Add partition filters to reduce data scanning",
                "potential_savings": 0.6
            },
            "no_limit_with_order": {
                "pattern": r"ORDER\s+BY.*(?!.*LIMIT)",
                "suggestion": "Add LIMIT clause to prevent sorting large result sets",
                "potential_savings": 0.2
            },
            "no_where_clause": {
                "pattern": r"FROM\s+`[^`]+`\s*(?!.*WHERE)",
                "suggestion": "Add WHERE clause to filter data and reduce processing",
                "potential_savings": 0.4
            },
            "complex_joins": {
                "threshold": 3,
                "suggestion": "Review JOIN order and consider denormalizing frequently joined tables",
                "potential_savings": 0.25
            },
            "unnecessary_distinct": {
                "pattern": r"SELECT\s+DISTINCT.*COUNT\(",
                "suggestion": "Remove unnecessary DISTINCT when using COUNT aggregations",
                "potential_savings": 0.15
            },
            "subquery_in_where": {
                "pattern": r"WHERE.*IN\s*\(\s*SELECT",
                "suggestion": "Convert IN subqueries to JOINs for better performance",
                "potential_savings": 0.3
            }
        }
    
    async def execute(
        self,
        sql: str,
        include_optimization: bool = True,
        optimization_model: str = "pattern_based",
        create_pr_if_savings: bool = False
    ) -> str:
        """
        Analyze the cost impact of a SQL query before execution.
        
        Args:
            sql: SQL query to analyze
            include_optimization: Include optimization suggestions
            optimization_model: Optimization method ("pattern_based", "ai_powered")
            create_pr_if_savings: Auto-create PR if significant savings found
            
        Returns:
            JSON string with detailed query cost analysis
        """
        try:
            logger.info("Analyzing query cost with dry-run execution")
            
            if not sql or not sql.strip():
                raise ValueError("SQL query cannot be empty")
            
            # Perform BigQuery dry-run analysis
            dry_run_result = await self._perform_dry_run_analysis(sql)
            
            # Calculate cost estimates
            cost_analysis = self._calculate_cost_estimates(dry_run_result)
            
            # Analyze query complexity
            complexity_score = self._calculate_complexity_score(sql)
            
            # Generate optimization suggestions if requested
            optimization_suggestions = []
            if include_optimization:
                optimization_suggestions = await self._generate_optimization_suggestions(
                    sql, dry_run_result, optimization_model
                )
            
            # Perform risk assessment
            risk_assessment = self._assess_execution_risk(sql, cost_analysis, dry_run_result)
            
            # Predict performance characteristics
            performance_predictions = self._predict_performance(sql, dry_run_result)
            
            # Create comprehensive analysis
            analysis = QueryCostAnalysis(
                estimated_cost_usd=cost_analysis["estimated_cost"],
                bytes_to_process=dry_run_result["bytes_processed"],
                cost_tier=cost_analysis["cost_tier"],
                optimization_suggestions=optimization_suggestions,
                risk_assessment=risk_assessment,
                performance_predictions=performance_predictions,
                query_complexity_score=complexity_score
            )
            
            # Build result
            result = {
                "success": True,
                "analysis": asdict(analysis),
                "query_metadata": {
                    "query_length": len(sql),
                    "query_hash": hash(sql.strip()) % 10000000,  # Simple hash for identification
                    "contains_dml": self._is_dml_query(sql),
                    "estimated_execution_time": performance_predictions.get("estimated_duration_seconds", 0)
                },
                "cost_breakdown": {
                    "data_processing_cost": cost_analysis["estimated_cost"],
                    "storage_cost_note": "Storage costs are separate and depend on result table size",
                    "pricing_model": "on_demand",
                    "price_per_tb": self.price_per_tb
                },
                "recommendations": self._generate_execution_recommendations(analysis),
                "metadata": {
                    "analyzed_at": datetime.now().isoformat(),
                    "project_id": self.project_id,
                    "optimization_model": optimization_model if include_optimization else None
                }
            }
            
            logger.info(f"Query analysis completed: ${cost_analysis['estimated_cost']:.2f} estimated cost, {cost_analysis['cost_tier']} tier")
            
            # Auto-create PR if significant savings and requested
            if create_pr_if_savings and optimization_suggestions:
                potential_savings = sum(
                    self.optimization_patterns.get(pattern, {}).get("potential_savings", 0) * cost_analysis["estimated_cost"]
                    for pattern in optimization_suggestions
                )
                if potential_savings > 25.0:  # $25+ savings threshold
                    result["pr_created"] = await self._create_optimization_pr(sql, analysis)
            
            return json.dumps(result, indent=2)
            
        except Exception as e:
            logger.error(f"Query cost analysis failed: {e}")
            return json.dumps({
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "suggestion": "Verify SQL syntax and table access permissions"
            })
    
    async def _perform_dry_run_analysis(self, sql: str) -> Dict[str, Any]:
        """Perform BigQuery dry-run to get cost estimates"""
        
        try:
            # Configure dry-run job
            job_config = bigquery.QueryJobConfig(
                dry_run=True,
                use_query_cache=False
            )
            
            # Execute dry-run
            job = self.bq_client.query(sql, job_config=job_config)
            
            # Extract results
            bytes_processed = job.total_bytes_processed or 0
            
            # Get schema information
            schema = []
            if job.schema:
                schema = [{"name": field.name, "type": field.field_type} for field in job.schema]
            
            return {
                "bytes_processed": bytes_processed,
                "schema": schema,
                "job_statistics": {
                    "creation_time": datetime.now().isoformat(),
                    "total_bytes_processed": bytes_processed
                }
            }
            
        except Exception as e:
            logger.error(f"Dry-run analysis failed: {e}")
            raise GoogleCloudError(f"Failed to analyze query: {str(e)}")
    
    def _calculate_cost_estimates(self, dry_run_result: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate cost estimates from dry-run results"""
        
        bytes_processed = dry_run_result["bytes_processed"]
        tb_processed = bytes_processed / (1024 ** 4)  # Convert to TB
        estimated_cost = tb_processed * self.price_per_tb
        
        # Determine cost tier
        cost_tier = "LOW"
        for tier, (min_cost, max_cost) in self.cost_tiers.items():
            if min_cost <= estimated_cost < max_cost:
                cost_tier = tier
                break
        
        return {
            "estimated_cost": estimated_cost,
            "cost_tier": cost_tier,
            "tb_processed": tb_processed,
            "gb_processed": tb_processed * 1024
        }
    
    def _calculate_complexity_score(self, sql: str) -> float:
        """Calculate query complexity score (0-1)"""
        
        sql_upper = sql.upper()
        
        # Complexity factors
        factors = {
            "length": min(len(sql) / 5000, 0.2),  # Normalize by 5k chars, max 0.2
            "joins": min(sql_upper.count("JOIN") * 0.1, 0.3),
            "subqueries": min((sql_upper.count("SELECT") - 1) * 0.05, 0.2),  # Exclude main SELECT
            "window_functions": min(sql_upper.count("OVER(") * 0.15, 0.2),
            "case_statements": min(sql_upper.count("CASE") * 0.05, 0.1),
            "unions": min(sql_upper.count("UNION") * 0.1, 0.1),
            "aggregations": min(len(re.findall(r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\(', sql_upper)) * 0.03, 0.1)
        }
        
        complexity_score = sum(factors.values())
        return min(complexity_score, 1.0)
    
    async def _generate_optimization_suggestions(
        self,
        sql: str,
        dry_run_result: Dict[str, Any],
        optimization_model: str
    ) -> List[str]:
        """Generate optimization suggestions based on query analysis"""
        
        suggestions = []
        sql_upper = sql.upper()
        
        # Pattern-based optimization suggestions
        for pattern_name, pattern_config in self.optimization_patterns.items():
            
            if pattern_name == "select_star":
                if re.search(pattern_config["pattern"], sql_upper):
                    suggestions.append(pattern_config["suggestion"])
            
            elif pattern_name == "no_partition_filter":
                if not any(keyword in sql_upper for keyword in pattern_config["keywords"]):
                    suggestions.append(pattern_config["suggestion"])
            
            elif pattern_name == "no_limit_with_order":
                if re.search(pattern_config["pattern"], sql_upper, re.DOTALL):
                    suggestions.append(pattern_config["suggestion"])
            
            elif pattern_name == "no_where_clause":
                if re.search(pattern_config["pattern"], sql_upper, re.DOTALL):
                    suggestions.append(pattern_config["suggestion"])
            
            elif pattern_name == "complex_joins":
                join_count = sql_upper.count("JOIN")
                if join_count >= pattern_config["threshold"]:
                    suggestions.append(pattern_config["suggestion"])
            
            elif pattern_name == "unnecessary_distinct":
                if re.search(pattern_config["pattern"], sql_upper):
                    suggestions.append(pattern_config["suggestion"])
            
            elif pattern_name == "subquery_in_where":
                if re.search(pattern_config["pattern"], sql_upper, re.DOTALL):
                    suggestions.append(pattern_config["suggestion"])
        
        # Data volume based suggestions
        bytes_processed = dry_run_result["bytes_processed"]
        gb_processed = bytes_processed / (1024 ** 3)
        
        if gb_processed > 100:  # > 100GB
            suggestions.append("Consider creating materialized views for frequently queried large datasets")
            suggestions.append("Implement incremental processing to avoid full table scans")
        
        if gb_processed > 10 and "LIMIT" not in sql_upper:  # > 10GB without LIMIT
            suggestions.append("Add LIMIT clause when exploring data to prevent processing large result sets")
        
        # Schema-based suggestions
        if dry_run_result.get("schema"):
            column_count = len(dry_run_result["schema"])
            if column_count > 20 and "SELECT *" in sql_upper:
                suggestions.append(f"Query selects {column_count} columns - specify only needed columns for better performance")
        
        # Remove duplicates and return
        return list(set(suggestions))
    
    def _assess_execution_risk(
        self,
        sql: str,
        cost_analysis: Dict[str, Any],
        dry_run_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Assess risks associated with executing the query"""
        
        risk_factors = []
        risk_score = 0
        
        # Cost-based risks
        estimated_cost = cost_analysis["estimated_cost"]
        if estimated_cost > 100:
            risk_factors.append("Very high cost (>$100)")
            risk_score += 4
        elif estimated_cost > 25:
            risk_factors.append("High cost (>$25)")
            risk_score += 3
        elif estimated_cost > 5:
            risk_factors.append("Medium cost (>$5)")
            risk_score += 2
        
        # Data volume risks
        gb_processed = dry_run_result["bytes_processed"] / (1024 ** 3)
        if gb_processed > 1000:  # > 1TB
            risk_factors.append("Extremely large data volume (>1TB)")
            risk_score += 4
        elif gb_processed > 100:  # > 100GB
            risk_factors.append("Large data volume (>100GB)")
            risk_score += 3
        elif gb_processed > 10:  # > 10GB
            risk_factors.append("Moderate data volume (>10GB)")
            risk_score += 1
        
        # Query pattern risks
        sql_upper = sql.upper()
        
        # SELECT * on large datasets
        if "SELECT *" in sql_upper and gb_processed > 1:
            risk_factors.append("SELECT * on large dataset")
            risk_score += 2
        
        # Many JOINs
        join_count = sql_upper.count("JOIN")
        if join_count > 5:
            risk_factors.append(f"Complex query with {join_count} JOINs")
            risk_score += 3
        elif join_count > 2:
            risk_factors.append(f"Multiple JOINs ({join_count}) may impact performance")
            risk_score += 1
        
        # No filters on large scans
        if "WHERE" not in sql_upper and gb_processed > 10:
            risk_factors.append("Full table scan without filters")
            risk_score += 3
        
        # ORDER BY without LIMIT
        if "ORDER BY" in sql_upper and "LIMIT" not in sql_upper and gb_processed > 1:
            risk_factors.append("ORDER BY without LIMIT on large dataset")
            risk_score += 2
        
        # Determine overall risk level
        if risk_score >= 8:
            risk_level = "CRITICAL"
        elif risk_score >= 5:
            risk_level = "HIGH"
        elif risk_score >= 3:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"
        
        # Generate mitigation recommendations
        mitigation_steps = self._generate_risk_mitigation(risk_level, risk_factors)
        
        return {
            "risk_level": risk_level,
            "risk_score": risk_score,
            "risk_factors": risk_factors,
            "mitigation_steps": mitigation_steps,
            "execution_recommendation": self._get_execution_recommendation(risk_level)
        }
    
    def _predict_performance(self, sql: str, dry_run_result: Dict[str, Any]) -> Dict[str, Any]:
        """Predict query performance characteristics"""
        
        bytes_processed = dry_run_result["bytes_processed"]
        gb_processed = bytes_processed / (1024 ** 3)
        
        # Base performance estimates (rough approximations)
        base_seconds_per_gb = 1.5
        sql_upper = sql.upper()
        
        # Complexity multipliers
        complexity_factor = 1.0
        
        # JOIN complexity
        join_count = sql_upper.count("JOIN")
        if join_count > 0:
            complexity_factor *= (1 + join_count * 0.3)
        
        # Aggregation complexity
        if "GROUP BY" in sql_upper:
            complexity_factor *= 1.4
        
        # Sorting complexity
        if "ORDER BY" in sql_upper:
            complexity_factor *= 1.2
        
        # Window function complexity
        window_count = sql_upper.count("OVER(")
        if window_count > 0:
            complexity_factor *= (1 + window_count * 0.5)
        
        # Calculate estimates
        estimated_duration = gb_processed * base_seconds_per_gb * complexity_factor
        
        # Slot utilization estimate (BigQuery's compute units)
        # More complex queries use more slots
        estimated_slots = min(2000, max(1, gb_processed * 5 * complexity_factor))
        
        # Performance tier
        if estimated_duration < 10:
            performance_tier = "FAST"
        elif estimated_duration < 60:
            performance_tier = "MEDIUM"
        elif estimated_duration < 300:
            performance_tier = "SLOW"
        else:
            performance_tier = "VERY_SLOW"
        
        return {
            "estimated_duration_seconds": estimated_duration,
            "estimated_slots_used": int(estimated_slots),
            "performance_tier": performance_tier,
            "complexity_factor": complexity_factor,
            "data_volume_gb": gb_processed,
            "performance_notes": self._get_performance_notes(performance_tier, estimated_duration)
        }
    
    def _generate_risk_mitigation(self, risk_level: str, risk_factors: List[str]) -> List[str]:
        """Generate risk mitigation recommendations"""
        
        mitigation_steps = []
        
        if risk_level in ["HIGH", "CRITICAL"]:
            mitigation_steps.extend([
                "Test query with LIMIT 1000 first to validate results",
                "Run during off-peak hours to minimize resource contention",
                "Monitor query progress and be prepared to cancel if needed",
                "Consider breaking down into smaller, incremental queries"
            ])
        
        if "Very high cost" in str(risk_factors):
            mitigation_steps.extend([
                "Get approval from data team lead before execution",
                "Implement query timeout to prevent runaway costs",
                "Document business justification for high-cost query"
            ])
        
        if "Extremely large data volume" in str(risk_factors):
            mitigation_steps.extend([
                "Verify that full dataset processing is actually required",
                "Consider sampling approach for analysis or testing",
                "Check if data can be pre-aggregated or filtered upstream"
            ])
        
        if "Complex query" in str(risk_factors) or "Many JOINs" in str(risk_factors):
            mitigation_steps.extend([
                "Review query execution plan for optimization opportunities",
                "Consider creating intermediate tables for complex JOINs",
                "Test individual components of complex query separately"
            ])
        
        return mitigation_steps
    
    def _get_execution_recommendation(self, risk_level: str) -> str:
        """Get overall execution recommendation based on risk"""
        
        recommendations = {
            "LOW": "Safe to execute - minimal cost and performance risk",
            "MEDIUM": "Proceed with caution - monitor execution and consider optimizations",
            "HIGH": "High risk - strongly recommend optimization before execution",
            "CRITICAL": "DO NOT EXECUTE - requires immediate optimization and approval"
        }
        
        return recommendations.get(risk_level, "Review required")
    
    def _get_performance_notes(self, performance_tier: str, duration: float) -> List[str]:
        """Get performance-related notes and recommendations"""
        
        notes = []
        
        if performance_tier == "FAST":
            notes.append("Query should execute quickly with minimal resource usage")
        elif performance_tier == "MEDIUM":
            notes.append("Query may take 1-2 minutes to complete")
        elif performance_tier == "SLOW":
            notes.append("Query may take several minutes - consider optimization")
        elif performance_tier == "VERY_SLOW":
            notes.extend([
                "Query may take 5+ minutes to complete",
                "Consider breaking into smaller operations",
                "Review optimization opportunities before execution"
            ])
        
        if duration > 300:  # > 5 minutes
            notes.append("Long-running query - ensure business justification")
        
        return notes
    
    def _generate_execution_recommendations(self, analysis: QueryCostAnalysis) -> List[str]:
        """Generate overall execution recommendations"""
        
        recommendations = []
        
        # Cost-based recommendations
        if analysis.cost_tier in ["HIGH", "CRITICAL"]:
            recommendations.append(f"Consider optimization before execution - estimated cost: ${analysis.estimated_cost_usd:.2f}")
        
        # Optimization recommendations
        if analysis.optimization_suggestions:
            recommendations.append(f"Found {len(analysis.optimization_suggestions)} optimization opportunities")
            recommendations.append("Review optimization suggestions to reduce costs")
        
        # Risk-based recommendations
        if analysis.risk_assessment["risk_level"] in ["HIGH", "CRITICAL"]:
            recommendations.append("High execution risk detected - review mitigation steps")
        
        # Performance recommendations
        performance_tier = analysis.performance_predictions.get("performance_tier", "UNKNOWN")
        if performance_tier in ["SLOW", "VERY_SLOW"]:
            recommendations.append("Query may take significant time - consider scheduling during off-peak hours")
        
        # Complexity recommendations
        if analysis.query_complexity_score > 0.7:
            recommendations.append("Complex query detected - consider simplification for maintainability")
        
        return recommendations
    
    def _is_dml_query(self, sql: str) -> bool:
        """Check if query contains DML operations"""
        dml_keywords = ["INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "ALTER"]
        sql_upper = sql.upper().strip()
        
        for keyword in dml_keywords:
            if sql_upper.startswith(keyword):
                return True
        
        return False
    
    async def _create_optimization_pr(self, sql: str, analysis: QueryCostAnalysis) -> Dict[str, Any]:
        """Create optimization PR if significant savings are possible"""
        # This would integrate with CreateOptimizationPRTool
        # For now, return placeholder
        return {
            "pr_creation_attempted": True,
            "estimated_savings": analysis.estimated_cost_usd * 0.3,  # Assume 30% savings
            "note": "PR creation requires GitHub integration to be configured"
        }
    
    async def health_check(self) -> bool:
        """Verify tool can perform dry-run queries"""
        try:
            test_sql = "SELECT 1 as test_value"
            await self._perform_dry_run_analysis(test_sql)
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
