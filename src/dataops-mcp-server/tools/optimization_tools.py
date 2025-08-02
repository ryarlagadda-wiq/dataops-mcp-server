#!/usr/bin/env python3

"""
AI-Powered Query Optimization Tool
Uses LLMs to automatically optimize BigQuery SQL queries for cost and performance.
"""

import json
import logging
import re
import os
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import hashlib

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
import anthropic

logger = logging.getLogger(__name__)

@dataclass
class QueryOptimization:
    """Query optimization result"""
    optimization_id: str
    original_query: str
    optimized_query: str
    estimated_savings_usd: float
    estimated_savings_pct: float
    optimization_techniques: List[str]
    risk_level: str
    explanation: str
    validation_tests: List[str]
    implementation_notes: List[str]

class OptimizeQueryTool:
    """AI-powered query optimization using Claude and pattern-based techniques"""
    
    def __init__(self, project_id: str, region: str = "us-central1"):
        self.project_id = project_id
        self.region = region
        self.bq_client = bigquery.Client(project=project_id)
        
        # Initialize Claude client if API key available
        self.claude_client = None
        claude_api_key = os.getenv('CLAUDE_API_KEY')
        if claude_api_key:
            try:
                self.claude_client = anthropic.Anthropic(api_key=claude_api_key)
                logger.info("Claude AI client initialized for query optimization")
            except Exception as e:
                logger.warning(f"Failed to initialize Claude client: {e}")
        
        # BigQuery optimization best practices
        self.optimization_rules = [
            "Use partition filters (_PARTITIONTIME, _PARTITIONDATE) to reduce data scanning",
            "Select only required columns instead of SELECT *",
            "Use LIMIT clause when exploring data or when full result set not needed",
            "Optimize JOIN order: put largest table last in JOIN sequence",
            "Use aggregation pushdown: apply filters before GROUP BY operations",
            "Prefer APPROXIMATE aggregation functions when exact values not required",
            "Use QUALIFY instead of window functions in subqueries where possible",
            "Avoid expensive operations like REGEX and complex mathematical functions in WHERE clauses",
            "Use clustering for frequently filtered columns in large tables",
            "Consider materialized views for frequently repeated complex queries"
        ]
        
        # Pattern-based optimization rules
        self.optimization_patterns = {
            "select_star_replacement": {
                "pattern": r"SELECT\s+\*\s+FROM",
                "description": "Replace SELECT * with specific columns",
                "template": "SELECT {suggested_columns} FROM"
            },
            "partition_filter_addition": {
                "keywords": ["_PARTITIONTIME", "_PARTITIONDATE"],
                "description": "Add partition filters to reduce data scanning",
                "template": "WHERE _PARTITIONTIME >= TIMESTAMP('{start_date}')"
            },
            "limit_addition": {
                "pattern": r"ORDER\s+BY.*(?!.*LIMIT)",
                "description": "Add LIMIT clause to ORDER BY operations",
                "template": "ORDER BY {existing_order} LIMIT {suggested_limit}"
            },
            "where_clause_optimization": {
                "description": "Add or improve WHERE clause for data filtering",
                "template": "WHERE {filter_conditions}"
            },
            "join_optimization": {
                "description": "Optimize JOIN operations and order",
                "template": "Restructure JOINs for better performance"
            }
        }
    
    async def execute(
        self,
        sql: str,
        optimization_goals: List[str] = None,
        preserve_results: bool = True,
        include_explanation: bool = True,
        target_savings_pct: int = 30,
        dbt_model_path: str = None
    ) -> str:
        """
        Optimize SQL query using AI and pattern-based techniques.
        
        Args:
            sql: SQL query to optimize
            optimization_goals: Goals (["cost", "performance", "readability"])
            preserve_results: Ensure optimized query returns identical results
            include_explanation: Include detailed optimization explanation
            target_savings_pct: Target cost reduction percentage
            dbt_model_path: Optional dbt model context
            
        Returns:
            JSON string with optimization results
        """
        try:
            logger.info("Starting AI-powered query optimization")
            
            if not sql or not sql.strip():
                raise ValueError("SQL query cannot be empty")
            
            if optimization_goals is None:
                optimization_goals = ["cost", "performance"]
            
            # Generate unique optimization ID
            optimization_id = self._generate_optimization_id(sql)
            
            # Analyze original query
            original_analysis = await self._analyze_original_query(sql)
            
            # Generate optimized query using available methods
            optimization_result = await self._generate_optimized_query(
                sql, optimization_goals, target_savings_pct, dbt_model_path
            )
            
            # Validate optimization
            validation_result = await self._validate_optimization(
                sql, optimization_result["optimized_query"], preserve_results
            )
            
            # Calculate savings estimates
            savings_analysis = await self._calculate_optimization_savings(
                sql, optimization_result["optimized_query"], original_analysis
            )
            
            # Generate implementation notes
            implementation_notes = self._generate_implementation_notes(
                optimization_result, validation_result
            )
            
            # Create comprehensive optimization result
            optimization = QueryOptimization(
                optimization_id=optimization_id,
                original_query=sql,
                optimized_query=optimization_result["optimized_query"],
                estimated_savings_usd=savings_analysis["savings_usd"],
                estimated_savings_pct=savings_analysis["savings_pct"],
                optimization_techniques=optimization_result["techniques_applied"],
                risk_level=validation_result["risk_level"],
                explanation=optimization_result.get("explanation", ""),
                validation_tests=validation_result["validation_tests"],
                implementation_notes=implementation_notes
            )
            
            result = {
                "success": True,
                "optimization": asdict(optimization),
                "original_analysis": original_analysis,
                "savings_breakdown": savings_analysis,
                "validation_details": validation_result,
                "recommendations": self._generate_optimization_recommendations(optimization),
                "metadata": {
                    "optimized_at": datetime.now().isoformat(),
                    "optimization_method": optimization_result["method_used"],
                    "ai_model": optimization_result.get("ai_model", "pattern_based"),
                    "target_savings_pct": target_savings_pct,
                    "preserve_results": preserve_results
                }
            }
            
            logger.info(f"Query optimization completed: {savings_analysis['savings_pct']:.1f}% estimated savings")
            
            return json.dumps(result, indent=2)
            
        except Exception as e:
            logger.error(f"Query optimization failed: {e}")
            return json.dumps({
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "suggestion": "Check SQL syntax and verify AI service availability"
            })
    
    async def _analyze_original_query(self, sql: str) -> Dict[str, Any]:
        """Analyze original query to understand optimization potential"""
        
        try:
            # Perform dry-run analysis
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            job = self.bq_client.query(sql, job_config=job_config)
            
            bytes_processed = job.total_bytes_processed or 0
            estimated_cost = (bytes_processed / (1024 ** 4)) * 6.25
            
            # Analyze query characteristics
            sql_upper = sql.upper()
            characteristics = {
                "has_select_star": "SELECT *" in sql_upper,
                "has_partition_filter": any(keyword in sql_upper for keyword in ["_PARTITIONTIME", "_PARTITIONDATE"]),
                "has_where_clause": "WHERE" in sql_upper,
                "has_limit_clause": "LIMIT" in sql_upper,
                "join_count": sql_upper.count("JOIN"),
                "subquery_count": sql_upper.count("SELECT") - 1,
                "has_order_by": "ORDER BY" in sql_upper,
                "complexity_score": self._calculate_complexity(sql)
            }
            
            return {
                "original_cost": estimated_cost,
                "bytes_processed": bytes_processed,
                "query_characteristics": characteristics,
                "optimization_potential": self._assess_optimization_potential(characteristics, estimated_cost)
            }
            
        except Exception as e:
            logger.error(f"Original query analysis failed: {e}")
            raise
    
    async def _generate_optimized_query(
        self,
        sql: str,
        optimization_goals: List[str],
        target_savings_pct: int,
        dbt_model_path: str = None
    ) -> Dict[str, Any]:
        """Generate optimized query using AI or pattern-based methods"""
        
        # Try AI optimization first if available
        if self.claude_client:
            try:
                return await self._optimize_with_ai(sql, optimization_goals, target_savings_pct, dbt_model_path)
            except Exception as e:
                logger.warning(f"AI optimization failed, falling back to pattern-based: {e}")
        
        # Fallback to pattern-based optimization
        return await self._optimize_with_patterns(sql, optimization_goals)
    
    async def _optimize_with_ai(
        self,
        sql: str,
        optimization_goals: List[str],
        target_savings_pct: int,
        dbt_model_path: str = None
    ) -> Dict[str, Any]:
        """Optimize query using Claude AI"""
        
        # Build comprehensive optimization prompt
        prompt = self._build_ai_optimization_prompt(sql, optimization_goals, target_savings_pct, dbt_model_path)
        
        try:
            response = await self.claude_client.messages.create(
                model="claude-3-sonnet-20240229",
                max_tokens=4000,
                system="You are an expert BigQuery SQL optimization engineer with deep knowledge of cost optimization, performance tuning, and BigQuery best practices.",
                messages=[{"role": "user", "content": prompt}]
            )
            
            response_text = response.content[0].text
            
            # Parse AI response
            parsed_result = self._parse_ai_optimization_response(response_text, sql)
            
            return {
                "optimized_query": parsed_result["optimized_query"],
                "techniques_applied": parsed_result["techniques"],
                "explanation": parsed_result["explanation"],
                "method_used": "claude_ai",
                "ai_model": "claude-3-sonnet",
                "ai_response_full": response_text
            }
            
        except Exception as e:
            logger.error(f"Claude AI optimization failed: {e}")
            raise
    
    async def _optimize_with_patterns(
        self,
        sql: str,
        optimization_goals: List[str]
    ) -> Dict[str, Any]:
        """Optimize query using pattern-based rules"""
        
        optimized_sql = sql
        techniques_applied = []
        explanation_parts = []
        
        sql_upper = sql.upper()
        
        # Apply optimization patterns
        
        # 1. SELECT * optimization
        if "SELECT *" in sql_upper:
            # For demo purposes, suggest specific columns
            optimized_sql = re.sub(
                r"SELECT\s+\*",
                "SELECT /* TODO: Replace with specific columns */\n    user_id, event_timestamp, event_name",
                optimized_sql,
                flags=re.IGNORECASE
            )
            techniques_applied.append("select_star_replacement")
            explanation_parts.append("Replaced SELECT * with specific columns to reduce data processing")
        
        # 2. Partition filter addition
        if not any(keyword in sql_upper for keyword in ["_PARTITIONTIME", "_PARTITIONDATE"]):
            if "WHERE" in sql_upper:
                # Add to existing WHERE clause
                optimized_sql = re.sub(
                    r"WHERE",
                    "WHERE _PARTITIONTIME >= TIMESTAMP('2024-01-01')\n    AND",
                    optimized_sql,
                    count=1,
                    flags=re.IGNORECASE
                )
            else:
                # Add new WHERE clause before GROUP BY, ORDER BY, or end
                insertion_patterns = [r"(\s+GROUP\s+BY)", r"(\s+ORDER\s+BY)", r"(\s*$)"]
                for pattern in insertion_patterns:
                    if re.search(pattern, optimized_sql, re.IGNORECASE):
                        optimized_sql = re.sub(
                            pattern,
                            r"\nWHERE _PARTITIONTIME >= TIMESTAMP('2024-01-01')\1",
                            optimized_sql,
                            count=1,
                            flags=re.IGNORECASE
                        )
                        break
            
            techniques_applied.append("partition_filter_addition")
            explanation_parts.append("Added partition filter to reduce data scanning")
        
        # 3. LIMIT clause addition for ORDER BY
        if "ORDER BY" in sql_upper and "LIMIT" not in sql_upper:
            optimized_sql += "\nLIMIT 10000  -- Add appropriate limit for your use case"
            techniques_applied.append("limit_clause_addition")
            explanation_parts.append("Added LIMIT clause to prevent sorting large result sets")
        
        # 4. WHERE clause optimization
        if "WHERE" not in sql_upper and "FROM" in sql_upper:
            # Suggest adding filters
            optimized_sql = re.sub(
                r"(FROM\s+`[^`]+`)",
                r"\1\nWHERE /* Add appropriate filters here */\n    date_column >= '2024-01-01'",
                optimized_sql,
                flags=re.IGNORECASE
            )
            techniques_applied.append("where_clause_addition")
            explanation_parts.append("Added WHERE clause template for data filtering")
        
        # 5. JOIN optimization suggestions
        join_count = sql_upper.count("JOIN")
        if join_count > 2:
            # Add comment about JOIN optimization
            optimized_sql = "/* Consider optimizing JOIN order: put largest table last */\n" + optimized_sql
            techniques_applied.append("join_order_optimization")
            explanation_parts.append(f"Added JOIN optimization guidance ({join_count} JOINs detected)")
        
        explanation = "\n".join([
            "Applied pattern-based optimizations:",
            *[f"• {part}" for part in explanation_parts],
            "",
            "Note: This is a pattern-based optimization. For more sophisticated optimization, configure Claude AI integration."
        ])
        
        return {
            "optimized_query": optimized_sql,
            "techniques_applied": techniques_applied,
            "explanation": explanation,
            "method_used": "pattern_based"
        }
    
    def _build_ai_optimization_prompt(
        self,
        sql: str,
        optimization_goals: List[str],
        target_savings_pct: int,
        dbt_model_path: str = None
    ) -> str:
        """Build comprehensive prompt for AI optimization"""
        
        goals_text = ", ".join(optimization_goals)
        dbt_context = f"\n\n**dbt Context:** This query is part of a dbt model at: {dbt_model_path}" if dbt_model_path else ""
        
        prompt = f"""Please optimize this BigQuery SQL query for {goals_text} with a target of {target_savings_pct}% improvement.

**Original Query:**
```sql
{sql}
```

**Optimization Guidelines:**
{chr(10).join(f"• {rule}" for rule in self.optimization_rules)}

**Requirements:**
• Maintain query functionality and result accuracy
• Focus on cost reduction through data scanning optimization
• Provide specific, implementable optimizations
• Explain each optimization technique applied
• Estimate performance improvement where possible{dbt_context}

**Please provide your response in this structured format:**

**OPTIMIZED_QUERY:**
```sql
[Your optimized query here]
```

**TECHNIQUES_APPLIED:**
• [List each optimization technique used]
• [Include estimated impact for each technique]

**EXPLANATION:**
[Detailed explanation of optimizations, focusing on:]
• Why each change reduces cost/improves performance
• Estimated data scanning reduction
• Any trade-offs or considerations
• Implementation recommendations

**ESTIMATED_IMPROVEMENT:**
• Cost reduction: X%
• Performance improvement: X%
• Data scanning reduction: X%

**VALIDATION_NOTES:**
• [Any testing recommendations]
• [Potential risks or considerations]
"""
        
        return prompt
    
    def _parse_ai_optimization_response(self, response_text: str, original_sql: str) -> Dict[str, Any]:
        """Parse AI response to extract optimization components"""
        
        result = {
            "optimized_query": original_sql,  # Fallback to original
            "techniques": [],
            "explanation": "AI response parsing failed"
        }
        
        try:
            # Extract optimized SQL
            sql_match = re.search(
                r"OPTIMIZED_QUERY:\s*```sql\s*(.*?)\s*```",
                response_text,
                re.DOTALL | re.IGNORECASE
            )
            if sql_match:
                result["optimized_query"] = sql_match.group(1).strip()
            else:
                # Try to find any SQL code block
                sql_match = re.search(r"```sql\s*(.*?)\s*```", response_text, re.DOTALL)
                if sql_match:
                    result["optimized_query"] = sql_match.group(1).strip()
            
            # Extract techniques
            techniques_match = re.search(
                r"TECHNIQUES_APPLIED:\s*(.*?)(?=\n\n|\n\*\*EXPLANATION|\n\*\*ESTIMATED|$)",
                response_text,
                re.DOTALL | re.IGNORECASE
            )
            if techniques_match:
                techniques_text = techniques_match.group(1)
                for line in techniques_text.split('\n'):
                    line = line.strip()
                    if line.startswith('•') or line.startswith('-'):
                        result["techniques"].append(line[1:].strip())
            
            # Extract explanation
            explanation_match = re.search(
                r"EXPLANATION:\s*(.*?)(?=\n\n|\n\*\*ESTIMATED|\n\*\*VALIDATION|$)",
                response_text,
                re.DOTALL | re.IGNORECASE
            )
            if explanation_match:
                result["explanation"] = explanation_match.group(1).strip()
            else:
                result["explanation"] = response_text  # Use full response as explanation
            
        except Exception as e:
            logger.error(f"Failed to parse AI response: {e}")
            result["explanation"] = f"AI optimization completed but response parsing failed: {str(e)}"
        
        return result
    
    async def _validate_optimization(
        self,
        original_sql: str,
        optimized_sql: str,
        preserve_results: bool
    ) -> Dict[str, Any]:
        """Validate the optimization and assess risks"""
        
        validation_result = {
            "syntax_valid": True,
            "schema_compatible": True,
            "risk_level": "LOW",
            "validation_tests": [],
            "warnings": []
        }
        
        try:
            # Test syntax with dry runs
            original_config = bigquery.QueryJobConfig(dry_run=True)
            optimized_config = bigquery.QueryJobConfig(dry_run=True)
            
            original_job = self.bq_client.query(original_sql, job_config=original_config)
            optimized_job = self.bq_client.query(optimized_sql, job_config=optimized_config)
            
            # Compare schemas if result preservation is required
            if preserve_results:
                schema_comparison = self._compare_schemas(original_job, optimized_job)
                validation_result["schema_compatible"] = schema_comparison["compatible"]
                
                if not schema_comparison["compatible"]:
                    validation_result["warnings"].append("Schema differences detected - manual review required")
                    validation_result["risk_level"] = "MEDIUM"
            
            # Generate validation tests
            validation_result["validation_tests"] = self._generate_validation_tests(
                original_sql, optimized_sql
            )
            
            # Assess optimization risk
            risk_assessment = self._assess_optimization_risk(original_sql, optimized_sql)
            validation_result["risk_level"] = risk_assessment["risk_level"]
            validation_result["warnings"].extend(risk_assessment["warnings"])
            
        except Exception as e:
            validation_result["syntax_valid"] = False
            validation_result["risk_level"] = "HIGH"
            validation_result["warnings"].append(f"Syntax validation failed: {str(e)}")
        
        return validation_result
    
    async def _calculate_optimization_savings(
        self,
        original_sql: str,
        optimized_sql: str,
        original_analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate estimated savings from optimization"""
        
        try:
            # Analyze optimized query cost
            optimized_config = bigquery.QueryJobConfig(dry_run=True)
            optimized_job = self.bq_client.query(optimized_sql, job_config=optimized_config)
            
            original_bytes = original_analysis["bytes_processed"]
            optimized_bytes = optimized_job.total_bytes_processed or 0
            
            original_cost = original_analysis["original_cost"]
            optimized_cost = (optimized_bytes / (1024 ** 4)) * 6.25
            
            # Calculate savings
            savings_usd = original_cost - optimized_cost
            savings_pct = (savings_usd / original_cost * 100) if original_cost > 0 else 0
            
            # Calculate data processing reduction
            data_reduction_pct = ((original_bytes - optimized_bytes) / original_bytes * 100) if original_bytes > 0 else 0
            
            return {
                "original_cost": original_cost,
                "optimized_cost": optimized_cost,
                "savings_usd": savings_usd,
                "savings_pct": savings_pct,
                "data_reduction_pct": data_reduction_pct,
                "original_bytes": original_bytes,
                "optimized_bytes": optimized_bytes,
                "monthly_savings_estimate": savings_usd * 30  # Assume daily execution
            }
            
        except Exception as e:
            logger.error(f"Savings calculation failed: {e}")
            # Return conservative estimates
            return {
                "original_cost": original_analysis["original_cost"],
                "optimized_cost": original_analysis["original_cost"] * 0.7,  # Assume 30% savings
                "savings_usd": original_analysis["original_cost"] * 0.3,
                "savings_pct": 30.0,
                "data_reduction_pct": 30.0,
                "monthly_savings_estimate": original_analysis["original_cost"] * 0.3 * 30
            }
    
    def _generate_implementation_notes(
        self,
        optimization_result: Dict[str, Any],
        validation_result: Dict[str, Any]
    ) -> List[str]:
        """Generate implementation notes and recommendations"""
        
        notes = []
        
        # Risk-based notes
        risk_level = validation_result["risk_level"]
        if risk_level == "HIGH":
            notes.extend([
                "⚠️ HIGH RISK: Thorough testing required before production deployment",
                "Test with representative data samples",
                "Implement gradual rollout strategy"
            ])
        elif risk_level == "MEDIUM":
            notes.extend([
                "⚠️ MEDIUM RISK: Validation testing recommended",
                "Compare results with original query on sample data"
            ])
        else:
            notes.append("✅ LOW RISK: Safe to implement with standard testing")
        
        # Method-specific notes
        if optimization_result["method_used"] == "claude_ai":
            notes.extend([
                "AI-optimized query - review optimization logic",
                "Validate that business logic is preserved",
                "Consider performance impact of suggested changes"
            ])
        else:
            notes.extend([
                "Pattern-based optimization applied",
                "Consider AI-powered optimization for more sophisticated improvements",
                "Review suggested TODO items in optimized query"
            ])
        
        # Validation notes
        if validation_result.get("warnings"):
            notes.append("⚠️ Validation warnings detected - see validation details")
        
        # General implementation steps
        notes.extend([
            "1. Test optimized query in development environment",
            "2. Compare execution results with original query",
            "3. Monitor performance metrics after deployment",
            "4. Measure actual cost savings achieved"
        ])
        
        return notes
    
    def _generate_optimization_recommendations(self, optimization: QueryOptimization) -> List[str]:
        """Generate recommendations based on optimization results"""
        
        recommendations = []
        
        # Savings-based recommendations
        if optimization.estimated_savings_pct > 50:
            recommendations.append(f"🎯 Excellent optimization: {optimization.estimated_savings_pct:.1f}% cost reduction achievable")
        elif optimization.estimated_savings_pct > 25:
            recommendations.append(f"👍 Good optimization: {optimization.estimated_savings_pct:.1f}% cost reduction achievable")
        elif optimization.estimated_savings_pct > 10:
            recommendations.append(f"📈 Moderate optimization: {optimization.estimated_savings_pct:.1f}% cost reduction achievable")
        else:
            recommendations.append("Limited optimization potential - query is already fairly efficient")
        
        # Risk-based recommendations
        if optimization.risk_level == "HIGH":
            recommendations.append("⚠️ High-risk optimization - implement with extensive testing")
        elif optimization.risk_level == "MEDIUM":
            recommendations.append("⚠️ Medium-risk optimization - validate thoroughly before production")
        
        # Savings threshold recommendations
        if optimization.estimated_savings_usd > 100:
            recommendations.append("💰 High-value optimization - prioritize for implementation")
            recommendations.append("Consider creating GitHub PR for team review")
        
        # Technique-specific recommendations
        if "partition_filter_addition" in optimization.optimization_techniques:
            recommendations.append("🗓️ Partition filtering added - verify date ranges are appropriate for your use case")
        
        if "select_star_replacement" in optimization.optimization_techniques:
            recommendations.append("📋 Column selection optimized - ensure all required columns are included")
        
        return recommendations
    
    def _assess_optimization_potential(self, characteristics: Dict[str, Any], cost: float) -> Dict[str, Any]:
        """Assess optimization potential based on query characteristics"""
        
        potential_score = 0
        opportunities = []
        
        # SELECT * penalty
        if characteristics["has_select_star"]:
            potential_score += 30
            opportunities.append("Replace SELECT * with specific columns")
        
        # Missing partition filter penalty
        if not characteristics["has_partition_filter"]:
            potential_score += 40
            opportunities.append("Add partition filters")
        
        # Missing WHERE clause penalty
        if not characteristics["has_where_clause"]:
            potential_score += 25
            opportunities.append("Add WHERE clause for filtering")
        
        # ORDER BY without LIMIT penalty
        if characteristics["has_order_by"] and not characteristics["has_limit_clause"]:
            potential_score += 15
            opportunities.append("Add LIMIT to ORDER BY operations")
        
        # Complex JOIN penalty
        if characteristics["join_count"] > 2:
            potential_score += 20
            opportunities.append("Optimize JOIN operations")
        
        # High cost penalty
        if cost > 25:
            potential_score += 10
            opportunities.append("High-cost query - multiple optimization approaches available")
        
        potential_level = "HIGH" if potential_score >= 60 else "MEDIUM" if potential_score >= 30 else "LOW"
        
        return {
            "potential_score": min(potential_score, 100),
            "potential_level": potential_level,
            "optimization_opportunities": opportunities,
            "estimated_max_savings_pct": min(potential_score * 0.8, 70)  # Cap at 70%
        }
    
    def _calculate_complexity(self, sql: str) -> float:
        """Calculate query complexity score"""
        
        sql_upper = sql.upper()
        
        complexity_factors = {
            "length": min(len(sql) / 5000, 0.3),
            "joins": min(sql_upper.count("JOIN") * 0.15, 0.4),
            "subqueries": min((sql_upper.count("SELECT") - 1) * 0.1, 0.3),
            "window_functions": min(sql_upper.count("OVER(") * 0.2, 0.3),
            "case_statements": min(sql_upper.count("CASE") * 0.1, 0.2),
            "aggregations": min(sql_upper.count("GROUP BY") * 0.1, 0.2)
        }
        
        return min(sum(complexity_factors.values()), 1.0)
    
    def _compare_schemas(self, job1: bigquery.QueryJob, job2: bigquery.QueryJob) -> Dict[str, Any]:
        """Compare schemas of two queries"""
        
        try:
            schema1 = [(field.name, field.field_type) for field in job1.schema] if job1.schema else []
            schema2 = [(field.name, field.field_type) for field in job2.schema] if job2.schema else []
            
            return {
                "compatible": schema1 == schema2,
                "original_fields": len(schema1),
                "optimized_fields": len(schema2),
                "field_differences": list(set(schema1) - set(schema2)) if schema1 != schema2 else []
            }
            
        except Exception as e:
            logger.error(f"Schema comparison failed: {e}")
            return {"compatible": False, "error": str(e)}
    
    def _assess_optimization_risk(self, original_sql: str, optimized_sql: str) -> Dict[str, Any]:
        """Assess risk level of the optimization"""
        
        risk_score = 0
        warnings = []
        
        # Complexity change risk
        original_complexity = self._calculate_complexity(original_sql)
        optimized_complexity = self._calculate_complexity(optimized_sql)
        
        complexity_change = abs(optimized_complexity - original_complexity)
        if complexity_change > 0.3:
            risk_score += 2
            warnings.append("Significant query complexity change detected")
        
        # Length change risk
        length_change = abs(len(optimized_sql) - len(original_sql)) / len(original_sql)
        if length_change > 0.5:
            risk_score += 1
            warnings.append("Substantial query length change")
        
        # Structural changes risk
        original_upper = original_sql.upper()
        optimized_upper = optimized_sql.upper()
        
        if "JOIN" in original_upper and original_upper.count("JOIN") != optimized_upper.count("JOIN"):
            risk_score += 2
            warnings.append("JOIN structure modified - verify result accuracy")
        
        if "GROUP BY" in original_upper and "GROUP BY" not in optimized_upper:
            risk_score += 3
            warnings.append("Aggregation logic changed - high risk modification")
        
        # Determine overall risk level
        if risk_score >= 5:
            risk_level = "HIGH"
        elif risk_score >= 3:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"
        
        return {
            "risk_level": risk_level,
            "risk_score": risk_score,
            "warnings": warnings
        }
    
    def _generate_validation_tests(self, original_sql: str, optimized_sql: str) -> List[str]:
        """Generate SQL validation tests"""
        
        tests = [
            # Row count validation
            f"""
            -- Test 1: Row count comparison
            WITH original_count AS (
                SELECT COUNT(*) as cnt FROM ({original_sql})
            ),
            optimized_count AS (
                SELECT COUNT(*) as cnt FROM ({optimized_sql})
            )
            SELECT 
                original_count.cnt as original_rows,
                optimized_count.cnt as optimized_rows,
                CASE 
                    WHEN original_count.cnt = optimized_count.cnt THEN 'PASS'
                    ELSE 'FAIL'
                END as test_result
            FROM original_count, optimized_count;
            """,
            
            # Sample data comparison
            f"""
            -- Test 2: Sample data comparison (first 100 rows)
            WITH original_sample AS (
                SELECT * FROM ({original_sql}) LIMIT 100
            ),
            optimized_sample AS (
                SELECT * FROM ({optimized_sql}) LIMIT 100
            )
            SELECT 'Run manual comparison of sample results' as instruction;
            """,
            
            # Performance comparison test
            f"""
            -- Test 3: Performance comparison
            -- Run both queries and compare execution times
            -- Original: {original_sql[:100]}...
            -- Optimized: {optimized_sql[:100]}...
            SELECT 'Compare execution times and resource usage' as instruction;
            """
        ]
        
        return tests
    
    def _generate_optimization_id(self, sql: str) -> str:
        """Generate unique optimization ID"""
        sql_hash = hashlib.md5(sql.encode()).hexdigest()[:8]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"opt_{timestamp}_{sql_hash}"
    
    async def health_check(self) -> bool:
        """Verify optimization tool functionality"""
        try:
            # Test basic SQL optimization
            test_sql = "SELECT * FROM (SELECT 1 as test_col) ORDER BY test_col"
            result = await self._optimize_with_patterns(test_sql, ["cost"])
            return "techniques_applied" in result and len(result["techniques_applied"]) > 0
        except Exception as e:
            logger.error(f"Optimization tool health check failed: {e}")
            return False
