#!/usr/bin/env python3

"""
GitHub PR Creation Tool
Automatically creates GitHub pull requests with query optimizations and cost analysis.
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict

from github import Github, GithubException
from github.Repository import Repository

logger = logging.getLogger(__name__)

@dataclass
class PRCreationResult:
    """GitHub PR creation result"""
    pr_number: int
    pr_url: str
    branch_name: str
    title: str
    estimated_savings: float
    files_changed: List[str]
    reviewers_assigned: List[str]
    labels_applied: List[str]
    status: str

class CreateOptimizationPRTool:
    """Tool for creating GitHub PRs with query optimizations"""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        
        # Initialize GitHub client
        self.github_client = None
        self.repository = None
        
        github_token = os.getenv('GITHUB_TOKEN')
        if github_token:
            try:
                self.github_client = Github(github_token)
                
                # Default repository configuration
                repo_name = os.getenv('GITHUB_REPOSITORY', 'quantium/data-platform')
                self.repository = self.github_client.get_repo(repo_name)
                
                logger.info(f"GitHub client initialized for repository: {repo_name}")
                
            except Exception as e:
                logger.error(f"Failed to initialize GitHub client: {e}")
        else:
            logger.warning("GitHub token not found - PR creation will not be available")
        
        # PR configuration
        self.pr_config = {
            "default_base_branch": "main",
            "default_reviewers": ["data-engineering-team"],
            "default_labels": ["cost-optimization", "automated"],
            "min_savings_for_pr": 25.0,  # Minimum $25 savings to create PR
            "file_paths": {
                "sql_optimizations": "sql/optimizations/",
                "dbt_models": "dbt/models/",
                "documentation": "docs/optimizations/"
            }
        }
    
    async def execute(
        self,
        optimization_id: str,
        repository: str = None,
        base_branch: str = "main",
        title_prefix: str = "🚀 Cost Optimization",
        assign_reviewers: bool = True,
        include_tests: bool = True
    ) -> str:
        """
        Create GitHub PR with query optimization implementation.
        
        Args:
            optimization_id: ID from previous optimization analysis
            repository: GitHub repository name (uses default if not specified)
            base_branch: Base branch for PR (default: main)
            title_prefix: PR title prefix
            assign_reviewers: Auto-assign reviewers
            include_tests: Generate validation tests
            
        Returns:
            JSON string with PR creation result
        """
        try:
            logger.info(f"Creating optimization PR for optimization ID: {optimization_id}")
            
            if not self.github_client:
                raise ValueError("GitHub integration not configured - missing GITHUB_TOKEN")
            
            # Load optimization data (in real implementation, this would come from a database/cache)
            optimization_data = await self._load_optimization_data(optimization_id)
            
            if not optimization_data:
                raise ValueError(f"Optimization data not found for ID: {optimization_id}")
            
            # Validate savings threshold
            estimated_savings = optimization_data.get("estimated_savings_usd", 0)
            if estimated_savings < self.pr_config["min_savings_for_pr"]:
                return json.dumps({
                    "success": False,
                    "error": f"Savings ${estimated_savings:.2f} below minimum threshold ${self.pr_config['min_savings_for_pr']}",
                    "suggestion": "Consider implementing manually or lowering threshold"
                })
            
            # Create branch name
            branch_name = self._generate_branch_name(optimization_id, optimization_data)
            
            # Create new branch
            await self._create_branch(branch_name, base_branch)
            
            # Generate file changes
            file_changes = await self._generate_file_changes(optimization_data, include_tests)
            
            # Create/update files in branch
            created_files = await self._create_files_in_branch(branch_name, file_changes)
            
            # Create pull request
            pr_details = await self._create_pull_request(
                branch_name, base_branch, title_prefix, optimization_data, created_files
            )
            
            # Assign reviewers if requested
            assigned_reviewers = []
            if assign_reviewers:
                assigned_reviewers = await self._assign_reviewers(pr_details["pr"], optimization_data)
            
            # Apply labels
            applied_labels = await self._apply_labels(pr_details["pr"], optimization_data)
            
            # Create result
            result = PRCreationResult(
                pr_number=pr_details["pr"].number,
                pr_url=pr_details["pr"].html_url,
                branch_name=branch_name,
                title=pr_details["title"],
                estimated_savings=estimated_savings,
                files_changed=created_files,
                reviewers_assigned=assigned_reviewers,
                labels_applied=applied_labels,
                status="created"
            )
            
            response = {
                "success": True,
                "pr_result": asdict(result),
                "next_steps": [
                    "Review the created PR for accuracy",
                    "Run tests in CI/CD pipeline",
                    "Get approval from assigned reviewers",
                    "Merge after validation passes"
                ],
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "optimization_id": optimization_id,
                    "repository": self.repository.full_name
                }
            }
            
            logger.info(f"PR created successfully: #{result.pr_number} with ${estimated_savings:.2f} estimated savings")
            
            return json.dumps(response, indent=2)
            
        except Exception as e:
            logger.error(f"PR creation failed: {e}")
            return json.dumps({
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__
            })
    
    async def _load_optimization_data(self, optimization_id: str) -> Optional[Dict[str, Any]]:
        """Load optimization data by ID (mock implementation)"""
        
        # In a real implementation, this would load from:
        # - Database/cache of previous optimizations
        # - File system storage
        # - Integration with OptimizeQueryTool results
        
        # For demo purposes, return mock data
        # This would be replaced with actual data loading logic
        return {
            "optimization_id": optimization_id,
            "original_query": """
                SELECT * 
                FROM `project.dataset.user_events` 
                WHERE user_id IS NOT NULL 
                ORDER BY event_timestamp DESC
            """,
            "optimized_query": """
                SELECT user_id, event_timestamp, event_name, event_properties
                FROM `project.dataset.user_events` 
                WHERE _PARTITIONTIME >= TIMESTAMP('2024-07-01')
                    AND user_id IS NOT NULL 
                ORDER BY event_timestamp DESC
                LIMIT 10000
            """,
            "estimated_savings_usd": 127.50,
            "estimated_savings_pct": 65.2,
            "optimization_techniques": [
                "select_star_replacement",
                "partition_filter_addition", 
                "limit_clause_addition"
            ],
            "explanation": "Optimized query reduces data scanning by 65% through partition filtering and column selection",
            "risk_level": "LOW",
            "query_context": {
                "usage_frequency": "hourly",
                "business_purpose": "user engagement analysis",
                "affected_dashboards": ["user_analytics", "engagement_report"]
            }
        }
    
    def _generate_branch_name(self, optimization_id: str, optimization_data: Dict[str, Any]) -> str:
        """Generate descriptive branch name for the optimization"""
        
        # Extract key info for branch name
        savings_pct = int(optimization_data.get("estimated_savings_pct", 0))
        date_str = datetime.now().strftime("%Y%m%d")
        
        # Create clean branch name
        branch_name = f"optimization/{optimization_id}-{savings_pct}pct-savings-{date_str}"
        
        return branch_name
    
    async def _create_branch(self, branch_name: str, base_branch: str):
        """Create new Git branch for the optimization"""
        
        try:
            # Get reference to base branch
            base_ref = self.repository.get_git_ref(f"heads/{base_branch}")
            
            # Create new branch
            self.repository.create_git_ref(
                ref=f"refs/heads/{branch_name}",
                sha=base_ref.object.sha
            )
            
            logger.info(f"Created branch: {branch_name}")
            
        except GithubException as e:
            if "already exists" in str(e):
                logger.warning(f"Branch {branch_name} already exists, using existing branch")
            else:
                raise
    
    async def _generate_file_changes(
        self,
        optimization_data: Dict[str, Any],
        include_tests: bool
    ) -> List[Dict[str, Any]]:
        """Generate file changes for the optimization"""
        
        file_changes = []
        optimization_id = optimization_data["optimization_id"]
        
        # 1. Create optimized SQL file
        sql_content = self._generate_optimized_sql_file(optimization_data)
        file_changes.append({
            "path": f"{self.pr_config['file_paths']['sql_optimizations']}{optimization_id}.sql",
            "content": sql_content,
            "type": "create"
        })
        
        # 2. Create dbt model if applicable
        if optimization_data.get("query_context", {}).get("usage_frequency") in ["daily", "hourly"]:
            dbt_content = self._generate_dbt_model_file(optimization_data)
            file_changes.append({
                "path": f"{self.pr_config['file_paths']['dbt_models']}optimized_{optimization_id}.sql",
                "content": dbt_content,
                "type": "create"
            })
        
        # 3. Create documentation
        doc_content = self._generate_optimization_documentation(optimization_data)
        file_changes.append({
            "path": f"{self.pr_config['file_paths']['documentation']}{optimization_id}.md",
            "content": doc_content,
            "type": "create"
        })
        
        # 4. Create validation tests if requested
        if include_tests:
            test_content = self._generate_validation_test_file(optimization_data)
            file_changes.append({
                "path": f"tests/optimization_validation/{optimization_id}_test.sql",
                "content": test_content,
                "type": "create"
            })
        
        return file_changes
    
    async def _create_files_in_branch(
        self,
        branch_name: str,
        file_changes: List[Dict[str, Any]]
    ) -> List[str]:
        """Create or update files in the branch"""
        
        created_files = []
        
        for file_change in file_changes:
            try:
                file_path = file_change["path"]
                content = file_change["content"]
                operation = file_change["type"]
                
                if operation == "create":
                    # Create new file
                    self.repository.create_file(
                        path=file_path,
                        message=f"Add {file_path}",
                        content=content,
                        branch=branch_name
                    )
                    created_files.append(file_path)
                    logger.info(f"Created file: {file_path}")
                
                elif operation == "update":
                    # Update existing file
                    try:
                        existing_file = self.repository.get_contents(file_path, ref=branch_name)
                        self.repository.update_file(
                            path=file_path,
                            message=f"Update {file_path}",
                            content=content,
                            sha=existing_file.sha,
                            branch=branch_name
                        )
                        created_files.append(file_path)
                        logger.info(f"Updated file: {file_path}")
                    except:
                        # File doesn't exist, create it
                        self.repository.create_file(
                            path=file_path,
                            message=f"Create {file_path}",
                            content=content,
                            branch=branch_name
                        )
                        created_files.append(file_path)
                
            except Exception as e:
                logger.error(f"Failed to create/update file {file_path}: {e}")
                # Continue with other files
        
        return created_files
    
    async def _create_pull_request(
        self,
        branch_name: str,
        base_branch: str,
        title_prefix: str,
        optimization_data: Dict[str, Any],
        created_files: List[str]
    ) -> Dict[str, Any]:
        """Create the pull request"""
        
        # Generate PR title
        savings_usd = optimization_data.get("estimated_savings_usd", 0)
        savings_pct = optimization_data.get("estimated_savings_pct", 0)
        title = f"{title_prefix}: ${savings_usd:.0f}/month savings ({savings_pct:.1f}% reduction)"
        
        # Generate PR description
        description = self._generate_pr_description(optimization_data, created_files)
        
        # Create PR
        pr = self.repository.create_pull(
            title=title,
            body=description,
            head=branch_name,
            base=base_branch
        )
        
        logger.info(f"Created PR #{pr.number}: {title}")
        
        return {
            "pr": pr,
            "title": title,
            "description": description
        }
    
    def _generate_optimized_sql_file(self, optimization_data: Dict[str, Any]) -> str:
        """Generate optimized SQL file content"""
        
        optimization_id = optimization_data["optimization_id"]
        estimated_savings = optimization_data.get("estimated_savings_usd", 0)
        savings_pct = optimization_data.get("estimated_savings_pct", 0)
        techniques = optimization_data.get("optimization_techniques", [])
        
        content = f"""-- Optimized Query: {optimization_id}
-- Generated by GCP Cost Optimization MCP Agent
-- 
-- COST SAVINGS ANALYSIS:
-- Original estimated cost: ${estimated_savings / (savings_pct / 100):.2f}
-- Optimized estimated cost: ${estimated_savings / (savings_pct / 100) - estimated_savings:.2f}
-- Estimated savings: ${estimated_savings:.2f} ({savings_pct:.1f}% reduction)
-- 
-- OPTIMIZATION TECHNIQUES APPLIED:
{chr(10).join(f'-- • {technique}' for technique in techniques)}
-- 
-- IMPLEMENTATION NOTES:
-- • Test this query with representative data before deploying to production
-- • Monitor performance metrics after deployment  
-- • Verify that results match the original query output
-- • Consider implementing gradually if this is a critical production query
--
-- ORIGINAL QUERY (for reference):
/*
{optimization_data.get('original_query', '').strip()}
*/

-- OPTIMIZED QUERY:
{optimization_data.get('optimized_query', '').strip()}
"""
        
        return content
    
    def _generate_dbt_model_file(self, optimization_data: Dict[str, Any]) -> str:
        """Generate dbt model file content"""
        
        optimization_id = optimization_data["optimization_id"]
        optimized_query = optimization_data.get("optimized_query", "").strip()
        
        # Basic dbt model template
        content = f"""{{{{ config(
    materialized='table',
    partition_by={{
        "field": "_partitiontime",
        "data_type": "timestamp",
        "granularity": "day"
    }},
    cluster_by=['user_id'],
    description="Optimized model generated from cost optimization analysis"
) }}}}

/*
    Optimized dbt model: {optimization_id}
    
    This model was generated by the GCP Cost Optimization MCP Agent
    as part of automated cost reduction initiatives.
    
    Cost savings: ${optimization_data.get('estimated_savings_usd', 0):.2f}/month
    Performance improvement: {optimization_data.get('estimated_savings_pct', 0):.1f}%
    
    Original query location: sql/optimizations/{optimization_id}.sql
*/

{optimized_query}
"""
        
        return content
    
    def _generate_optimization_documentation(self, optimization_data: Dict[str, Any]) -> str:
        """Generate optimization documentation"""
        
        optimization_id = optimization_data["optimization_id"]
        estimated_savings = optimization_data.get("estimated_savings_usd", 0)
        savings_pct = optimization_data.get("estimated_savings_pct", 0)
        techniques = optimization_data.get("optimization_techniques", [])
        explanation = optimization_data.get("explanation", "")
        risk_level = optimization_data.get("risk_level", "UNKNOWN")
        
        content = f"""# Query Optimization: {optimization_id}

## Summary
- **Estimated Monthly Savings**: ${estimated_savings:.2f}
- **Cost Reduction**: {savings_pct:.1f}%
- **Risk Level**: {risk_level}
- **Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Problem Statement
This optimization addresses high query costs in our BigQuery usage. The original query was identified as a high-cost operation that runs frequently and impacts our overall data processing budget.

## Solution
{explanation}

## Optimization Techniques Applied
{chr(10).join(f'- **{technique.replace("_", " ").title()}**' for technique in techniques)}

## Cost Impact Analysis
- **Before Optimization**: ${estimated_savings / (savings_pct / 100):.2f} per execution
- **After Optimization**: ${estimated_savings / (savings_pct / 100) - estimated_savings:.2f} per execution  
- **Monthly Savings**: ${estimated_savings:.2f} (assuming daily execution)
- **Annual Savings**: ${estimated_savings * 12:.2f}

## Implementation Plan

### Phase 1: Validation (Days 1-2)
- [ ] Run optimized query in development environment
- [ ] Compare results with original query using validation tests
- [ ] Verify performance improvements match estimates
- [ ] Check for any edge cases or data quality issues

### Phase 2: Staged Rollout (Days 3-5)
- [ ] Deploy to staging environment with monitoring
- [ ] Run parallel execution for 24-48 hours
- [ ] Validate business metrics are unaffected
- [ ] Monitor for any performance regressions

### Phase 3: Production Deployment (Days 6-7)  
- [ ] Deploy to production during maintenance window
- [ ] Monitor cost metrics for actual savings validation
- [ ] Update documentation and runbooks
- [ ] Communicate changes to stakeholders

## Testing Checklist
- [ ] Query syntax validation passes
- [ ] Results match original query output (sample testing)
- [ ] Performance improvements verified
- [ ] No regression in dependent downstream processes
- [ ] Monitoring and alerting configured for the optimized query

## Rollback Plan
If issues are discovered after deployment:
1. Revert to original query immediately
2. Investigate root cause of issues
3. Apply fixes to optimized version
4. Re-test before re-deployment

## Risk Assessment: {risk_level}
{self._get_risk_description(risk_level)}

## Monitoring and Success Metrics
- **Cost Reduction**: Track actual vs predicted savings
- **Performance**: Monitor query execution time and resource usage
- **Reliability**: Ensure no increase in query failures or timeouts
- **Business Impact**: Verify downstream analytics and reports are unaffected

## Related Resources
- Original query: `sql/optimizations/{optimization_id}.sql`
- Validation tests: `tests/optimization_validation/{optimization_id}_test.sql`
- dbt model: `dbt/models/optimized_{optimization_id}.sql` (if applicable)

## Questions or Issues?
Contact the Data Engineering team or create an issue in this repository.
"""
        
        return content
    
    def _generate_validation_test_file(self, optimization_data: Dict[str, Any]) -> str:
        """Generate validation test SQL file"""
        
        optimization_id = optimization_data["optimization_id"]
        original_query = optimization_data.get("original_query", "").strip()
        optimized_query = optimization_data.get("optimized_query", "").strip()
        
        content = f"""-- Validation Tests for Optimization: {optimization_id}
-- Generated by GCP Cost Optimization MCP Agent

-- Test 1: Row Count Validation
-- Ensures both queries return the same number of rows
WITH original_count AS (
    SELECT COUNT(*) as row_count
    FROM ({original_query})
),
optimized_count AS (
    SELECT COUNT(*) as row_count  
    FROM ({optimized_query})
)
SELECT 
    'row_count_validation' as test_name,
    original_count.row_count as original_rows,
    optimized_count.row_count as optimized_rows,
    CASE 
        WHEN original_count.row_count = optimized_count.row_count THEN 'PASS'
        ELSE 'FAIL'
    END as test_result,
    ABS(original_count.row_count - optimized_count.row_count) as row_difference
FROM original_count, optimized_count;

-- Test 2: Schema Validation  
-- Compare column names and types between queries
WITH original_schema AS (
    SELECT * FROM ({original_query}) WHERE FALSE  -- Get schema without data
),
optimized_schema AS (
    SELECT * FROM ({optimized_query}) WHERE FALSE  -- Get schema without data
)
SELECT 'schema_validation' as test_name, 'Manual verification required' as note;

-- Test 3: Sample Data Comparison
-- Compare first 100 rows to check for data consistency
WITH original_sample AS (
    SELECT * FROM ({original_query}) LIMIT 100
),
optimized_sample AS (
    SELECT * FROM ({optimized_query}) LIMIT 100  
)
SELECT 'sample_data_comparison' as test_name, 'Manual comparison required' as note;

-- Test 4: Performance Comparison
-- Compare execution statistics (run separately)
-- 
-- Run these queries individually and compare:
-- 1. Original: {original_query[:100]}...
-- 2. Optimized: {optimized_query[:100]}...
--
-- Compare:
-- • Execution time
-- • Bytes processed  
-- • Slot time used
-- • Cost per execution
"""
        
        return content
    
    def _generate_pr_description(
        self,
        optimization_data: Dict[str, Any],
        created_files: List[str]
    ) -> str:
        """Generate comprehensive PR description"""
        
        optimization_id = optimization_data["optimization_id"]
        estimated_savings = optimization_data.get("estimated_savings_usd", 0)
        savings_pct = optimization_data.get("estimated_savings_pct", 0)
        techniques = optimization_data.get("optimization_techniques", [])
        explanation = optimization_data.get("explanation", "")
        risk_level = optimization_data.get("risk_level", "UNKNOWN")
        
        description = f"""## 🚀 BigQuery Cost Optimization

**Estimated Monthly Savings: ${estimated_savings:.2f} ({savings_pct:.1f}% reduction)**

### 📊 Optimization Summary
This PR implements AI-powered query optimization that reduces BigQuery costs while maintaining query functionality and performance.

**Optimization ID**: `{optimization_id}`
**Risk Level**: {risk_level}
**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

### 🎯 Cost Impact
- **Current Monthly Cost**: ${estimated_savings / (savings_pct / 100):.2f}
- **Optimized Monthly Cost**: ${estimated_savings / (savings_pct / 100) - estimated_savings:.2f}
- **Monthly Savings**: ${estimated_savings:.2f}
- **Annual Savings**: ${estimated_savings * 12:.2f}

### ⚡ Optimization Techniques Applied
{chr(10).join(f'- **{technique.replace("_", " ").title()}**' for technique in techniques)}

### 📝 Technical Details
{explanation}

### 📁 Files Changed
{chr(10).join(f'- `{file}`' for file in created_files)}

### 🧪 Testing Requirements
- [ ] Run validation tests in `tests/optimization_validation/`
- [ ] Compare query results with original implementation
- [ ] Verify performance improvements in staging environment
- [ ] Monitor cost impact for 48 hours after deployment

### 🔒 Risk Assessment: {risk_level}
{self._get_risk_description(risk_level)}

### 🚀 Deployment Plan
1. **Development Testing**: Run validation tests and verify results
2. **Staging Deployment**: Deploy to staging with monitoring
3. **Production Rollout**: Deploy during maintenance window with rollback plan
4. **Monitoring**: Track cost savings and performance metrics

### 📊 Success Metrics
- Achieve {savings_pct:.1f}% cost reduction
- Maintain query execution time within ±10% of original
- Zero regression in downstream analytics and reports

### 🔄 Rollback Plan
If issues are detected:
1. Immediately revert to original query
2. Investigate and fix optimization issues
3. Re-test before re-deployment

---

🤖 **Generated by**: GCP Cost Optimization MCP Agent
📧 **Questions?**: Contact @data-engineering-team
"""
        
        return description
    
    async def _assign_reviewers(self, pr, optimization_data: Dict[str, Any]) -> List[str]:
        """Assign appropriate reviewers to the PR"""
        
        try:
            # Determine reviewers based on optimization characteristics
            reviewers = self.pr_config["default_reviewers"].copy()
            
            # Add specific reviewers based on risk level
            risk_level = optimization_data.get("risk_level", "LOW")
            if risk_level in ["HIGH", "CRITICAL"]:
                reviewers.extend(["senior-data-engineers", "data-platform-leads"])
            
            # Add reviewers based on savings amount
            estimated_savings = optimization_data.get("estimated_savings_usd", 0)
            if estimated_savings > 500:  # High-impact optimization
                reviewers.extend(["data-engineering-manager"])
            
            # Remove duplicates and filter existing team members
            unique_reviewers = list(set(reviewers))
            
            # Assign reviewers (GitHub API expects usernames, not team names)
            # In production, you'd map team names to actual usernames
            actual_reviewers = [r for r in unique_reviewers if not r.startswith('data-')]  # Filter team names
            
            if actual_reviewers:
                pr.create_review_request(reviewers=actual_reviewers)
            
            logger.info(f"Assigned reviewers: {unique_reviewers}")
            return unique_reviewers
            
        except Exception as e:
            logger.error(f"Failed to assign reviewers: {e}")
            return []
    
    async def _apply_labels(self, pr, optimization_data: Dict[str, Any]) -> List[str]:
        """Apply appropriate labels to the PR"""
        
        try:
            labels = self.pr_config["default_labels"].copy()
            
            # Add labels based on optimization characteristics
            estimated_savings = optimization_data.get("estimated_savings_usd", 0)
            risk_level = optimization_data.get("risk_level", "LOW")
            
            # Savings-based labels
            if estimated_savings > 500:
                labels.append("high-impact")
            elif estimated_savings > 100:
                labels.append("medium-impact")
            else:
                labels.append("low-impact")
            
            # Risk-based labels
            labels.append(f"risk-{risk_level.lower()}")
            
            # Technique-based labels
            techniques = optimization_data.get("optimization_techniques", [])
            if "select_star_replacement" in techniques:
                labels.append("column-optimization")
            if "partition_filter_addition" in techniques:
                labels.append("partition-optimization")
            
            # Apply labels
            pr.set_labels(*labels)
            
            logger.info(f"Applied labels: {labels}")
            return labels
            
        except Exception as e:
            logger.error(f"Failed to apply labels: {e}")
            return []
    
    def _get_risk_description(self, risk_level: str) -> str:
        """Get description for risk level"""
        
        descriptions = {
            "LOW": "This optimization uses standard BigQuery best practices with minimal risk of issues.",
            "MEDIUM": "This optimization includes structural changes that require validation testing.",
            "HIGH": "This optimization involves significant query changes and requires thorough testing and gradual rollout.",
            "CRITICAL": "This optimization requires senior engineer review and extensive validation before deployment."
        }
        
        return descriptions.get(risk_level, "Risk level assessment not available.")
    
    async def health_check(self) -> bool:
        """Verify GitHub integration is working"""
        try:
            if not self.github_client or not self.repository:
                return False
            
            # Test repository access
            _ = self.repository.get_contents("README.md")
            return True
            
        except Exception as e:
            logger.error(f"GitHub integration health check failed: {e}")
            return False
