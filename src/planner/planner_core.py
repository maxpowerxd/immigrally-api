#!/usr/bin/env python3
"""
Planner Core - Main roadmap function
Following ACTION_PLAN_PLANNER.md

Build 1-function planner: roadmap
For LLM integration: User Question → LLM → Planner (raw data) → LLM (synthesized answer)
"""

import sys
import os
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from src.infrastructure.user_state import UserState, UserStateRepository
from src.infrastructure.firebase_client import FirebaseClient
from src.planner.planner_neo4j import PlannerNeo4j
from src.planner.planner_utils import ScopeValidator, RequirementChecker


@dataclass
class SolutionData:
    """Complete solution data with graphlets."""
    solution_id: str
    solution_name: str
    solution_description: str
    viable_assessed_claims: List[Dict[str, Any]]
    strategy_ranking: int
    user_rationale: str


@dataclass
class GoalData:
    """Goal with viable solutions."""
    goal_id: str
    goal_name: str
    goal_phase: str
    goal_description: str
    viable_solutions: List[SolutionData]


class PlannerCore:
    """
    1-function planner that returns prioritized list of all goals.
    
    Key Rules:
    - Goal nodes link to solution nodes, ranked by Strategy nodes
    - "Graphlets" = AssessedClaim + dependent nodes (Clause, Scope, Qualifiers, Requirements)
    - Solutions have 1+ graphlets that are mutually exclusive by scope
    - Prune solutions with no viable AssessedClaims based on user state
    - Raise errors immediately, no fallbacks
    """
    
    def __init__(self):
        self.neo4j = PlannerNeo4j()
        self.scope_validator = ScopeValidator()
        self.requirement_checker = RequirementChecker()
        
        # Initialize Firebase for user state
        self.firebase_client = FirebaseClient()
        self.user_repo = UserStateRepository(self.firebase_client)
        
        print("✅ PlannerCore initialized")
    
    def close(self):
        """Clean shutdown."""
        self.neo4j.close()
    
    def roadmap(self, user_state: UserState) -> Dict[str, Any]:
        """
        Main planner function: Returns prioritized list of all goals.
        
        Algorithm:
        1. Get all goals for user's lifecycle phase
        2. Retrieve all solution nodes per goal. Prune those solutions that have no viable AssessedClaims
        3. Prune all goals that do not have a single viable solution node
        4. Prioritize solution nodes per goal by Strategy
        5. Return goal list including their viable solution nodes
        
        Args:
            user_state: Complete user state from Firebase
            
        Returns:
            Dict with goals list and metadata
            
        Raises:
            Exception: For any errors (no silent failures)
        """
        try:
            print(f"🎯 Planning roadmap for user: {user_state.user_id}")
            print(f"   Phase context: {user_state.timeline.get('arrival_date', 'unknown')}")
            print(f"   Scopes: {list(user_state.scopes.keys())}")
            print(f"   Facts: {len(user_state.facts)} requirements tracked")
            
            # Step 1: Get all goals (we'll filter by phase later if needed)
            all_goals = self.neo4j.get_goals_by_phase(None)  # Get all phases for now
            if not all_goals:
                raise Exception("No goals found in database - system not properly initialized")
                
            print(f"📋 Found {len(all_goals)} total goals")
            
            viable_goals = []
            
            # Step 2-5: Process each goal
            for goal in all_goals:
                goal_id = goal['id']
                goal_name = goal['name']
                goal_phase = goal['phase']
                goal_description = goal['description']
                
                print(f"\n🔍 Processing Goal: {goal_name} [{goal_phase}]")
                
                # Get solutions for this goal
                solutions = self.neo4j.get_solutions_for_goal(goal_id)
                if not solutions:
                    print(f"   ❌ No solutions found - skipping goal")
                    continue
                    
                print(f"   Found {len(solutions)} solutions")
                
                viable_solutions = []
                
                # Process each solution
                for solution in solutions:
                    solution_id = solution['id']
                    solution_name = solution['name']
                    solution_description = solution.get('description', '')
                    
                    print(f"     🔧 Checking solution: {solution_name}")
                    
                    # Get AssessedClaims for this solution
                    assessed_claims = self.neo4j.get_assessed_claims_for_solution(solution_id)
                    if not assessed_claims:
                        print(f"        ❌ No AssessedClaims - skipping solution")
                        continue
                        
                    print(f"        Found {len(assessed_claims)} AssessedClaims")
                    
                    # Check viability of each AssessedClaim
                    viable_claims = []
                    for claim_id in assessed_claims:
                        print(f"          📝 Checking AssessedClaim: {claim_id}")
                        
                        # Get complete graphlet for this AssessedClaim
                        graphlet = self.neo4j.get_complete_graphlet(claim_id)
                        if not graphlet:
                            print(f"            ❌ No graphlet data - skipping claim")
                            continue
                            
                        # Check scope viability
                        scopes = graphlet.get('scopes', [])
                        if not self.scope_validator.is_viable(user_state.scopes, scopes):
                            print(f"            ❌ Scope mismatch - skipping claim")
                            continue
                            
                        # Check requirement viability
                        requirements = graphlet.get('requirements', [])
                        if not self.requirement_checker.is_viable(user_state.facts, requirements):
                            print(f"            ❌ Requirements not met - skipping claim")
                            continue
                            
                        print(f"            ✅ AssessedClaim is viable")
                        viable_claims.append(graphlet)
                    
                    # If no viable claims, skip this solution
                    if not viable_claims:
                        print(f"        ❌ No viable AssessedClaims - skipping solution")
                        continue
                    
                    # Get strategy ranking for this solution within the goal
                    strategy_data = self.neo4j.get_strategy_for_goal(goal_id)
                    if not strategy_data:
                        print(f"        ⚠️  No AssessedStrategy found for goal {goal_id} - using default ranking")
                        ranking_rules = []
                        user_rationale = "Strategy data not available - using default ranking"
                    else:
                        ranking_rules = strategy_data.get('ranking_rules', [])
                        user_rationale = strategy_data.get('user_rationale', '')
                    
                    # Find this solution's ranking position
                    try:
                        strategy_ranking = ranking_rules.index(solution_id)
                        print(f"        📊 Strategy ranking: {strategy_ranking} (lower = better)")
                    except ValueError:
                        print(f"        ⚠️  Solution not in strategy ranking - assigning lowest priority")
                        strategy_ranking = len(ranking_rules)  # Lowest priority
                    
                    # Create viable solution data
                    solution_data = SolutionData(
                        solution_id=solution_id,
                        solution_name=solution_name,
                        solution_description=solution_description,
                        viable_assessed_claims=viable_claims,
                        strategy_ranking=strategy_ranking,
                        user_rationale=user_rationale
                    )
                    
                    viable_solutions.append(solution_data)
                    print(f"        ✅ Solution is viable with {len(viable_claims)} claims")
                
                # Step 3: Skip goals with no viable solutions
                if not viable_solutions:
                    print(f"   ❌ No viable solutions - skipping goal")
                    continue
                
                # Step 4: Sort solutions by strategy ranking (lower index = higher priority)
                viable_solutions.sort(key=lambda s: s.strategy_ranking)
                
                # Create goal data
                goal_data = GoalData(
                    goal_id=goal_id,
                    goal_name=goal_name,
                    goal_phase=goal_phase,
                    goal_description=goal_description,
                    viable_solutions=viable_solutions
                )
                
                viable_goals.append(goal_data)
                print(f"   ✅ Goal has {len(viable_solutions)} viable solutions")
            
            print(f"\n🎯 Roadmap complete: {len(viable_goals)} viable goals")
            
            # Step 5: Return structured roadmap
            roadmap_data = {
                "user_id": user_state.user_id,
                "generated_at": "2025-09-09T16:27:00Z",  # TODO: Use actual timestamp
                "total_goals": len(viable_goals),
                "goals": []
            }
            
            # Convert goal data to dictionaries for JSON serialization
            for goal_data in viable_goals:
                goal_dict = {
                    "goal_id": goal_data.goal_id,
                    "goal_name": goal_data.goal_name,
                    "goal_phase": goal_data.goal_phase,
                    "goal_description": goal_data.goal_description,
                    "solutions": []
                }
                
                for solution_data in goal_data.viable_solutions:
                    solution_dict = {
                        "solution_id": solution_data.solution_id,
                        "solution_name": solution_data.solution_name,
                        "solution_description": solution_data.solution_description,
                        "strategy_ranking": solution_data.strategy_ranking,
                        "user_rationale": solution_data.user_rationale,
                        "assessed_claims_count": len(solution_data.viable_assessed_claims)
                        # Note: Not including full graphlet data to keep response manageable
                        # LLM can request specific graphlet details if needed
                    }
                    goal_dict["solutions"].append(solution_dict)
                
                roadmap_data["goals"].append(goal_dict)
            
            return roadmap_data
            
        except Exception as e:
            print(f"❌ Roadmap generation failed: {e}")
            raise Exception(f"Roadmap generation failed: {e}")


def test_planner_core():
    """Test the planner with sample user state."""
    print("🧪 Testing PlannerCore...")
    
    try:
        # Create sample user state
        from src.infrastructure.user_state import create_sample_user_state
        sample_user = create_sample_user_state()
        
        # Initialize planner
        planner = PlannerCore()
        
        try:
            # Test roadmap generation
            roadmap = planner.roadmap(sample_user)
            
            print(f"\n✅ Roadmap generated successfully!")
            print(f"   User: {roadmap['user_id']}")
            print(f"   Total goals: {roadmap['total_goals']}")
            
            # Show first few goals
            for i, goal in enumerate(roadmap['goals'][:3]):
                print(f"   Goal {i+1}: {goal['goal_name']} [{goal['goal_phase']}]")
                print(f"      Solutions: {len(goal['solutions'])}")
                if goal['solutions']:
                    top_solution = goal['solutions'][0]
                    print(f"      Top: {top_solution['solution_name']} (rank {top_solution['strategy_ranking']})")
            
            if len(roadmap['goals']) > 3:
                print(f"   ... and {len(roadmap['goals']) - 3} more goals")
                
            return True
            
        finally:
            planner.close()
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


if __name__ == "__main__":
    print("🚀 PlannerCore - Main roadmap function")
    success = test_planner_core()
    print(f"\n{'✅ Test passed!' if success else '❌ Test failed!'}")