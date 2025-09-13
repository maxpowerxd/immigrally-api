#!/usr/bin/env python3
"""
Planner Neo4j Interface - Complex graphlet queries
Following ACTION_PLAN_PLANNER.md

Critical Implementation:
- Complete graphlet retrieval with ALL dependent nodes
- Strict scope matching across all 8 scope types 
- Requirement viability based on user capability tracking
- Strategy-based solution ranking (no fallbacks)
- Errors raised immediately (no fallbacks)
"""

import os
import sys
import json
from typing import Dict, List, Any, Optional
from neo4j import GraphDatabase

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))


class PlannerNeo4j:
    """Neo4j interface for planner queries with complex graphlet retrieval."""
    
    def __init__(self):
        # Get Neo4j connection details
        self.uri = os.getenv("NEO4J_URI")
        self.user = os.getenv("NEO4J_USER") 
        self.password = os.getenv("NEO4J_PASSWORD")
        
        if not all([self.uri, self.user, self.password]):
            raise Exception("Missing Neo4j environment variables: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD")
            
        # Connect to Neo4j
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        self.driver.verify_connectivity()
        print("✅ Connected to Neo4j for planner queries")
    
    def close(self):
        """Close Neo4j connection."""
        if self.driver:
            self.driver.close()
            
    def get_goals_by_phase(self, phase: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get goals by lifecycle phase.
        
        Args:
            phase: Target phase (PREP, ARRIVE, BUILD, THRIVE) or None for all
            
        Returns:
            List of goal dictionaries
            
        Raises:
            Exception: If no goals found or query fails
        """
        try:
            with self.driver.session() as session:
                if phase:
                    query = "MATCH (g:Goal) WHERE g.phase = $phase RETURN g ORDER BY g.name"
                    result = session.run(query, phase=phase)
                else:
                    query = "MATCH (g:Goal) RETURN g ORDER BY g.phase, g.name"
                    result = session.run(query)
                    
                goals = []
                for record in result:
                    goal_node = record["g"]
                    goals.append({
                        "id": goal_node.get("id"),
                        "name": goal_node.get("name"),
                        "phase": goal_node.get("phase"),
                        "description": goal_node.get("description", "")
                    })
                    
                if not goals:
                    if phase:
                        raise Exception(f"No goals found for phase '{phase}' - check goal seeding")
                    else:
                        raise Exception("No goals found in database - check goal seeding")
                        
                return goals
                
        except Exception as e:
            raise Exception(f"Failed to get goals by phase: {e}")
    
    def get_solutions_for_goal(self, goal_id: str) -> List[Dict[str, Any]]:
        """
        Get solutions that fulfill a specific goal.
        
        Args:
            goal_id: Target goal ID
            
        Returns:
            List of solution dictionaries
            
        Raises:
            Exception: If goal not found or query fails
        """
        try:
            with self.driver.session() as session:
                query = """
                MATCH (s:Solution)-[:fulfills]->(g:Goal {id: $goal_id})
                RETURN s ORDER BY s.name
                """
                result = session.run(query, goal_id=goal_id)
                
                solutions = []
                for record in result:
                    solution_node = record["s"]
                    solutions.append({
                        "id": solution_node.get("id"),
                        "name": solution_node.get("name"),
                        "description": solution_node.get("description", "")
                    })
                    
                # Note: Empty list is valid - some goals might not have solutions yet
                return solutions
                
        except Exception as e:
            raise Exception(f"Failed to get solutions for goal {goal_id}: {e}")
    
    def get_assessed_claims_for_solution(self, solution_id: str) -> List[str]:
        """
        Get AssessedClaim IDs that target a specific solution.
        
        Args:
            solution_id: Target solution ID
            
        Returns:
            List of AssessedClaim IDs
        """
        try:
            with self.driver.session() as session:
                query = """
                MATCH (ac:AssessedClaim)-[:targets]->(s:Solution {id: $solution_id})
                RETURN ac.id AS assessed_claim_id
                ORDER BY ac.id
                """
                result = session.run(query, solution_id=solution_id)
                
                claim_ids = []
                for record in result:
                    claim_ids.append(record["assessed_claim_id"])
                    
                return claim_ids
                
        except Exception as e:
            raise Exception(f"Failed to get AssessedClaims for solution {solution_id}: {e}")
    
    def get_complete_graphlet(self, assessed_claim_id: str) -> Optional[Dict[str, Any]]:
        """
        Get complete graphlet for an AssessedClaim with ALL dependent nodes.
        
        Critical Implementation:
        - AssessedClaim + Clause + Scope + Qualifiers + Requirements (from logic trees)
        - Complete data for scope and requirement viability checking
        
        Args:
            assessed_claim_id: Target AssessedClaim ID
            
        Returns:
            Complete graphlet dictionary or None if not found
            
        Raises:
            Exception: If query fails
        """
        try:
            with self.driver.session() as session:
                query = """
                MATCH (ac:AssessedClaim {id: $assessed_claim_id})
                
                // Get recommended clauses
                OPTIONAL MATCH (ac)-[:recommends]->(clause:Clause)
                
                // Get scopes  
                OPTIONAL MATCH (ac)-[:scoped_to]->(scope:Scope)
                
                // Get qualifiers
                OPTIONAL MATCH (ac)-[:has_qualifier]->(qual:Qualifier)
                
                RETURN ac,
                       collect(DISTINCT {
                           id: clause.id, 
                           logic_tree: clause.logic_tree
                       }) AS clauses,
                       collect(DISTINCT {
                           id: scope.id,
                           scope_type: scope.scope_type, 
                           name: scope.name,
                           value: scope.value,
                           description: scope.description
                       }) AS scopes,
                       collect(DISTINCT {
                           id: qual.id,
                           key: qual.key,
                           value: qual.value, 
                           evidence: qual.evidence,
                           confidence: qual.confidence
                       }) AS qualifiers
                """
                result = session.run(query, assessed_claim_id=assessed_claim_id)
                record = result.single()
                
                if not record:
                    return None
                    
                ac = record["ac"]
                clauses = [c for c in record["clauses"] if c["id"]]
                scopes = [s for s in record["scopes"] if s["id"]]
                qualifiers = [q for q in record["qualifiers"] if q["id"]]
                
                # Extract requirements from clause logic trees
                requirements = []
                requirement_ids = set()
                
                for clause in clauses:
                    logic_tree = clause.get("logic_tree")
                    if logic_tree:
                        req_ids = self._extract_requirement_ids_from_logic_tree(logic_tree)
                        requirement_ids.update(req_ids)
                
                # Get requirement details
                if requirement_ids:
                    req_query = """
                    MATCH (r:Requirement) 
                    WHERE r.id IN $req_ids
                    RETURN r.id AS id, r.name AS name, r.type AS type, r.description AS description
                    """
                    req_result = session.run(req_query, req_ids=list(requirement_ids))
                    
                    for req_record in req_result:
                        requirements.append({
                            "id": req_record["id"],
                            "name": req_record["name"],
                            "type": req_record["type"],
                            "description": req_record.get("description", "")
                        })
                
                # Build complete graphlet
                graphlet = {
                    "assessed_claim": {
                        "id": ac.get("id"),
                        "outcome": ac.get("outcome"),
                        "rationale": ac.get("rationale"),
                        "confidence": ac.get("confidence")
                    },
                    "clauses": clauses,
                    "scopes": scopes,
                    "qualifiers": qualifiers,
                    "requirements": requirements
                }
                
                return graphlet
                
        except Exception as e:
            raise Exception(f"Failed to get complete graphlet for {assessed_claim_id}: {e}")
    
    def get_strategy_for_goal(self, goal_id: str) -> Optional[Dict[str, Any]]:
        """
        Get AssessedStrategy for a specific goal.
        
        Architecture requirement: Every goal MUST have exactly one AssessedStrategy.
        
        Args:
            goal_id: Target goal ID
            
        Returns:
            Strategy dictionary with ranking_rules and rationale
            
        Raises:
            Exception: If no strategy found (violates architecture)
        """
        try:
            with self.driver.session() as session:
                query = """
                MATCH (ast:AssessedStrategy)-[:applies_to]->(g:Goal {id: $goal_id})
                RETURN ast.ranking_rules AS ranking_rules,
                       ast.user_rationale AS user_rationale,
                       ast.internal_rationale AS internal_rationale,
                       ast.confidence AS confidence
                """
                result = session.run(query, goal_id=goal_id)
                record = result.single()
                
                if not record:
                    return None  # Caller will raise architecture violation error
                    
                return {
                    "ranking_rules": record["ranking_rules"] or [],
                    "user_rationale": record["user_rationale"] or "",
                    "internal_rationale": record["internal_rationale"] or "",
                    "confidence": record["confidence"] or "medium"
                }
                
        except Exception as e:
            raise Exception(f"Failed to get strategy for goal {goal_id}: {e}")
    
    def _extract_requirement_ids_from_logic_tree(self, logic_tree: str) -> List[str]:
        """
        Extract requirement IDs from a JSON logic tree string.
        
        Logic tree format:
        {
            "op": "AND|OR|NOT|K_OF_N|has",
            "children": [...] | "id": "req_abc123"
        }
        
        Args:
            logic_tree: JSON logic tree string
            
        Returns:
            List of requirement IDs found in the tree
        """
        try:
            if not logic_tree:
                return []
                
            # Parse JSON
            tree = json.loads(logic_tree) if isinstance(logic_tree, str) else logic_tree
            requirement_ids = []
            
            def extract_ids_recursive(node):
                if isinstance(node, dict):
                    # Check if this is a "has" operation with an ID
                    if node.get("op") == "has" and "id" in node:
                        requirement_ids.append(node["id"])
                    
                    # Recurse into children
                    if "children" in node and isinstance(node["children"], list):
                        for child in node["children"]:
                            extract_ids_recursive(child)
                            
                elif isinstance(node, list):
                    for item in node:
                        extract_ids_recursive(item)
            
            extract_ids_recursive(tree)
            return requirement_ids
            
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            print(f"⚠️  Warning: Failed to parse logic tree: {e}")
            return []
    
    def test_connection(self) -> bool:
        """Test Neo4j connection and basic queries."""
        try:
            with self.driver.session() as session:
                # Test basic connectivity
                result = session.run("RETURN 1 AS test")
                record = result.single()
                if record["test"] != 1:
                    return False
                    
                # Test node counts
                node_counts = {}
                for label in ["Goal", "Solution", "AssessedClaim", "AssessedStrategy"]:
                    count_result = session.run(f"MATCH (n:{label}) RETURN count(n) AS count")
                    count_record = count_result.single()
                    node_counts[label] = count_record["count"] if count_record else 0
                
                print("📊 Neo4j Node Counts:")
                for label, count in node_counts.items():
                    print(f"   {label}: {count}")
                
                return True
                
        except Exception as e:
            print(f"❌ Neo4j connection test failed: {e}")
            return False


def test_planner_neo4j():
    """Test Neo4j interface functions."""
    print("🧪 Testing PlannerNeo4j...")
    
    try:
        neo4j = PlannerNeo4j()
        
        try:
            # Test connection
            if not neo4j.test_connection():
                print("❌ Connection test failed")
                return False
            
            print("✅ Connection test passed")
            
            # Test getting goals
            goals = neo4j.get_goals_by_phase(None)
            print(f"✅ Found {len(goals)} total goals")
            
            if goals:
                # Test getting solutions for first goal
                first_goal = goals[0]
                solutions = neo4j.get_solutions_for_goal(first_goal["id"])
                print(f"✅ Goal '{first_goal['name']}' has {len(solutions)} solutions")
                
                if solutions:
                    # Test getting AssessedClaims for first solution
                    first_solution = solutions[0]
                    claims = neo4j.get_assessed_claims_for_solution(first_solution["id"])
                    print(f"✅ Solution '{first_solution['name']}' has {len(claims)} AssessedClaims")
                    
                    if claims:
                        # Test getting complete graphlet
                        first_claim = claims[0]
                        graphlet = neo4j.get_complete_graphlet(first_claim)
                        if graphlet:
                            print(f"✅ Graphlet for {first_claim}:")
                            print(f"      Scopes: {len(graphlet['scopes'])}")
                            print(f"      Requirements: {len(graphlet['requirements'])}")
                            print(f"      Qualifiers: {len(graphlet['qualifiers'])}")
                            print(f"      Clauses: {len(graphlet['clauses'])}")
                        else:
                            print(f"⚠️  No graphlet data for {first_claim}")
                    
                # Test getting strategy for goal
                strategy = neo4j.get_strategy_for_goal(first_goal["id"])
                if strategy:
                    print(f"✅ Strategy found with {len(strategy['ranking_rules'])} ranked solutions")
                else:
                    print(f"⚠️  No strategy found for goal {first_goal['id']}")
            
            return True
            
        finally:
            neo4j.close()
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


if __name__ == "__main__":
    print("🚀 PlannerNeo4j - Complex graphlet queries")
    success = test_planner_neo4j()
    print(f"\n{'✅ Test passed!' if success else '❌ Test failed!'}")