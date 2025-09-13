#!/usr/bin/env python3
"""
Planner Utils - Helper classes for scope validation and requirement checking
Following ACTION_PLAN_PLANNER.md

Critical Implementation:
- Scope Viability: User must satisfy ALL 7 scope types when present
- Provider scope does not need to be fulfilled (user choice) 
- ALL scopes require EXACT match (no hierarchies)
- Missing scope constraint = solution applies universally for that dimension
- Missing user scope when constraint exists = immediate error
- Requirement Viability: User facts must be "have" for ALL required capabilities
"""

from typing import Dict, List, Any


class ScopeValidator:
    """
    Validates whether user scopes match AssessedClaim scope constraints.
    
    Scope Types (7 must match exactly, provider is optional):
    - state: Specific US state code only (e.g., "CA", "NY") 
    - nationality: Specific country code (e.g., "CH", "IN")
    - visa_type: Specific visa type (e.g., "H-1B", "L-1")
    - age: Standard brackets (under_18, 18_21, 21_65, 65_plus)
    - credit_score: Standard bands (no_credit, under_600, 600_700, 700_plus)
    - asset_band: Standard ranges (under_100k, 100k_1m, 1m_plus)  
    - previous_residence: Specific country code (e.g., "CH", "IN")
    - provider: Optional (user choice) - does NOT need to match
    """
    
    # Scope types that MUST match exactly (provider is optional)
    REQUIRED_SCOPE_TYPES = {
        "state", "nationality", "visa_type", "age", 
        "credit_score", "asset_band", "previous_residence"
    }
    
    OPTIONAL_SCOPE_TYPES = {"provider"}
    
    def __init__(self):
        print("✅ ScopeValidator initialized")
    
    def is_viable(self, user_scopes: Dict[str, str], claim_scopes: List[Dict[str, Any]]) -> bool:
        """
        Check if user scopes satisfy AssessedClaim scope constraints.
        
        Rules:
        1. For each claim scope constraint, user MUST have matching value
        2. Missing claim scope = applies universally (no constraint)
        3. Missing user scope when constraint exists = FAIL
        4. Provider scopes are optional (user choice)
        5. All matches must be EXACT (no hierarchies)
        
        Args:
            user_scopes: User's scope values {scope_type: value}
            claim_scopes: AssessedClaim's scope constraints [{"scope_type": str, "value": str, ...}]
            
        Returns:
            True if user satisfies all scope constraints, False otherwise
        """
        try:
            if not claim_scopes:
                # No scope constraints = applies universally
                return True
            
            for claim_scope in claim_scopes:
                scope_type = claim_scope.get("scope_type")
                required_value = claim_scope.get("value")
                
                if not scope_type or not required_value:
                    # Invalid scope data - skip
                    continue
                
                # Provider scopes are optional (user choice)
                if scope_type in self.OPTIONAL_SCOPE_TYPES:
                    continue
                
                # Required scope types must match exactly
                if scope_type in self.REQUIRED_SCOPE_TYPES:
                    user_value = user_scopes.get(scope_type)
                    
                    if not user_value:
                        # User missing required scope when constraint exists
                        print(f"❌ Scope validation failed: User missing {scope_type}, but claim requires '{required_value}'")
                        return False
                    
                    if user_value != required_value:
                        # Values don't match exactly
                        print(f"❌ Scope validation failed: User {scope_type}='{user_value}', but claim requires '{required_value}'")
                        return False
            
            # All scope constraints satisfied
            return True
            
        except Exception as e:
            print(f"❌ Scope validation error: {e}")
            return False
    
    def get_missing_scopes(self, user_scopes: Dict[str, str], claim_scopes: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """
        Get list of scope constraints that user doesn't satisfy.
        
        Args:
            user_scopes: User's scope values  
            claim_scopes: AssessedClaim's scope constraints
            
        Returns:
            List of missing scope constraints [{"scope_type": str, "required_value": str, "user_value": str}]
        """
        missing = []
        
        try:
            for claim_scope in claim_scopes:
                scope_type = claim_scope.get("scope_type")
                required_value = claim_scope.get("value")
                
                if not scope_type or not required_value:
                    continue
                
                # Skip optional scope types
                if scope_type in self.OPTIONAL_SCOPE_TYPES:
                    continue
                
                if scope_type in self.REQUIRED_SCOPE_TYPES:
                    user_value = user_scopes.get(scope_type)
                    
                    if not user_value or user_value != required_value:
                        missing.append({
                            "scope_type": scope_type,
                            "required_value": required_value,
                            "user_value": user_value or "missing"
                        })
            
        except Exception as e:
            print(f"⚠️  Error getting missing scopes: {e}")
        
        return missing


class RequirementChecker:
    """
    Checks whether user has the capabilities required by AssessedClaim logic trees.
    
    User Fact Values:
    - "have": User has this requirement satisfied
    - "need": User still needs to obtain this requirement  
    - "blocked": User cannot obtain this requirement
    
    Viability Rules:
    - User facts must be "have" for ALL required capabilities
    - "need" or "blocked" = prune solution entirely
    - Missing requirement tracking = immediate error (no fallbacks)
    """
    
    def __init__(self):
        print("✅ RequirementChecker initialized")
    
    def is_viable(self, user_facts: Dict[str, str], requirements: List[Dict[str, Any]]) -> bool:
        """
        Check if user has all required capabilities.
        
        Args:
            user_facts: User's requirement states {req_id: "have"|"need"|"blocked"}
            requirements: Required capabilities [{"id": str, "name": str, ...}]
            
        Returns:
            True if user has all requirements, False otherwise
            
        Raises:
            Exception: If required capability is not tracked in user facts
        """
        try:
            if not requirements:
                # No requirements = always viable
                return True
            
            for requirement in requirements:
                req_id = requirement.get("id")
                req_name = requirement.get("name", req_id)
                
                if not req_id:
                    # Invalid requirement data - skip
                    continue
                
                user_status = user_facts.get(req_id)
                
                if user_status is None:
                    # Missing requirement tracking - architecture violation
                    raise Exception(f"Required capability '{req_name}' ({req_id}) not tracked in user facts - system error")
                
                if user_status != "have":
                    # User doesn't have this requirement
                    print(f"❌ Requirement check failed: {req_name} status is '{user_status}', need 'have'")
                    return False
            
            # All requirements satisfied
            return True
            
        except Exception as e:
            print(f"❌ Requirement checking error: {e}")
            raise  # Re-raise to maintain "no fallbacks" rule
    
    def get_missing_requirements(self, user_facts: Dict[str, str], requirements: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """
        Get list of requirements that user doesn't have.
        
        Args:
            user_facts: User's requirement states
            requirements: Required capabilities
            
        Returns:
            List of missing requirements [{"req_id": str, "req_name": str, "status": str}]
        """
        missing = []
        
        try:
            for requirement in requirements:
                req_id = requirement.get("id")
                req_name = requirement.get("name", req_id)
                
                if not req_id:
                    continue
                
                user_status = user_facts.get(req_id, "unknown")
                
                if user_status != "have":
                    missing.append({
                        "req_id": req_id,
                        "req_name": req_name,
                        "status": user_status
                    })
        
        except Exception as e:
            print(f"⚠️  Error getting missing requirements: {e}")
        
        return missing
    
    def get_blocked_requirements(self, user_facts: Dict[str, str]) -> List[str]:
        """
        Get list of requirement IDs that user has marked as blocked.
        
        Args:
            user_facts: User's requirement states
            
        Returns:
            List of blocked requirement IDs
        """
        blocked = []
        
        try:
            for req_id, status in user_facts.items():
                if status == "blocked":
                    blocked.append(req_id)
        
        except Exception as e:
            print(f"⚠️  Error getting blocked requirements: {e}")
        
        return blocked


class PlannerValidationError(Exception):
    """Custom exception for planner validation errors."""
    pass


def test_scope_validator():
    """Test ScopeValidator with various scenarios."""
    print("\n🧪 Testing ScopeValidator...")
    
    validator = ScopeValidator()
    
    # Test user scopes
    user_scopes = {
        "state": "CA",
        "nationality": "CH", 
        "visa_type": "H-1B",
        "age": "21_65",
        "credit_score": "no_credit",
        "asset_band": "100k_1m",
        "previous_residence": "CH"
    }
    
    # Test Case 1: No scope constraints (should pass)
    assert validator.is_viable(user_scopes, []) == True
    print("✅ No constraints test passed")
    
    # Test Case 2: Matching scope constraints (should pass)
    matching_scopes = [
        {"scope_type": "state", "value": "CA", "name": "California"},
        {"scope_type": "visa_type", "value": "H-1B", "name": "H-1B Visa"}
    ]
    assert validator.is_viable(user_scopes, matching_scopes) == True
    print("✅ Matching scopes test passed")
    
    # Test Case 3: Non-matching scope constraints (should fail)  
    non_matching_scopes = [
        {"scope_type": "state", "value": "NY", "name": "New York"}
    ]
    assert validator.is_viable(user_scopes, non_matching_scopes) == False
    print("✅ Non-matching scopes test passed")
    
    # Test Case 4: Provider scope (should pass - optional)
    provider_scopes = [
        {"scope_type": "provider", "value": "Chase", "name": "Chase Bank"}
    ]
    assert validator.is_viable(user_scopes, provider_scopes) == True
    print("✅ Provider scope test passed")
    
    # Test Case 5: Missing user scope (should fail)
    incomplete_user_scopes = {"state": "CA"}  # Missing other required scopes
    missing_scope_constraint = [
        {"scope_type": "nationality", "value": "CH", "name": "Switzerland"}
    ]
    assert validator.is_viable(incomplete_user_scopes, missing_scope_constraint) == False
    print("✅ Missing user scope test passed")
    
    print("✅ ScopeValidator tests completed successfully")


def test_requirement_checker():
    """Test RequirementChecker with various scenarios."""
    print("\n🧪 Testing RequirementChecker...")
    
    checker = RequirementChecker()
    
    # Test user facts
    user_facts = {
        "req_ssn": "have",
        "req_address_proof": "have", 
        "req_passport": "have",
        "req_itin": "need",
        "req_credit_history": "blocked"
    }
    
    # Test Case 1: No requirements (should pass)
    assert checker.is_viable(user_facts, []) == True
    print("✅ No requirements test passed")
    
    # Test Case 2: All requirements satisfied (should pass)
    satisfied_requirements = [
        {"id": "req_ssn", "name": "Social Security Number"},
        {"id": "req_address_proof", "name": "Proof of Address"}
    ]
    assert checker.is_viable(user_facts, satisfied_requirements) == True
    print("✅ Satisfied requirements test passed")
    
    # Test Case 3: Some requirements not satisfied (should fail)
    unsatisfied_requirements = [
        {"id": "req_ssn", "name": "Social Security Number"},
        {"id": "req_itin", "name": "Individual Taxpayer ID"}  # Status is "need"
    ]
    assert checker.is_viable(user_facts, unsatisfied_requirements) == False
    print("✅ Unsatisfied requirements test passed")
    
    # Test Case 4: Blocked requirement (should fail)
    blocked_requirements = [
        {"id": "req_credit_history", "name": "Credit History"}  # Status is "blocked" 
    ]
    assert checker.is_viable(user_facts, blocked_requirements) == False
    print("✅ Blocked requirements test passed")
    
    # Test Case 5: Missing requirement tracking (should raise exception)
    try:
        untracked_requirements = [
            {"id": "req_unknown", "name": "Unknown Requirement"}
        ]
        checker.is_viable(user_facts, untracked_requirements)
        assert False, "Should have raised exception"
    except Exception as e:
        assert "not tracked in user facts" in str(e)
        print("✅ Missing requirement tracking test passed")
    
    print("✅ RequirementChecker tests completed successfully")


def test_planner_utils():
    """Run all planner utils tests."""
    print("🚀 Testing Planner Utils...")
    
    try:
        test_scope_validator()
        test_requirement_checker()
        print("\n✅ All planner utils tests passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Tests failed: {e}")
        return False


if __name__ == "__main__":
    success = test_planner_utils()
    print(f"\n{'✅ All tests passed!' if success else '❌ Tests failed!'}")