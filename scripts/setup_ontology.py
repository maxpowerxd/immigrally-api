#!/usr/bin/env python3
"""
STAGE 2: Create Ontology Constraints
KEEP IT SIMPLE - NO OVERENGINEERING
Direct implementation from scope_preload_spec.md
"""

from neo4j import GraphDatabase
import sys

# Neo4j connection details (from Stage 1)
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "testpassword")

def create_constraints_and_indexes(driver):
    """Create constraints and indexes from scope_preload_spec.md and Strategy requirements"""
    print("Creating constraints and indexes...")
    
    with driver.session() as session:
        # Create Scope id uniqueness constraint
        try:
            session.run("""
                CREATE CONSTRAINT scope_id_unique IF NOT EXISTS
                FOR (s:Scope) REQUIRE s.id IS UNIQUE
            """)
            print("✅ Created Scope id uniqueness constraint")
        except Exception as e:
            print(f"⚠️  Constraint may already exist: {e}")
        
        # Create index on scope_type and code
        try:
            session.run("""
                CREATE INDEX scope_type_code IF NOT EXISTS
                FOR (s:Scope) ON (s.scope_type, s.code)
            """)
            print("✅ Created index on scope_type and code")
        except Exception as e:
            print(f"⚠️  Index may already exist: {e}")
        
        # Create Strategy id uniqueness constraint
        try:
            session.run("""
                CREATE CONSTRAINT strategy_id_unique IF NOT EXISTS
                FOR (st:Strategy) REQUIRE st.id IS UNIQUE
            """)
            print("✅ Created Strategy id uniqueness constraint")
        except Exception as e:
            print(f"⚠️  Constraint may already exist: {e}")
        
        # Create AssessedStrategy id uniqueness constraint
        try:
            session.run("""
                CREATE CONSTRAINT assessed_strategy_id_unique IF NOT EXISTS
                FOR (ast:AssessedStrategy) REQUIRE ast.id IS UNIQUE
            """)
            print("✅ Created AssessedStrategy id uniqueness constraint")
        except Exception as e:
            print(f"⚠️  Constraint may already exist: {e}")
        
        # Create index on Strategy goal_context for efficient grouping
        try:
            session.run("""
                CREATE INDEX strategy_goal_context IF NOT EXISTS
                FOR (st:Strategy) ON (st.goal_context)
            """)
            print("✅ Created index on Strategy goal_context")
        except Exception as e:
            print(f"⚠️  Index may already exist: {e}")
        
        # Create index on AssessedStrategy goal_id for efficient querying
        try:
            session.run("""
                CREATE INDEX assessed_strategy_goal IF NOT EXISTS
                FOR (ast:AssessedStrategy) ON (ast.goal_id)
            """)
            print("✅ Created index on AssessedStrategy goal_id")
        except Exception as e:
            print(f"⚠️  Index may already exist: {e}")

def seed_minimal_scopes(driver):
    """Pre-load EXACTLY 5 minimal scope seeds for testing"""
    print("\nSeeding minimal test scopes...")
    
    test_scopes = [
        # 1. California state (specific state only)
        {
            "id": "Scope:state:CA",
            "scope_type": "state", 
            "name": "California",
            "code": "CA"
        },
        # 2. Texas state (another specific state) - NO parent relationships
        {
            "id": "Scope:state:TX",
            "scope_type": "state",
            "name": "Texas", 
            "code": "TX"
        },
        # 3. Chase provider
        {
            "id": "Scope:provider:CHASE",
            "scope_type": "provider",
            "name": "Chase",
            "code": "CHASE"
        },
        # 4. H-1B visa type  
        {
            "id": "Scope:visa_type:H-1B",
            "scope_type": "visa_type",
            "name": "H-1B Specialty Occupation",
            "code": "H-1B",
            "category": "nonimmigrant"
        },
        # 5. A test requirement (even though Requirement is different from Scope, 
        #    we'll create a simple Requirement node for testing)
        # Actually, let's create another Scope instead to stay consistent
        {
            "id": "Scope:nationality:IN",
            "scope_type": "nationality",
            "name": "India",
            "code": "IN"
        }
    ]
    
    with driver.session() as session:
        for scope in test_scopes:
            # Create the scope node
            parent = scope.pop("parent", None)
            category = scope.pop("category", None)
            
            query = """
                MERGE (s:Scope {id: $id})
                SET s.scope_type = $scope_type,
                    s.name = $name,
                    s.code = $code,
                    s.value = coalesce($code, s.value)
            """
            
            if category:
                query += ", s.category = $category"
                scope["category"] = category
            
            session.run(query, **scope)
            print(f"✅ Created Scope: {scope['name']} ({scope['id']})")
            
            # NO parent relationships in specific-only system
            # (parent field kept for backwards compatibility but ignored)
    
    # Also create one simple Requirement node for testing
    with driver.session() as session:
        session.run("""
            MERGE (r:Requirement {id: 'req_ssn'})
            SET r.name = 'Social Security Number',
                r.type = 'document'
        """)
        print("✅ Created test Requirement: SSN (req_ssn)")

def verify_setup(driver):
    """Verify constraints and seed nodes were created"""
    print("\n" + "="*50)
    print("VERIFICATION")
    print("="*50)
    
    with driver.session() as session:
        # Check constraints
        print("\n📋 Constraints:")
        constraints = session.run("SHOW CONSTRAINTS").data()
        for c in constraints:
            if any(label in str(c) for label in ['Scope', 'Strategy', 'AssessedStrategy']):
                print(f"  - {c.get('name', 'unnamed')}: {c.get('labelsOrTypes', [])} {c.get('properties', [])}")
        
        # Check indexes
        print("\n📋 Indexes:")
        indexes = session.run("SHOW INDEXES").data()
        for idx in indexes:
            if any(label in str(idx) for label in ['Scope', 'Strategy', 'AssessedStrategy']):
                print(f"  - {idx.get('name', 'unnamed')}: {idx.get('labelsOrTypes', [])} {idx.get('properties', [])}")
        
        # Count and list nodes
        print("\n📋 Seed Nodes:")
        result = session.run("""
            MATCH (s:Scope)
            RETURN s.id as id, s.scope_type as type, s.name as name, s.code as code
            ORDER BY s.scope_type, s.code
        """)
        
        count = 0
        for record in result:
            count += 1
            print(f"  {count}. {record['type']}: {record['name']} (id={record['id']}, code={record['code']})")
        
        print(f"\nTotal Scope nodes: {count}")
        
        # Check Requirement node
        req_result = session.run("""
            MATCH (r:Requirement)
            RETURN r.id as id, r.name as name, r.type as type
        """)
        for record in req_result:
            print(f"\nRequirement node: {record['name']} (id={record['id']}, type={record['type']})")
        
        # Check Strategy nodes
        strategy_result = session.run("""
            MATCH (s:Strategy)
            RETURN s.id as id, s.goal_context as goal_context, s.source as source
            ORDER BY s.goal_context
        """)
        strategy_count = 0
        for record in strategy_result:
            strategy_count += 1
            print(f"\nStrategy node {strategy_count}: {record['goal_context']} (id={record['id']}, source={record['source']})")
        
        if strategy_count == 0:
            print(f"\nNo Strategy nodes found (expected - created during extraction)")
            
        # Check AssessedStrategy nodes  
        assessed_strategy_result = session.run("""
            MATCH (as:AssessedStrategy)
            RETURN as.id as id, as.goal_id as goal_id, as.source_type as source_type, as.confidence as confidence
            ORDER BY as.goal_id
        """)
        assessed_strategy_count = 0
        for record in assessed_strategy_result:
            assessed_strategy_count += 1
            print(f"\nAssessedStrategy node {assessed_strategy_count}: Goal {record['goal_id']} (id={record['id']}, type={record['source_type']}, confidence={record['confidence']})")
            
        if assessed_strategy_count == 0:
            print(f"\nNo AssessedStrategy nodes found (expected - created during deduplication)")
        
        # Check parent relationship
        print("\n📋 Relationships:")
        rel_result = session.run("""
            MATCH (child:Scope)-[:parent]->(parent:Scope)
            RETURN child.name as child, parent.name as parent
        """)
        for record in rel_result:
            print(f"  - {record['child']} --parent--> {record['parent']}")

def main():
    """Main execution for Stage 2"""
    print("STAGE 2: Create Ontology Constraints")
    print("="*50)
    print("KEEP IT SIMPLE - NO OVERENGINEERING")
    print("="*50 + "\n")
    
    try:
        # Connect to Neo4j
        driver = GraphDatabase.driver(URI, auth=AUTH)
        driver.verify_connectivity()
        print("✅ Connected to Neo4j")
        
        # Execute Stage 2 tasks
        create_constraints_and_indexes(driver)
        seed_minimal_scopes(driver)
        verify_setup(driver)
        
        print("\n" + "="*50)
        print("STAGE 2 COMPLETE")
        print("="*50)
        print("\n⏸️  STOP - AWAIT USER APPROVAL BEFORE PROCEEDING")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        sys.exit(1)
    finally:
        if 'driver' in locals():
            driver.close()

if __name__ == "__main__":
    main()