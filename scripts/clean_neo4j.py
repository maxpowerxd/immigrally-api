#!/usr/bin/env python3
"""
Neo4j Database Cleaner
KEEP IT SIMPLE - NO OVERENGINEERING

This script cleans the Neo4j database for development iterations.
Removes all nodes and relationships while preserving constraints and indexes.

Usage:
    python clean_neo4j.py                    # Clean everything
    python clean_neo4j.py --keep-catalog     # Keep catalog nodes (Solution/Requirement with catalog=true)
    python clean_neo4j.py --extraction-only  # Keep only catalog, remove all extraction runs
"""

import os
import sys
import argparse
from neo4j import GraphDatabase

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, use system environment variables

# Neo4j connection details
URI = os.getenv("NEO4J_URI")
AUTH = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))

def clean_all_data(driver):
    """Remove all nodes and relationships"""
    print("🧹 Cleaning all data from Neo4j...")
    
    with driver.session() as session:
        # Delete all relationships first
        result = session.run("MATCH ()-[r]->() DELETE r")
        print("   ✅ Deleted all relationships")
        
        # Delete all nodes (including ExtractionRun nodes)
        result = session.run("MATCH (n) DELETE n")
        print("   ✅ Deleted all nodes")
        
        # Show final count
        result = session.run("MATCH (n) RETURN count(n) as count")
        count = result.single()['count']
        print(f"   📊 Remaining nodes: {count}")

def clean_extraction_runs_only(driver):
    """Remove only extraction runs, keep catalog data"""
    print("🧹 Cleaning extraction runs (keeping catalog data)...")
    
    with driver.session() as session:
        # Delete all relationships from nodes with extraction_run property
        result = session.run("""
            MATCH (a)-[r]->(b)
            WHERE a.extraction_run IS NOT NULL OR b.extraction_run IS NOT NULL
            DELETE r
        """)
        print(f"   ✅ Deleted extraction run relationships")
        
        # Delete all nodes with extraction_run property
        result = session.run("""
            MATCH (n)
            WHERE n.extraction_run IS NOT NULL
            DELETE n
        """)
        print(f"   ✅ Deleted nodes with extraction_run property")
        
        # Delete all ExtractionRun nodes themselves
        result = session.run("""
            MATCH (n:ExtractionRun)
            DELETE n
        """)
        print(f"   ✅ Deleted ExtractionRun tracking nodes")
        
        # Show remaining counts
        result = session.run("MATCH (n) RETURN count(n) as count")
        total_count = result.single()['count']
        
        result = session.run("MATCH (n {catalog: true}) RETURN count(n) as count")
        catalog_count = result.single()['count']
        
        print(f"   📊 Total remaining nodes: {total_count}")
        print(f"   📊 Catalog nodes: {catalog_count}")

def keep_catalog_only(driver):
    """Remove all data except catalog nodes"""
    print("🧹 Cleaning all data except catalog nodes...")
    
    with driver.session() as session:
        # Delete all relationships first
        result = session.run("MATCH ()-[r]->() DELETE r")
        print("   ✅ Deleted all relationships")
        
        # Delete all non-catalog nodes
        result = session.run("""
            MATCH (n)
            WHERE n.catalog IS NULL OR n.catalog <> true
            DELETE n
        """)
        print("   ✅ Deleted all non-catalog nodes")
        
        # Show remaining counts
        result = session.run("MATCH (n) RETURN count(n) as count")
        total_count = result.single()['count']
        
        result = session.run("MATCH (n {catalog: true}) RETURN count(n) as count")
        catalog_count = result.single()['count']
        
        print(f"   📊 Total remaining nodes: {total_count}")
        print(f"   📊 Catalog nodes: {catalog_count}")

def show_database_status(driver):
    """Show current database status"""
    print("\n📊 Current Database Status:")
    print("=" * 40)
    
    with driver.session() as session:
        # Count nodes by type
        result = session.run("""
            MATCH (n)
            WITH labels(n)[0] as node_type, count(*) as count
            RETURN node_type, count
            ORDER BY node_type
        """)
        
        print("📋 Nodes by type:")
        total_nodes = 0
        for record in result:
            node_type = record['node_type']
            count = record['count']
            total_nodes += count
            print(f"   {node_type}: {count}")
        print(f"   Total: {total_nodes}")
        
        # Count relationships
        result = session.run("MATCH ()-[r]->() RETURN count(r) as count")
        rel_count = result.single()['count']
        print(f"\n🔗 Total relationships: {rel_count}")
        
        # Show extraction runs
        result = session.run("""
            MATCH (n)
            WHERE n.extraction_run IS NOT NULL
            WITH n.extraction_run as run_id, count(*) as node_count
            RETURN run_id, node_count
            ORDER BY run_id
        """)
        
        extractions = list(result)
        if extractions:
            print(f"\n🔍 Extraction runs:")
            for record in extractions:
                print(f"   {record['run_id']}: {record['node_count']} nodes")
        else:
            print(f"\n🔍 No extraction runs found")
        
        # Show catalog status
        result = session.run("MATCH (n {catalog: true}) RETURN count(n) as count")
        catalog_count = result.single()['count']
        print(f"\n📚 Catalog nodes: {catalog_count}")

def main():
    parser = argparse.ArgumentParser(description="Clean Neo4j database for development iterations")
    parser.add_argument("--keep-catalog", action="store_true", 
                       help="Keep catalog nodes (Solution/Requirement with catalog=true)")
    parser.add_argument("--extraction-only", action="store_true",
                       help="Remove only extraction runs, keep catalog and other data")
    parser.add_argument("--status", action="store_true",
                       help="Show database status without cleaning")
    
    args = parser.parse_args()
    
    print("Neo4j Database Cleaner")
    print("=" * 30)
    print("KEEP IT SIMPLE - NO OVERENGINEERING")
    print("=" * 30 + "\n")
    
    # Fail fast if any required environment variables are missing
    required_env_vars = {
        'NEO4J_URI': 'Neo4j database connection URI',
        'NEO4J_USER': 'Neo4j database username',
        'NEO4J_PASSWORD': 'Neo4j database password'
    }
    
    missing_vars = []
    for var, description in required_env_vars.items():
        if not os.getenv(var):
            missing_vars.append(f"   {var}: {description}")
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(var)
        print("\nPlease set these in your .env file or environment.")
        sys.exit(1)
    
    try:
        # Connect to Neo4j
        driver = GraphDatabase.driver(URI, auth=AUTH)
        driver.verify_connectivity()
        print("✅ Connected to Neo4j")
        
        # Show current status
        show_database_status(driver)
        
        if args.status:
            print("\n✅ Status check complete")
            return
        
        # Show what will be cleaned
        if args.keep_catalog:
            print(f"\n🧹 Proceeding to delete ALL data except catalog nodes")
            print("   Catalog nodes (catalog=true) will be preserved.")
        elif args.extraction_only:
            print(f"\n🧹 Proceeding to delete ALL extraction runs")
            print("   Catalog nodes and other data will be preserved.")
        else:
            print(f"\n🧹 Proceeding to delete ALL data from Neo4j")
            print("   All nodes, relationships, and data will be removed.")
        
        # Perform cleaning
        if args.keep_catalog:
            keep_catalog_only(driver)
        elif args.extraction_only:
            clean_extraction_runs_only(driver)
        else:
            clean_all_data(driver)
        
        # Show final status
        show_database_status(driver)
        
        print("\n" + "=" * 30)
        print("✅ CLEANING COMPLETE")
        print("=" * 30)
        print("\n💡 Next steps:")
        print("   1. Run: python setup_ontology.py (if needed)")
        print("   2. Run: python seed_catalog.py (optional)")
        print("   3. Run: python extract_graphlets.py")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        sys.exit(1)
    finally:
        if 'driver' in locals():
            driver.close()

if __name__ == "__main__":
    main()
