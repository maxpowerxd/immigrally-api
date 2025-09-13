#!/usr/bin/env python3
"""
Neo4j Extraction Run Export Script

Exports extraction runs from Neo4j database to JSON files.
This script works without APOC procedures by using standard Cypher queries.

Usage:
    python export_neo4j_runs.py [--run-id RUN_ID] [--output-dir DIR] [--list-runs]

Examples:
    # List all available extraction runs
    python export_neo4j_runs.py --list-runs
    
    # Export specific run
    python export_neo4j_runs.py --run-id "abc12345"
    
    # Export all runs to custom directory
    python export_neo4j_runs.py --output-dir "/tmp/neo4j_exports"
"""

import os
import json
import argparse
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()


class Neo4jRunExporter:
    """Export Neo4j extraction runs to JSON files."""
    
    def __init__(self, uri: str, user: str, password: str):
        """Initialize with Neo4j connection parameters."""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        """Close the Neo4j driver connection."""
        self.driver.close()
    
    def list_extraction_runs(self) -> List[Dict[str, Any]]:
        """Get list of all extraction runs with metadata."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (n)
                WHERE n.extraction_run IS NOT NULL
                WITH n.extraction_run as run_id, 
                     count(n) as node_count,
                     collect(DISTINCT labels(n)[0]) as node_types
                RETURN run_id, node_count, node_types
                ORDER BY run_id
            """)
            
            runs = []
            for record in result:
                runs.append({
                    'run_id': record['run_id'],
                    'node_count': record['node_count'],
                    'node_types': record['node_types']
                })
            
            return runs
    
    def get_run_metadata(self, run_id: str) -> Dict[str, Any]:
        """Get detailed metadata for a specific extraction run."""
        with self.driver.session() as session:
            # Get basic stats
            result = session.run("""
                MATCH (n {extraction_run: $run_id})
                WITH labels(n)[0] as node_type, count(*) as count
                RETURN node_type, count
                ORDER BY node_type
            """, run_id=run_id)
            
            node_counts = {record['node_type']: record['count'] for record in result}
            
            # Get relationship counts
            result = session.run("""
                MATCH (a {extraction_run: $run_id})-[r]->(b {extraction_run: $run_id})
                WITH type(r) as rel_type, count(*) as count
                RETURN rel_type, count
                ORDER BY rel_type
            """, run_id=run_id)
            
            rel_counts = {record['rel_type']: record['count'] for record in result}
            
            # Get source documents
            result = session.run("""
                MATCH (c:Claim {extraction_run: $run_id})
                RETURN DISTINCT c.source as source, 
                       c.source_authority as authority,
                       c.source_date as date
                ORDER BY c.source
            """, run_id=run_id)
            
            sources = [dict(record) for record in result]
            
            return {
                'run_id': run_id,
                'export_timestamp': datetime.now().isoformat(),
                'node_counts': node_counts,
                'relationship_counts': rel_counts,
                'source_documents': sources,
                'total_nodes': sum(node_counts.values()),
                'total_relationships': sum(rel_counts.values())
            }
    
    def export_extraction_run(self, run_id: str) -> Dict[str, Any]:
        """
        Export a complete extraction run to a structured JSON format.
        
        This replicates the structure of the proposed APOC query but uses
        standard Cypher without APOC procedures.
        """
        with self.driver.session() as session:
            # Main query to get all claim-solution relationships with their data
            result = session.run("""
                MATCH (cl:Claim)-[:targets]->(s:Solution)
                WHERE cl.extraction_run = $run_id
                OPTIONAL MATCH (cl)-[:asserts_clause]->(c:Clause)
                OPTIONAL MATCH (c)-[:references]->(r:Requirement)
                OPTIONAL MATCH (cl)-[:has_qualifier]->(q:Qualifier)
                OPTIONAL MATCH (cl)-[:scoped_to]->(sc:Scope)
                RETURN
                    s.id AS solution_id,
                    s.name AS solution_name,
                    s.description AS solution_description,
                    cl.id AS claim_id,
                    cl.name AS pathway_name,
                    cl.description AS pathway_description,
                    cl.source AS claim_source,
                    cl.source_authority AS claim_authority,
                    cl.source_date AS claim_date,
                    c.id AS clause_id,
                    c.logic_tree AS logic_tree,
                    collect(DISTINCT {
                        id: r.id, 
                        name: r.name, 
                        type: r.type, 
                        description: r.description
                    }) AS requirements,
                    collect(DISTINCT {
                        id: q.id,
                        key: q.key, 
                        value: q.value, 
                        evidence: q.evidence, 
                        confidence: q.confidence
                    }) AS qualifiers,
                    collect(DISTINCT {
                        id: sc.id,
                        scope_type: sc.scope_type, 
                        name: sc.name,
                        value: sc.value
                    }) AS scope
                ORDER BY s.name, cl.name
            """, run_id=run_id)
            
            # Process results into structured format
            solutions = {}
            claims = []
            
            for record in result:
                solution_id = record['solution_id']
                solution_name = record['solution_name']
                
                # Group by solution
                if solution_id not in solutions:
                    solutions[solution_id] = {
                        'id': solution_id,
                        'name': solution_name,
                        'description': record['solution_description'],
                        'claims': []
                    }
                
                # Parse logic tree if present
                logic_tree = None
                if record['clause_id'] and record['logic_tree']:
                    try:
                        logic_tree = json.loads(record['logic_tree'])
                    except json.JSONDecodeError:
                        logic_tree = record['logic_tree']  # Keep as string if not valid JSON
                
                # Create claim data
                claim_data = {
                    'id': record['claim_id'],
                    'name': record['pathway_name'],
                    'description': record['pathway_description'],
                    'source': record['claim_source'],
                    'source_authority': record['claim_authority'],
                    'source_date': record['claim_date'],
                    'clause': {
                        'id': record['clause_id'],
                        'logic_tree': logic_tree
                    } if record['clause_id'] else None,
                    'requirements': [req for req in record['requirements'] if req['id'] is not None],
                    'qualifiers': [qual for qual in record['qualifiers'] if qual['id'] is not None],
                    'scope': [sc for sc in record['scope'] if sc['id'] is not None]
                }
                
                solutions[solution_id]['claims'].append(claim_data)
                claims.append(claim_data)
            
            # Get any orphaned nodes (not connected to claims)
            orphaned_data = self._get_orphaned_nodes(session, run_id)
            
            return {
                'metadata': self.get_run_metadata(run_id),
                'solutions': list(solutions.values()),
                'claims': claims,
                'orphaned_nodes': orphaned_data,
                'export_format_version': '1.0',
                'export_timestamp': datetime.now().isoformat()
            }
    
    def _get_orphaned_nodes(self, session, run_id: str) -> Dict[str, List[Dict]]:
        """Get nodes that exist in the run but aren't connected to claims."""
        orphaned = {}
        
        # Get orphaned requirements
        result = session.run("""
            MATCH (r:Requirement {extraction_run: $run_id})
            WHERE NOT (r)<-[:references]-(:Clause {extraction_run: $run_id})
            RETURN r.id as id, r.name as name, r.type as type, r.description as description
        """, run_id=run_id)
        orphaned['requirements'] = [dict(record) for record in result]
        
        # Get orphaned scopes
        result = session.run("""
            MATCH (s:Scope {extraction_run: $run_id})
            WHERE NOT (s)<-[:scoped_to]-(:Claim {extraction_run: $run_id})
            RETURN s.id as id, s.scope_type as scope_type, s.name as name, s.value as value
        """, run_id=run_id)
        orphaned['scopes'] = [dict(record) for record in result]
        
        # Get orphaned qualifiers
        result = session.run("""
            MATCH (q:Qualifier {extraction_run: $run_id})
            WHERE NOT (q)<-[:has_qualifier]-(:Claim {extraction_run: $run_id})
            RETURN q.id as id, q.key as key, q.value as value, q.evidence as evidence, q.confidence as confidence
        """, run_id=run_id)
        orphaned['qualifiers'] = [dict(record) for record in result]
        
        return orphaned
    
    def save_export(self, data: Dict[str, Any], output_path: str):
        """Save exported data to JSON file."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Exported to: {output_path}")
        print(f"   📊 {data['metadata']['total_nodes']} nodes, {data['metadata']['total_relationships']} relationships")
        print(f"   🎯 {len(data['solutions'])} solutions, {len(data['claims'])} claims")


def main():
    parser = argparse.ArgumentParser(description='Export Neo4j extraction runs to JSON')
    parser.add_argument('--run-id', help='Specific extraction run ID to export')
    parser.add_argument('--output-dir', default='./neo4j_exports', 
                       help='Output directory for export files (default: ./neo4j_exports)')
    parser.add_argument('--list-runs', action='store_true', 
                       help='List all available extraction runs')
    parser.add_argument('--all-runs', action='store_true',
                       help='Export all extraction runs')
    
    args = parser.parse_args()
    
    # Get Neo4j connection details
    uri = os.getenv('NEO4J_URI')
    user = os.getenv('NEO4J_USER')
    password = os.getenv('NEO4J_PASSWORD')
    
    if not all([uri, user, password]):
        print("❌ Missing Neo4j environment variables.")
        print("   Please set: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD")
        return 1
    
    try:
        exporter = Neo4jRunExporter(uri, user, password)
        
        if args.list_runs:
            print("📋 Available extraction runs:")
            print("=" * 50)
            runs = exporter.list_extraction_runs()
            if not runs:
                print("   No extraction runs found")
            else:
                for run in runs:
                    print(f"   {run['run_id']}: {run['node_count']} nodes ({', '.join(run['node_types'])})")
            return 0
        
        # Create output directory
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        if args.run_id:
            # Export specific run
            print(f"🔄 Exporting extraction run: {args.run_id}")
            data = exporter.export_extraction_run(args.run_id)
            output_file = output_dir / f"extraction_run_{args.run_id}.json"
            exporter.save_export(data, output_file)
            
        elif args.all_runs:
            # Export all runs
            runs = exporter.list_extraction_runs()
            if not runs:
                print("❌ No extraction runs found to export")
                return 1
            
            print(f"🔄 Exporting {len(runs)} extraction runs...")
            for run in runs:
                run_id = run['run_id']
                print(f"\n📦 Processing run: {run_id}")
                data = exporter.export_extraction_run(run_id)
                output_file = output_dir / f"extraction_run_{run_id}.json"
                exporter.save_export(data, output_file)
            
            print(f"\n✅ All runs exported to: {output_dir}")
            
        else:
            print("❌ Please specify --run-id, --list-runs, or --all-runs")
            return 1
        
        exporter.close()
        return 0
        
    except Exception as e:
        print(f"❌ Export failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
