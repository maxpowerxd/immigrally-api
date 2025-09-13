#!/usr/bin/env python3
"""
Seed canonical Solutions, Requirements, and Goals into Neo4j.

- Idempotent: uses MERGE and IF NOT EXISTS constraints.
- Accepts JSON or YAML (requires PyYAML if using .yaml/.yml).
- Goals are loaded from CSV.
- Keeps catalog separate from extraction runs (no extraction_run property).

Usage:
  export NEO4J_URI=bolt://localhost:7687
  export NEO4J_USER=neo4j
  export NEO4J_PASSWORD=pass
  python seed_catalog.py \
    --solutions seeds/seed_solutions.json \
    --requirements seeds/seed_requirements.json \
    --goals seeds/seed_goals.csv \
    --catalog-version v1

If paths are omitted, defaults are used.
"""

import os
import sys
import json
import csv
import argparse
import hashlib
from typing import Any, Dict, List

from neo4j import GraphDatabase

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, use system environment variables

try:
    import yaml  # optional
except Exception:
    yaml = None


def generate_id(base: str, prefix: str) -> str:
    digest = hashlib.md5(base.encode("utf-8")).hexdigest()[:8]
    return f"{prefix}_{digest}"


def load_any(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        print(f"❌ File not found: {path}")
        sys.exit(1)
    ext = os.path.splitext(path)[1].lower()
    with open(path, "r", encoding="utf-8") as f:
        if ext in (".json",):
            return json.load(f)
        if ext in (".yaml", ".yml"):
            if not yaml:
                print("❌ PyYAML not installed. Install with: pip install pyyaml")
                sys.exit(1)
            return yaml.safe_load(f)
        print(f"❌ Unsupported file extension: {ext}. Use .json or .yaml/.yml")
        sys.exit(1)


def load_csv(path: str) -> List[Dict[str, Any]]:
    """Load CSV file as list of dictionaries"""
    if not os.path.exists(path):
        print(f"❌ File not found: {path}")
        sys.exit(1)
    
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def ensure_requirement_id(item: Dict[str, Any]) -> str:
    if item.get("id"):
        return item["id"]
    base = f"{item.get('type','')}-{item.get('name','')}".strip().lower()
    return generate_id(base, "req")


def ensure_solution_id(item: Dict[str, Any]) -> str:
    if item.get("id"):
        return item["id"]
    base = item.get("name","").strip().lower()
    return generate_id(base, "sol")


def ensure_goal_id(name: str) -> str:
    """Generate goal ID from name (consistent with existing pattern)"""
    return generate_id(name.lower(), "goal")


def create_constraints(session):
    # Minimal uniqueness constraints; only created once.
    session.run("CREATE CONSTRAINT solution_id IF NOT EXISTS FOR (s:Solution) REQUIRE s.id IS UNIQUE;")
    session.run("CREATE CONSTRAINT requirement_id IF NOT EXISTS FOR (r:Requirement) REQUIRE r.id IS UNIQUE;")
    session.run("CREATE CONSTRAINT goal_id IF NOT EXISTS FOR (g:Goal) REQUIRE g.id IS UNIQUE;")


def seed_solutions(session, items: List[Dict[str, Any]], catalog_version: str):
    count = 0
    for it in items:
        sid = ensure_solution_id(it)
        name = it.get("name","").strip()
        if not name:
            continue
        session.run("""
            MERGE (s:Solution {id: $id})
            SET s.name = $name,
                s.description = coalesce($description, s.description),
                s.synonyms = coalesce($synonyms, []),
                s.catalog = true,
                s.catalog_version = $catalog_version,
                s.updated_at = timestamp()
        """, id=sid, name=name, description=it.get("description"), synonyms=it.get("synonyms", []),
             catalog_version=catalog_version)
        count += 1
    return count


def seed_requirements(session, items: List[Dict[str, Any]], catalog_version: str):
    count = 0
    for it in items:
        rid = ensure_requirement_id(it)
        name = it.get("name","").strip()
        rtype = it.get("type","").strip().lower()
        if not name or rtype not in {"document","action","eligibility","time"}:
            continue

        session.run("""
            MERGE (r:Requirement {id: $id})
            SET r.name = $name,
                r.type = $type,
                r.description = coalesce($description, r.description),
                r.archetype = coalesce($archetype, r.archetype),
                r.params_json = coalesce($params_json, r.params_json),
                r.synonyms = coalesce($synonyms, []),
                r.catalog = true,
                r.catalog_version = $catalog_version,
                r.updated_at = timestamp()
        """, id=rid, name=name, type=rtype,
             description=it.get("description",""),
             archetype=it.get("archetype"),
             params_json=json.dumps(it.get("params", {})),
             synonyms=it.get("synonyms", []),
             catalog_version=catalog_version)
        count += 1
    return count


def seed_goals(session, items: List[Dict[str, Any]], catalog_version: str):
    """Seed Goal nodes from CSV data"""
    count = 0
    for it in items:
        # CSV columns: Phase, Goal, Why It Actually Matters
        phase = it.get("Phase", "").strip()
        name = it.get("Goal", "").strip()
        description = it.get("Why It Actually Matters", "").strip()
        
        if not name or not phase:
            continue
            
        goal_id = ensure_goal_id(name)
        
        session.run("""
            MERGE (g:Goal {id: $id})
            SET g.phase = $phase,
                g.name = $name,
                g.description = $description,
                g.catalog = true,
                g.catalog_version = $catalog_version,
                g.updated_at = timestamp()
        """, id=goal_id, phase=phase, name=name, description=description,
             catalog_version=catalog_version)
        count += 1
    return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--solutions", default="seeds/seed_solutions.json", help="Path to solutions JSON/YAML")
    parser.add_argument("--requirements", default="seeds/seed_requirements.json", help="Path to requirements JSON/YAML")
    parser.add_argument("--goals", default="seeds/seed_goals.csv", help="Path to goals CSV")
    parser.add_argument("--catalog-version", default="v1", help="Catalog version label to stamp")
    args = parser.parse_args()

    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    pwd  = os.getenv("NEO4J_PASSWORD")

    if not (uri and user and pwd):
        print("❌ Missing NEO4J env vars. Please set NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD.")
        sys.exit(1)

    # Load data files
    solutions = load_any(args.solutions) if args.solutions else []
    requirements = load_any(args.requirements) if args.requirements else []
    goals = load_csv(args.goals) if args.goals else []

    with GraphDatabase.driver(uri, auth=(user, pwd)) as driver:
        with driver.session() as session:
            create_constraints(session)
            s_count = seed_solutions(session, solutions, args.catalog_version)
            r_count = seed_requirements(session, requirements, args.catalog_version)
            g_count = seed_goals(session, goals, args.catalog_version)

    print("\n✅ Seeding complete")
    print(f"   Solutions upserted:   {s_count}")
    print(f"   Requirements upserted:{r_count}")
    print(f"   Goals upserted:       {g_count}")


if __name__ == "__main__":
    main()
