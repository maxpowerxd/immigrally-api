# Immigrally API

Main service for the Immigrally ecosystem - provides Neo4j access, Firebase user state management, and FastAPI endpoints for roadmap generation.

## Architecture Overview

Immigrally is now split into 5 focused repositories:

```
immigrally-api          → Main service (Neo4j + Firebase + Planner API)
immigrally-extraction   → Document processing pipeline
immigrally-deduplication → 9-stage data cleaning pipeline
immigrally-mapping      → Goal/solution mapping service
immigrally-judge        → LLM evaluation and validation
```

## Quick Start

1. **Environment Setup**
```bash
cp .env.example .env  # Configure your environment variables
pip install -r requirements.txt
```

2. **Database Setup**
```bash
python scripts/setup_ontology.py
python data/seeds/seed_catalog.py
```

3. **Start API Server**
```bash
python main.py
```

## API Endpoints

- `GET /` - Health check
- `GET /health` - Service status
- `GET /api/v1/roadmap/{user_id}` - Generate personalized financial roadmap

## Environment Variables

Required environment variables:
- `OPENAI_API_KEY` - For LLM operations
- `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD` - Neo4j connection
- `FIREBASE_PROJECT_ID`, `FIREBASE_PRIVATE_KEY`, etc. - Firebase configuration

## Full Pipeline

To run the complete Immigrally processing pipeline across all repositories:

```bash
./scripts/run_full_pipeline.sh
```

This orchestrates:
1. Database setup (Neo4j schema + seed data)
2. Document extraction (immigrally-extraction)
3. Solution-to-goal mapping (immigrally-mapping)
4. 9-stage deduplication (immigrally-deduplication)
5. Quality validation (immigrally-judge)
6. API functionality testing

## Development

This repository contains:
- **Core planner logic** (`src/planner/`)
- **Infrastructure components** (`src/infrastructure/`)
- **Database setup scripts** (`scripts/`)
- **Seed data** (`data/seeds/`)
- **FastAPI endpoints** (`main.py`)

## Repository Dependencies

- **Reads from**: Neo4j database (all node types)
- **Writes to**: Firebase user states
- **Depends on**: Other repositories to populate Neo4j with processed data

For full system setup, clone all 5 repositories in the same parent directory.