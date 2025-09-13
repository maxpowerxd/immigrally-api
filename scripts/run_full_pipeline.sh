#!/bin/bash
# Immigrally Full Pipeline Orchestrator
# Runs the complete knowledge graph processing pipeline

set -e  # Exit on any error

echo "🚀 Running full Immigrally pipeline..."
echo "====================================="

# Determine base directory (parent of immigrally-api)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
BASE_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"

echo "Base directory: $BASE_DIR"

# Verify all repositories exist
REQUIRED_REPOS=("immigrally-api" "immigrally-extraction" "immigrally-deduplication" "immigrally-mapping" "immigrally-judge")
for repo in "${REQUIRED_REPOS[@]}"; do
    if [ ! -d "$BASE_DIR/$repo" ]; then
        echo "❌ Error: $repo directory not found in $BASE_DIR"
        echo "Please ensure all repositories are cloned in the same parent directory"
        exit 1
    fi
done

echo "✅ All repositories found"

# Step 1: Setup database
echo ""
echo "1️⃣ Setting up Neo4j database..."
cd "$BASE_DIR/immigrally-api"
python scripts/setup_ontology.py
python data/seeds/seed_catalog.py

# Step 2: Run extraction
echo ""
echo "2️⃣ Running document extraction..."
cd "$BASE_DIR/immigrally-extraction"
python run_extraction.py

# Step 3: Run mapping
echo ""
echo "3️⃣ Running solution-to-goal mapping..."
cd "$BASE_DIR/immigrally-mapping"
python src/mapping/map_solutions_to_goals.py

# Step 4: Run partial deduplication (stages 1-3)
echo ""
echo "4️⃣ Running deduplication stages 1-3..."
cd "$BASE_DIR/immigrally-deduplication"
python run_deduplication.py --stage 1
python run_deduplication.py --stage 2
python run_deduplication.py --stage 3

# Step 5: Run strategy mapping
echo ""
echo "5️⃣ Running strategy-to-goal mapping..."
cd "$BASE_DIR/immigrally-mapping"
python src/mapping/map_strategies_to_goals.py

# Step 6: Complete deduplication (stages 4-9)
echo ""
echo "6️⃣ Running deduplication stages 4-9..."
cd "$BASE_DIR/immigrally-deduplication"
for stage in 4 5 6 7 8 9; do
    echo "  Running stage $stage..."
    python run_deduplication.py --stage $stage
done

# Step 7: Validate with judge
echo ""
echo "7️⃣ Running quality validation..."
cd "$BASE_DIR/immigrally-judge"
python src/judge/llm_judge.py

echo ""
echo "✅ Pipeline completed successfully!"
echo ""

# Test API functionality
echo "8️⃣ Testing API functionality..."
cd "$BASE_DIR/immigrally-api"
echo "Starting API server for testing..."
python main.py &
API_PID=$!

# Give API time to start
sleep 3

# Test API endpoints
echo "Testing API endpoints..."
curl -s http://localhost:8000/health | jq . || echo "Health check failed"
curl -s http://localhost:8000/api/v1/roadmap/u_dummy_001 | jq '.total_goals' || echo "API test failed"

# Stop API server
kill $API_PID 2>/dev/null || true

echo ""
echo "🎉 Full pipeline execution completed!"
echo "===================================="