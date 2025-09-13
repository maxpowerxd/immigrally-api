#!/usr/bin/env python3
"""
FastAPI application entry point for Immigrally Planner API
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.planner.api.endpoints import router as planner_router

app = FastAPI(
    title="Immigrally Planner API",
    description="API for immigrant financial roadmap planning",
    version="0.1.0",
)

# CORS middleware for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for testing
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include the planner router
app.include_router(planner_router)


@app.get("/")
async def root():
    """Root endpoint - health check."""
    return {"message": "Immigrally Planner API", "status": "operational"}


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "planner-api"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)