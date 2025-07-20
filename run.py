#!/usr/bin/env python3
"""
SMB Contracts Compiler - Startup Script
"""

import os
import sys
from pathlib import Path

# Add src to Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

if __name__ == "__main__":
    # Set default environment variables if not set
    os.environ.setdefault("LOG_LEVEL", "INFO")
    os.environ.setdefault("HOST", "0.0.0.0")
    os.environ.setdefault("PORT", "8000")
    
    # Import and run the FastAPI app
    from src.web.main import app
    import uvicorn
    
    print("🚀 Starting SMB Contracts Compiler...")
    print("📊 Dashboard will be available at: http://localhost:8000")
    print("📚 API documentation at: http://localhost:8000/docs")
    print("💚 Health check at: http://localhost:8000/health")
    print("\nPress Ctrl+C to stop the server\n")
    
    uvicorn.run(
        app,
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
        reload=True
    ) 