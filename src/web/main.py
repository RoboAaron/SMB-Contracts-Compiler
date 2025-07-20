"""
SMB Contracts Compiler - Main FastAPI Application
"""

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
import uvicorn
from pathlib import Path

# Create FastAPI app
app = FastAPI(
    title="SMB Contracts Compiler",
    description="Government contract opportunity aggregator for South Texas",
    version="1.0.0"
)

# Setup templates and static files
templates = Jinja2Templates(directory="src/web/templates")
app.mount("/static", StaticFiles(directory="src/web/static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard page"""
    # Mock data for now
    opportunities = []
    stats = {
        "total_opportunities": 0,
        "recent_opportunities": 0,
        "last_scrape": "Not yet configured"
    }
    
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "opportunities": opportunities,
            "stats": stats
        }
    )

@app.get("/api/opportunities")
async def get_opportunities(
    limit: int = 50,
    offset: int = 0,
    source: str = None,
    location: str = None
):
    """API endpoint to get opportunities"""
    return {"success": True, "data": [], "message": "API not yet implemented"}

@app.get("/api/stats")
async def get_stats():
    """API endpoint to get system statistics"""
    return {"success": True, "data": {"total": 0, "recent": 0, "last_scrape": "Not configured"}}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "SMB Contracts Compiler"}

if __name__ == "__main__":
    uvicorn.run(
        "src.web.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
