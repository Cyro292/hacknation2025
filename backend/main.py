from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from contextlib import asynccontextmanager
import logging
from datetime import datetime

from app.core.config import settings
from app.routers import api_router
from app.data_retrieval.scraper import scrape_and_store_markets

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

async def run_scheduler():
    """Background task that runs the scraper at configured intervals."""
    logger.info("=" * 80)
    logger.info("🔄 SCHEDULER STARTED")
    logger.info(f"⏰ Scrape interval: {settings.SCRAPE_INTERVAL_HOURS} hour(s)")
    logger.info(f"⏰ Next scrape in: {settings.SCRAPE_INTERVAL_HOURS} hour(s)")
    logger.info("=" * 80)
    
    cycle = 1
    while True:
        try:
            logger.info(f"\n⏰ Starting scheduled scrape cycle #{cycle}")
            scrape_and_store_markets(settings.SUPABASE_URL, settings.SUPABASE_API_KEY)
            logger.info(f"⏰ Next scrape in {settings.SCRAPE_INTERVAL_HOURS} hour(s) at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            cycle += 1
        except Exception as e:
            logger.error(f"❌ Error in scheduler cycle #{cycle}: {e}", exc_info=True)
            logger.info(f"⏰ Will retry in {settings.SCRAPE_INTERVAL_HOURS} hour(s)\n")
        
        await asyncio.sleep(settings.SCRAPE_INTERVAL_HOURS * 3600)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("\n" + "🚀" * 80)
    logger.info("APPLICATION STARTUP")
    logger.info("🚀" * 80)
    logger.info(f"Project: {settings.PROJECT_NAME}")
    logger.info(f"Version: {settings.VERSION}")
    logger.info(f"Debug Mode: {settings.DEBUG}")
    logger.info(f"Supabase URL: {settings.SUPABASE_URL}")
    logger.info(f"Scrape Interval: {settings.SCRAPE_INTERVAL_HOURS} hour(s)")
    logger.info("=" * 80 + "\n")
    
    # Start the background scraper
    logger.info("Starting background data scraper...")
    asyncio.create_task(run_scheduler())
    
    yield
    
    # Shutdown
    logger.info("\n" + "🛑" * 80)
    logger.info("APPLICATION SHUTDOWN")
    logger.info("🛑" * 80 + "\n")

# Initialize FastAPI app
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    openapi_url=f"/api/v1/openapi.json",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.BACKEND_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    print("Startup event")
    print(settings.OPENAI_API_KEY)


# Include routers
app.include_router(api_router, prefix=settings.API_V1_STR)


@app.get("/")
async def root():
    return {"message": "Welcome to HackNation 2025 API"}


@app.get("/health")
async def health_check():
    return {"status": "healthy", "version": settings.VERSION}

