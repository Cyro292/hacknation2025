"""
Create embeddings for existing markets
"""
import asyncio
import sys
from app.core.config import settings
from app.services.vector_service import get_vector_service
from app.services.database_service import get_database_service
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


async def create_all_embeddings():
    """Create embeddings for all markets that don't have them."""
    print()
    print("="*80)
    print("CREATING EMBEDDINGS FOR EXISTING MARKETS")
    print("="*80)
    print()
    
    # Check OpenAI API key
    if not settings.OPENAI_API_KEY:
        print("✗ ERROR: OPENAI_API_KEY not set!")
        print("  Add this to your .env file:")
        print("  OPENAI_API_KEY=sk-your-key-here")
        print()
        return False
    
    print(f"✓ OpenAI API Key: Set")
    print(f"✓ Supabase URL: {settings.SUPABASE_URL}")
    print()
    
    try:
        vs = get_vector_service()
        db = get_database_service()
        
        # Get all markets
        print("📊 Fetching markets from database...")
        markets = await db.get_markets(limit=30000)
        print(f"✓ Found {len(markets)} markets")
        print()
        
        if not markets:
            print("⚠️  No markets found in database!")
            print("   Run the scraper first to populate markets")
            return False
        
        # Check existing embeddings (BATCH!)
        print("🔍 Checking existing embeddings (fast mode - IDs only)...")
        
        # Get only market IDs (much faster than downloading full embeddings!)
        existing_market_ids = set(await db.get_all_embedding_market_ids(limit=100000))
        
        # Filter markets that need embeddings
        needs_embedding = [m for m in markets if m.id not in existing_market_ids]
        has_embedding = len(markets) - len(needs_embedding)
        
        print(f"✓ Already have embeddings: {has_embedding}")
        print(f"✓ Need to create: {len(needs_embedding)}")
        print()
        
        if not needs_embedding:
            print("✅ All markets already have embeddings!")
            return True
        
        # Create embeddings in batches (MUCH FASTER!)
        print(f"🧠 Creating {len(needs_embedding)} embeddings in batches...")
        print("   Using batch API calls (100x faster!)...")
        print()
        
        # Get all market IDs that need embeddings
        market_ids = [m.id for m in needs_embedding]
        
        # Process in batches of 1000 (burst mode - respects 1000 RPM)
        batch_size = 300
        total_created = 0
        total_failed = 0
        
        import time
        overall_start = time.time()
        
        for i in range(0, len(market_ids), batch_size):
            batch = market_ids[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(market_ids) + batch_size - 1) // batch_size
            
            elapsed = time.time() - overall_start
            if batch_num > 1:
                avg_per_batch = elapsed / (batch_num - 1)
                remaining_batches = total_batches - batch_num + 1
                eta = avg_per_batch * remaining_batches
                print(f"  📦 Processing batch {batch_num}/{total_batches} ({len(batch)} markets) - ETA: {eta/60:.1f} min")
            else:
                print(f"  📦 Processing batch {batch_num}/{total_batches} ({len(batch)} markets)...")
            
            batch_start = time.time()
            
            try:
                result = await vs.batch_create_embeddings(batch, batch_size=len(batch))
                total_created += result['created']
                total_failed += result['failed']
                
                batch_time = time.time() - batch_start
                pct = ((i + len(batch)) / len(market_ids)) * 100
                print(f"  ✓ Batch complete in {batch_time:.1f}s: {result['created']} created, {result['failed']} failed")
                print(f"  📊 Progress: {i + len(batch)}/{len(market_ids)} ({pct:.1f}%) - Elapsed: {elapsed/60:.1f} min")
                print()
                
            except Exception as e:
                batch_time = time.time() - batch_start
                print(f"  ✗ Batch {batch_num} failed after {batch_time:.1f}s: {e}")
                total_failed += len(batch)
        
        created = total_created
        failed = total_failed
        
        print()
        print("="*80)
        print("✓ EMBEDDING CREATION COMPLETE")
        print("="*80)
        print(f"✓ Successfully created: {created}")
        if failed > 0:
            print(f"✗ Failed: {failed}")
        print(f"✓ Total with embeddings: {has_embedding + created}")
        print("="*80)
        print()
        return True
        
    except Exception as e:
        print()
        print("="*80)
        print(f"✗ ERROR: {e}")
        print("="*80)
        import traceback
        traceback.print_exc()
        print()
        return False


if __name__ == "__main__":
    success = asyncio.run(create_all_embeddings())
    sys.exit(0 if success else 1)

