"""
Create relations between markets based on similarity and correlation thresholds
"""
import asyncio
import sys
from app.core.config import settings
from app.services.relation_service import get_relation_service
from app.services.database_service import get_database_service
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


async def create_all_relations(
    similarity_threshold: float = 0.7,
    correlation_threshold: float = 0.0,
    limit: int = None,
    batch_size: int = 50,
    skip_confirmation: bool = False
):
    """
    Create relations for all markets that have embeddings.
    
    Args:
        similarity_threshold: Minimum similarity threshold (0.0-1.0)
        correlation_threshold: Minimum correlation threshold (0.0-1.0)
        limit: Maximum number of markets to process (None = all)
        batch_size: Number of markets to process in each batch
    """
    print()
    print("="*80)
    print("CREATING MARKET RELATIONS")
    print("="*80)
    print()
    print(f"Similarity Threshold: {similarity_threshold}")
    print(f"Correlation Threshold: {correlation_threshold}")
    print()
    
    # Check configuration
    if not settings.SUPABASE_URL or not settings.SUPABASE_API_KEY:
        print("✗ ERROR: SUPABASE_URL or SUPABASE_API_KEY not set!")
        return False
    
    try:
        db = get_database_service()
        rs = get_relation_service()
        
        # Get all markets with embeddings
        print("📊 Fetching markets with embeddings...")
        all_embeddings = await db.get_all_embeddings(limit=100000)
        market_ids_with_embeddings = {emb.market_id for emb in all_embeddings}
        
        # Get all markets
        all_markets = await db.get_markets(limit=50000)
        
        # Filter to only markets with embeddings
        markets_to_process = [
            m for m in all_markets 
            if m.id in market_ids_with_embeddings
        ]
        
        if limit:
            markets_to_process = markets_to_process[:limit]
        
        print(f"✓ Found {len(markets_to_process)} markets with embeddings")
        print()
        
        if not markets_to_process:
            print("⚠️  No markets with embeddings found!")
            return False
        
        # Estimate relations count first
        print("🔍 Estimating relations count...")
        market_ids = [m.id for m in markets_to_process]
        
        # Use sampling for faster estimation (sample 100 markets or 10% whichever is smaller)
        sample_size = min(100, max(10, len(market_ids) // 10))
        estimate = await rs.estimate_relations_count(
            market_ids=market_ids,
            similarity_threshold=similarity_threshold,
            correlation_threshold=correlation_threshold,
            sample_size=sample_size
        )
        
        print()
        print("="*80)
        print("ESTIMATION RESULTS")
        print("="*80)
        if estimate.get("is_sampled"):
            print(f"📊 Sampled {estimate['sampled_markets']} markets (out of {estimate['total_markets']})")
            print(f"📊 Found {estimate['sample_relations']} relations in sample")
            print(f"📊 Average: {estimate['relations_per_market']} relations per market")
            print()
            print(f"📈 ESTIMATED TOTAL: ~{estimate['estimated_total']:,} relations")
        else:
            print(f"📊 Processed all {estimate['total_markets']} markets")
            print(f"📊 Found {estimate['sample_relations']} relations")
            print(f"📊 Average: {estimate['relations_per_market']} relations per market")
            print()
            print(f"📈 TOTAL: {estimate['estimated_total']:,} relations")
        
        if estimate.get("error"):
            print(f"⚠️  Warning: {estimate['error']}")
        
        print("="*80)
        print()
        
        # Ask for confirmation (optional - can be skipped with --yes flag)
        if not skip_confirmation:
            response = input("Continue with relation creation? (y/n): ").strip().lower()
            if response not in ['y', 'yes']:
                print("Cancelled by user.")
                return False
            print()
        
        # Process in batches
        total_created = 0
        total_skipped = 0
        
        for i in range(0, len(markets_to_process), batch_size):
            batch = markets_to_process[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(markets_to_process) + batch_size - 1) // batch_size
            
            print(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} markets)...")
            
            batch_created = 0
            batch_skipped = 0
            
            for market in batch:
                # Use service method to create relations
                created, skipped = await rs.create_relations_for_market(
                    market.id,
                    similarity_threshold,
                    correlation_threshold,
                    limit=100
                )
                batch_created += created
                batch_skipped += skipped
            
            total_created += batch_created
            total_skipped += batch_skipped
            
            pct = ((i + len(batch)) / len(markets_to_process)) * 100
            print(f"  ✓ Batch complete: {batch_created} created, {batch_skipped} skipped")
            print(f"  Progress: {i + len(batch)}/{len(markets_to_process)} ({pct:.1f}%)")
            print()
        
        print()
        print("="*80)
        print("✓ RELATION CREATION COMPLETE")
        print("="*80)
        print(f"✓ Relations created: {total_created}")
        print(f"✓ Relations skipped: {total_skipped}")
        print(f"✓ Total processed: {len(markets_to_process)}")
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
    import argparse
    
    parser = argparse.ArgumentParser(description="Create market relations based on similarity and correlation")
    parser.add_argument(
        "--similarity-threshold",
        type=float,
        default=0.7,
        help="Minimum similarity threshold (0.0-1.0, default: 0.7)"
    )
    parser.add_argument(
        "--correlation-threshold",
        type=float,
        default=0.0,
        help="Minimum correlation threshold (0.0-1.0, default: 0.0)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of markets to process (default: all)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Number of markets to process per batch (default: 50)"
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation prompt and proceed automatically"
    )
    
    args = parser.parse_args()
    
    success = asyncio.run(create_all_relations(
        similarity_threshold=args.similarity_threshold,
        correlation_threshold=args.correlation_threshold,
        limit=args.limit,
        batch_size=args.batch_size,
        skip_confirmation=args.yes
    ))
    
    sys.exit(0 if success else 1)

