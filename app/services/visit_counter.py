from typing import Dict, List, Any
import asyncio
from datetime import datetime
from ..core.redis_manager import RedisManager

cache = {}
ttl = 5
class VisitCounterService:
    def __init__(self):
        """Initialize the visit counter service with Redis manager"""
        self.redis_manager = RedisManager()
    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page
        
        Args:
            page_id: Unique identifier for the page
        """
        await self.redis_manager.increment(page_id)
        
        if page_id in cache :
            del cache[page_id]

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count
        """
        
        now = datetime.utcnow().timestamp()
        served_via = ""
        if page_id in cache:
            count, ts = cache[page_id]
            if now - ts < ttl:
                served_via = "in_memory"
                return {"count": count, "served_via": served_via}

        count = await self.redis_manager.get(page_id)
        cache[page_id] = (count, now)
        served_via = "redis"
        return {"count": count, "served_via": served_via}
