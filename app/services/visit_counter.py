from typing import Dict, List, Any
import asyncio
from datetime import datetime
from ..core.redis_manager import RedisManager

class VisitCounterService:
    def __init__(self):
        """Initialize the visit counter service with Redis manager"""
        # self.redis_manager = RedisManager()
        self.visit_count_set : Dict[str, int] = {}
    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page
        
        Args:
            page_id: Unique identifier for the page
        """
        self.visit_count_set[page_id] = self.visit_count_set.get(page_id,0) + 1
        # print(self.visit_count_set)

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count
        """
        # print(self.visit_count_set)
        return self.visit_count_set.get(page_id,0)

visit_counter_service = VisitCounterService()