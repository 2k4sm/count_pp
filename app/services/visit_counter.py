from typing import Dict, List, Any
import asyncio
from datetime import datetime
from ..core.redis_manager import RedisManager
import threading

class VisitCounterService:
    def __init__(self):
        """Initialize the visit counter service with Redis manager"""
        self.redis_manager = RedisManager()
        self.cache = {}
        self.ttl = 5
        self.buffer = {}
        self.buffer_lock = asyncio.Lock()
        thread = threading.Thread(target=self._start_background_loop, daemon=True)
        thread.start()

    
    def _start_background_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(self.flush_buffer())
        loop.run_forever()

    async def flush_buffer(self):
        while True:
            await asyncio.sleep(30)
            async with self.buffer_lock:
                for page_id, pending in list(self.buffer.items()):
                    if pending:
                        print(f"incrementing {page_id} with count {pending} to redis")
                        await self.redis_manager.increment(page_id, pending)
                self.buffer.clear()
                
            now = datetime.utcnow().timestamp()
            keys_to_delete = []
            for page_id, (count, ts) in self.cache.items():
                if now - ts >= self.ttl:
                    keys_to_delete.append(page_id)
            for key in keys_to_delete:
                del self.cache[key]


    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page
        
        Args:
            page_id: Unique identifier for the page
        """
        async with self.buffer_lock:
            self.buffer[page_id] = self.buffer.get(page_id, 0) + 1
            print("current buffer:",self.buffer)
                      
        if page_id in self.cache :
            del self.cache[page_id]

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count
        """
        
        async with self.buffer_lock:
            pending = self.buffer.get(page_id, 0)
            print("current buffer: ",{page_id : pending})
        now = datetime.utcnow().timestamp()
        served_via = ""
        if page_id in self.cache:
            count, ts = self.cache[page_id]
            if now - ts < self.ttl:
                served_via = "in_memory"
                return {"count": count + pending, "served_via": served_via}
        count = await self.redis_manager.get(page_id)
        self.cache[page_id] = (count, now)
        served_via = "redis"
        return {"count": count + pending, "served_via": served_via}

visit_counter_service = VisitCounterService()