import redis
from typing import Dict, List, Optional, Any
from .consistent_hash import ConsistentHash
from .config import settings
from urllib.parse import urlparse
import asyncio

class RedisManager:
    def __init__(self):
        """Initialize Redis connection pools and consistent hashing"""
        self.connection_pools: Dict[str, redis.ConnectionPool] = {}
        self.redis_clients: Dict[str, redis.Redis] = {}
        
        # Parse Redis nodes from comma-separated string
        redis_nodes = [node.strip() for node in settings.REDIS_NODES.split(",") if node.strip()]
        self.consistent_hash = ConsistentHash(redis_nodes, settings.VIRTUAL_NODES)
        # TODO: Initialize connection pools for each Redis node
        # 1. Create connection pools for each Redis node
        # 2. Initialize Redis clients

        for node_url in redis_nodes:
            parsed_url = urlparse(node_url)

            hostname = parsed_url.hostname
            port = parsed_url.port
            
            pool = redis.ConnectionPool(host=hostname, port=port, decode_responses=True)
            self.connection_pools[hostname] = pool
            
            client = redis.Redis(connection_pool=pool)
            self.redis_clients[hostname] = client

    async def get_connection(self, key: str) -> redis.Redis:
        """
        Get Redis connection for the given key using consistent hashing
        
        Args:
            key: The key to determine which Redis node to use
            
        Returns:
            Redis client for the appropriate node
        """
        # TODO: Implement getting the appropriate Redis connection
        # 1. Use consistent hashing to determine which node should handle this key
        # 2. Return the Redis client for that node
        
        return self.redis_clients.get(key)

    async def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment a counter in Redis
        
        Args:
            key: The key to increment
            amount: Amount to increment by
            
        Returns:
            New value of the counter
        """
        # TODO: Implement incrementing a counter
        # 1. Get the appropriate Redis connection
        # 2. Increment the counter
        # 3. Handle potential failures and retries
        
        conn = await self.get_connection("redis1")
        return await asyncio.to_thread(conn.incr, key, amount)
        

    async def get(self, key: str) -> Optional[int]:
        """
        Get value for a key from Redis
        
        Args:
            key: The key to get
            
        Returns:
            Value of the key or None if not found
        """
        # TODO: Implement getting a value
        # 1. Get the appropriate Redis connection
        # 2. Retrieve the value
        # 3. Handle potential failures and retries
        
        conn = await self.get_connection("redis1")
        value = await asyncio.to_thread(conn.get, key)
        return int(value) if value is not None else 0
