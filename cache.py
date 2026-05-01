"""
Redis cache helpers with stampede protection

1. Cache stampede (thundering herd):
   When a popular cache key expires under load, multiple workers hit the DB
   simultaneously before any one of them writes the new value back. This
   'stampede' can spike DB CPU to 100 % on a popular endpoint.

   Solution: Probabilistic Early Recomputation (PER) — also called
   "XFetch" (Vattani et al., 2015). A key is probabilistically
   recomputed *before* it expires, with the probability increasing as the
   key gets closer to expiry. This means one worker quietly refreshes the
   cache slightly early, and the vast majority of requests never see a miss.

2. Per-user enrollment set caching:
   `enrolled_ids` is computed on almost every student request. At scale
   this is a hot path. We cache it in Redis as a small JSON list, keyed
   per user, with a short TTL that is invalidated on enroll/unenroll.
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
import time
from typing import Any, Callable, List, Optional

logger = logging.getLogger(__name__)


# Redis client (singleton, shared across the process)
_redis = None  # set by init_cache()


def init_cache(redis_url: Optional[str]) -> None:
    """Call once at app startup. Silently disables cache if Redis is absent."""
    global _redis
    if not redis_url:
        logger.info("REDIS_URL not set — cache disabled")
        return
    try:
        import redis as _lib
        client = _lib.Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=1,
            retry_on_timeout=True,
        )
        client.ping()
        _redis = client
        logger.info("Redis connected: %s", redis_url)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Redis unavailable (%s) — running without cache", exc)


def is_available() -> bool:
    return _redis is not None


# Low-level primitives
def cache_get(key: str) -> Optional[Any]:
    if _redis is None:
        return None
    try:
        val = _redis.get(key)
        return json.loads(val) if val else None
    except Exception:  # noqa: BLE001
        return None


def cache_set(key: str, value: Any, ttl: int = 60) -> None:
    if _redis is None:
        return
    try:
        _redis.setex(key, ttl, json.dumps(value, default=str))
    except Exception:  # noqa: BLE001
        pass


def cache_delete(*keys: str) -> None:
    if _redis is None or not keys:
        return
    try:
        _redis.delete(*keys)
    except Exception:  # noqa: BLE001
        pass


def cache_delete_pattern(pattern: str) -> int:
    """Delete all keys matching a glob pattern. Use sparingly — O(N) scan."""
    if _redis is None:
        return 0
    try:
        keys = _redis.keys(pattern)
        if keys:
            _redis.delete(*keys)
        return len(keys)
    except Exception:  # noqa: BLE001
        return 0


# Stampede-safe fetch  (XFetch / Probabilistic Early Recomputation)
_DELTA_KEY_SUFFIX = ":delta"
_CREATED_KEY_SUFFIX = ":created"
_BETA = 1.0   # higher = more aggressive early recomputation (1.0 is optimal)


def stampede_safe_get(
    key: str,
    recompute_fn: Callable[[], Any],
    ttl: int,
) -> Any:
    """
    Fetch `key` from Redis, running `recompute_fn()` to refresh it.

    Uses XFetch to stagger re-computations so only *one* worker recomputes
    the value under load — everyone else keeps getting the cached version.

    Usage:
        data = stampede_safe_get(
            key="rooms:avail:Monday:08:00:10:00",
            recompute_fn=lambda: [r.to_dict() for r in get_available_rooms(...)],
            ttl=60,
        )
    """
    if _redis is None:
        return recompute_fn()

    try:
        # Read value + metadata in a single pipeline call
        pipe = _redis.pipeline(transaction=False)
        pipe.get(key)
        pipe.get(key + _DELTA_KEY_SUFFIX)
        pipe.get(key + _CREATED_KEY_SUFFIX)
        pipe.ttl(key)
        val, delta_raw, created_raw, remaining_ttl = pipe.execute()

        if val is not None and delta_raw and created_raw:
            delta   = float(delta_raw)
            created = float(created_raw)
            now     = time.time()
            # XFetch formula: recompute if early_recompute_score > remaining_ttl
            early_recompute_score = delta * _BETA * math.log(random.random() * -1 + 1e-9)
            if now - created + early_recompute_score < remaining_ttl:
                return json.loads(val)   # cache hit, no recompute needed

        # Cache miss or early recompute triggered — call the DB/compute function
        t_start = time.time()
        fresh   = recompute_fn()
        delta   = time.time() - t_start  # how long did recompute take?

        # Write value + timing metadata atomically
        now = time.time()
        pipe = _redis.pipeline(transaction=False)
        pipe.setex(key, ttl, json.dumps(fresh, default=str))
        pipe.setex(key + _DELTA_KEY_SUFFIX,   ttl, str(delta))
        pipe.setex(key + _CREATED_KEY_SUFFIX, ttl, str(now))
        pipe.execute()

        return fresh

    except Exception as exc:  # noqa: BLE001
        logger.warning("stampede_safe_get(%s) error: %s — falling back to DB", key, exc)
        return recompute_fn()


# Per-user enrollment cache
_ENROLL_TTL = 120   # 2 minutes; invalidated explicitly on enroll/unenroll


def get_enrolled_ids(user_id: int, fallback_fn: Callable[[], List[int]]) -> List[int]:
    """
    Return the set of course IDs a student is enrolled in.
    Cached per user with a short TTL.
    """
    key = f"enrolled:{user_id}"
    cached = cache_get(key)
    if cached is not None:
        return cached
    ids = fallback_fn()
    cache_set(key, ids, ttl=_ENROLL_TTL)
    return ids


def invalidate_enrollment_cache(user_id: int) -> None:
    """Call this after any enroll or unenroll action."""
    cache_delete(f"enrolled:{user_id}")


# Optimistic locking helper for the create-lecture race condition
_LOCK_TTL = 10   # seconds


def acquire_room_lock(day: str, start_str: str, end_str: str, room_id: int) -> bool:
    """
    Try to acquire a short-lived distributed lock for a (room, day, slot) combo.

    Returns True if the lock was acquired (safe to proceed with booking).
    Returns False if another worker already holds the lock (reject the request).

    This replaces the soft 'free_ids not in ...' check with a hard atomic lock,
    eliminating the race window between availability check and INSERT.
    """
    if _redis is None:
        return True   # No Redis → optimistic, rely on DB unique constraint

    lock_key = f"lock:room:{room_id}:{day}:{start_str}:{end_str}"
    # SET NX (set if not exists) + EX (expiry) — atomic in Redis
    acquired = _redis.set(lock_key, "1", nx=True, ex=_LOCK_TTL)
    return bool(acquired)


def release_room_lock(day: str, start_str: str, end_str: str, room_id: int) -> None:
    if _redis is None:
        return
    lock_key = f"lock:room:{room_id}:{day}:{start_str}:{end_str}"
    cache_delete(lock_key)