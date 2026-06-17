"""
crawler.py - asynchronous Karrot community crawler engine.

The terminal UI imports this module, but it can also be reused directly.
It supports batch scanning, bounded concurrency, and adaptive backoff when
the remote service returns HTTP 429.
"""

import asyncio
import aiohttp
import json
import csv
import os
import time
from dataclasses import dataclass, field
from typing import Callable, Optional


FIELDNAMES = [
    "dbId","regionId","regionName","gu",
    "subject","status","createdAt","updatedAt",
    "commentsCount","emotionCount","readsCount","watchesCount",
    "imageCount","articleUrl",
]

HEADERS = {
    "accept": "*/*",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}


@dataclass
class Config:
    rps: float = 1.0               # max requests per second
    concurrency: int = 1           # concurrent TCP connections
    batch_size: int = 10           # dbIds per batch
    batch_pause: float = 2.0       # minimum wait between batches
    step: int = 1
    save_every: int = 500
    request_timeout: float = 10.0
    retries: int = 2
    start_dbid: int = 0
    end_dbid: int = 0
    output_csv: str = "daangn_seoul.csv"
    progress_file: str = "daangn_progress.json"
    regions_file: str = "seoul_regions.json"


@dataclass
class Stats:
    scanned: int = 0
    collected: int = 0
    current_dbid: int = 0
    err_rate: float = 0.0
    speed: float = 0.0
    seoul_speed: float = 0.0
    elapsed: float = 0.0
    running: bool = False
    paused: bool = False
    gu_counts: dict = field(default_factory=dict)
    current_rps: float = 0.0       # effective RPS after backoff


def load_seoul_map(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return {int(r["id"]): r for r in json.load(f)}


def load_progress(path: str):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None


def save_progress(path: str, last_dbid: int, scanned: int, collected: int):
    with open(path, "w") as f:
        json.dump({"last_dbid": last_dbid, "scanned": scanned, "collected": collected}, f)


def append_csv(path: str, rows: list):
    exists = os.path.exists(path) and os.path.getsize(path) > 0
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES, quoting=csv.QUOTE_ALL)
        if not exists:
            w.writeheader()
        w.writerows(rows)


def _clean(text: str, maxlen: int = 0) -> str:
    if not text:
        return ""
    t = text.replace("\r\n", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ")
    return t[:maxlen] if maxlen else t


def to_row(article: dict, seoul_map: dict) -> dict:
    rid = article.get("regionId")
    info = seoul_map.get(rid, {})
    return {
        "dbId": article.get("dbId", ""),
        "regionId": rid,
        "regionName": article.get("regionName", ""),
        "gu": info.get("gu", ""),
        "subject": article.get("subject", ""),
        "status": article.get("status", ""),
        "createdAt": article.get("createdAt", ""),
        "updatedAt": article.get("updatedAt", ""),
        "commentsCount": article.get("commentsCount", 0),
        "emotionCount": article.get("emotionCount", 0),
        "readsCount": article.get("readsCount", 0),
        "watchesCount": article.get("watchesCount", 0),
        "imageCount": len(article.get("imageUrls") or []),
        "articleUrl": f"https://www.daangn.com{article.get('id', '')}",
    }


# ── 🚀 adaptive rate limiter ──────────────────────────────────────────────────

class AdaptiveRateLimiter:
    """
    Simple token-bucket limiter with adaptive speed control.

    - Enforces the configured requests-per-second limit.
    - Halves the rate after HTTP 429 responses.
    - Increases the rate gradually after clean batches, up to max_rps.
    """

    def __init__(self, rps: float, min_rps: float = 2.0, max_rps: float = 30.0):
        self.rps = rps
        self.min_rps = min_rps
        self.max_rps = max_rps
        self._interval = 1.0 / rps  # minimum interval between requests(seconds)
        self._last_request = 0.0
        self._lock = asyncio.Lock()
        self._ok_streak = 0         # consecutive healthy batch count

    async def acquire(self):
        """wait for the next request permit without exceeding rps."""
        async with self._lock:
            now = asyncio.get_event_loop().time()
            wait = self._interval - (now - self._last_request)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request = asyncio.get_event_loop().time()

    def on_429(self):
        """Apply an aggressive slowdown after a 429 response."""
        old = self.rps
        self.rps = max(self.rps * 0.5, self.min_rps)
        self._interval = 1.0 / self.rps
        self._ok_streak = 0
        return old, self.rps

    def on_success(self):
        """Increase rate by 10 percent after five clean batches."""
        self._ok_streak += 1
        if self._ok_streak >= 5:
            old = self.rps
            self.rps = min(self.rps * 1.1, self.max_rps)
            self._interval = 1.0 / self.rps
            self._ok_streak = 0
            return old, self.rps
        return None


# ── async fetch ──────────────────────────────────────────────────────────────

async def fetch_one_async(
    session: aiohttp.ClientSession,
    dbid: int,
    limiter: AdaptiveRateLimiter,
    sem: asyncio.Semaphore,
    retries: int = 2,
):
    """
    Fetch one candidate post.

    Returns a post dict, "SKIP" for 404 gaps, or an error marker dict.
    """
    url = (
        f"https://www.daangn.com/kr/community/{dbid}/"
        f"?_data=routes%2Fkr.community.%24community_agora_id"
    )
    last_error = "unknown"
    is_429 = False

    async with sem:
        for attempt in range(retries + 1):
            # Respect the shared rate limit before each request.
            await limiter.acquire()
            try:
                async with session.get(url) as r:
                    if r.status in (200, 410):
                        data = await r.json(content_type=None)
                        return data.get("data", {}).get("communityArticle")
                    if r.status == 404:
                        return "SKIP"
                    if r.status == 429:
                        is_429 = True
                        last_error = "HTTP 429 (Too Many Requests)"
                        await asyncio.sleep(2 ** (attempt + 1))
                        continue
                    last_error = f"HTTP {r.status}"
                    if attempt < retries:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
            except aiohttp.ClientError as e:
                last_error = f"ClientError: {type(e).__name__}: {e}"
                if attempt < retries:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
            except asyncio.TimeoutError:
                last_error = f"TimeoutError (>{session.timeout.total}s)"
                if attempt < retries:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
            except Exception as e:
                last_error = f"Unexpected: {type(e).__name__}: {e}"
                if attempt < retries:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
    return {"_error": last_error, "_dbid": dbid, "_is_429": is_429}


async def fetch_batch_async(
    session: aiohttp.ClientSession,
    batch_ids: list,
    limiter: AdaptiveRateLimiter,
    sem: asyncio.Semaphore,
    retries: int,
) -> list:
    """Fetch a batch of dbIds concurrently under the shared rate limit."""
    tasks = [
        fetch_one_async(session, dbid, limiter, sem, retries)
        for dbid in batch_ids
    ]
    results = await asyncio.gather(*tasks)
    return list(zip(batch_ids, results))


# ── main crawl loop ───────────────────────────────────────────────────────────

async def _run_crawl_async(
    cfg: Config,
    stats: Stats,
    on_batch: Optional[Callable] = None,
    on_save: Optional[Callable] = None,
    on_log: Optional[Callable] = None,
    stop_flag: Optional[Callable] = None,
    pause_flag: Optional[Callable] = None,
):
    log = on_log or (lambda m: print(m))

    seoul_map = load_seoul_map(cfg.regions_file)
    seoul_ids = set(seoul_map.keys())
    log(f"Loaded {len(seoul_ids)} Seoul regionIds")

    progress = load_progress(cfg.progress_file)
    if progress:
        start_id = cfg.start_dbid
        stats.scanned = progress["scanned"]
        stats.collected = progress["collected"]
        resume_dbid = progress["last_dbid"]
        log(f"Resume: last dbId={resume_dbid:,}, scanned={stats.scanned:,}, Seoul={stats.collected:,}")
    else:
        start_id = cfg.start_dbid
        stats.scanned = 0
        stats.collected = 0
        if os.path.exists(cfg.output_csv):
            os.remove(cfg.output_csv)

    buffer = []
    pause = cfg.batch_pause
    t_start = time.time()
    gu_counts = {}
    stats.running = True

    # Adaptive rate limiter shared by all concurrent fetches.
    limiter = AdaptiveRateLimiter(rps=cfg.rps, min_rps=2.0, max_rps=cfg.rps * 2)
    sem = asyncio.Semaphore(cfg.concurrency)
    stats.current_rps = limiter.rps
    log(f"Initial RPS: {limiter.rps:.1f}, concurrency: {cfg.concurrency}")

    timeout = aiohttp.ClientTimeout(total=cfg.request_timeout)
    connector = aiohttp.TCPConnector(limit=cfg.concurrency + 10, ttl_dns_cache=300)

    async with aiohttp.ClientSession(
        headers=HEADERS,
        timeout=timeout,
        connector=connector,
    ) as session:
        while True:
            if stop_flag and stop_flag():
                break

            while pause_flag and pause_flag():
                stats.paused = True
                await asyncio.sleep(0.3)
            stats.paused = False

            # Build the next descending dbId batch.
            batch_ids = [
                start_id - (stats.scanned + i) * cfg.step
                for i in range(cfg.batch_size)
            ]
            if batch_ids[-1] < cfg.end_dbid:
                batch_ids = [d for d in batch_ids if d >= cfg.end_dbid]
                if not batch_ids:
                    break

            # Fetch concurrently while preserving the rate limit.
            real_errors = 0
            got_429 = False
            new_rows = []
            error_samples = []
            results = await fetch_batch_async(session, batch_ids, limiter, sem, cfg.retries)

            for dbid, result in results:
                if result == "SKIP":
                    pass
                elif isinstance(result, dict) and "_error" in result:
                    real_errors += 1
                    if result.get("_is_429"):
                        got_429 = True
                    if len(error_samples) < 3:
                        error_samples.append(result["_error"])
                elif result is None:
                    real_errors += 1
                else:
                    rid = result.get("regionId")
                    if rid in seoul_ids:
                        row = to_row(result, seoul_map)
                        new_rows.append(row)
                        gu = row["gu"]
                        gu_counts[gu] = gu_counts.get(gu, 0) + 1

            stats.scanned += len(batch_ids)
            stats.collected += len(new_rows)
            stats.current_dbid = batch_ids[-1]
            stats.err_rate = real_errors / len(batch_ids) if batch_ids else 0
            stats.gu_counts = dict(gu_counts)

            elapsed = time.time() - t_start
            stats.elapsed = elapsed
            stats.speed = stats.scanned / elapsed if elapsed > 0 else 0
            stats.seoul_speed = stats.collected / elapsed if elapsed > 0 else 0

            buffer.extend(new_rows)

            # Adjust the rate according to batch health.
            if got_429:
                old, new = limiter.on_429()
                stats.current_rps = limiter.rps
                err_detail = " | ".join(error_samples) if error_samples else "429"
                log(f"HTTP 429: RPS {old:.1f} -> {new:.1f} [{err_detail}]")
                # Cool down after throttling.
                await asyncio.sleep(10)
            elif stats.err_rate > 0.2:
                err_detail = " | ".join(error_samples) if error_samples else "unknown errors"
                log(f"High error rate {stats.err_rate:.0%} [{err_detail}]")
            else:
                result = limiter.on_success()
                if result:
                    old, new = result
                    stats.current_rps = limiter.rps
                    log(f"Rate increase: RPS {old:.1f} -> {new:.1f}")

            stats.current_rps = limiter.rps

            if on_batch:
                on_batch(stats)

            # Save periodically to make interruption safe.
            if len(buffer) >= cfg.save_every:
                append_csv(cfg.output_csv, buffer)
                save_progress(cfg.progress_file, stats.current_dbid, stats.scanned, stats.collected)
                buffer = []
                if on_save:
                    on_save(stats)

            await asyncio.sleep(pause)

    # Final save.
    if buffer:
        append_csv(cfg.output_csv, buffer)
    save_progress(cfg.progress_file, stats.current_dbid, stats.scanned, stats.collected)
    stats.running = False
    log(f"complete: {stats.scanned:,}cases scan → Seoul {stats.collected:,}cases")


def run_crawl(
    cfg: Config,
    stats: Stats,
    on_batch: Optional[Callable] = None,
    on_save: Optional[Callable] = None,
    on_log: Optional[Callable] = None,
    stop_flag: Optional[Callable] = None,
    pause_flag: Optional[Callable] = None,
):
    """
    Synchronous wrapper used by the terminal UI.
    """
    asyncio.run(
        _run_crawl_async(
            cfg, stats,
            on_batch=on_batch,
            on_save=on_save,
            on_log=on_log,
            stop_flag=stop_flag,
            pause_flag=pause_flag,
        )
    )
