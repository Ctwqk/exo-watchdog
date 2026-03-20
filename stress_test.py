#!/usr/bin/env python3
"""
Stress test for exo-watchdog simulating news + worldmonitor-hourly workloads.

Usage:
    python3 stress_test.py [--restart] [--concurrent N] [--rounds N]
"""
import argparse
import asyncio
import json
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx

WATCHDOG = "http://localhost:8000"
TIMEOUT = 300  # seconds per request

# ---------------------------------------------------------------------------
# Test payloads
# ---------------------------------------------------------------------------

SAMPLE_NEWS_ARTICLES = [
    {
        "title": "Iran nuclear talks collapse after new IAEA inspection demands",
        "url": "https://example.com/iran-nuclear",
        "content": "Negotiations between Iran and Western powers have broken down after the IAEA demanded access to previously undisclosed enrichment sites. Iran's foreign minister called the demands 'politically motivated' and said Tehran would continue uranium enrichment unilaterally.",
    },
    {
        "title": "OpenAI announces GPT-5 with multimodal capabilities",
        "url": "https://example.com/gpt5",
        "content": "OpenAI has unveiled GPT-5, claiming a 40% improvement in reasoning benchmarks over GPT-4o. The new model supports native image, audio, and video understanding. Enterprise pricing starts at $60/million tokens.",
    },
    {
        "title": "Saudi Aramco cuts oil production in surprise OPEC+ move",
        "url": "https://example.com/aramco",
        "content": "Saudi Arabia announced an unilateral 500,000 barrel-per-day production cut effective next month, citing market volatility. Brent crude rose 4.2% on the news. The move comes despite pressure from the US to keep prices low ahead of summer.",
    },
    {
        "title": "Ukraine strikes Russian fuel depot deep inside Crimea",
        "url": "https://example.com/ukraine-crimea",
        "content": "Ukrainian forces used long-range drones to strike a major Russian fuel storage facility near Sevastopol, triggering fires visible for miles. Ukraine's military said the depot supplied roughly 20% of Russian air operations in the region.",
    },
    {
        "title": "Fed signals pause on rate cuts amid persistent inflation",
        "url": "https://example.com/fed-rates",
        "content": "Federal Reserve Chair Jerome Powell said the central bank is 'in no hurry' to cut rates further, citing core PCE inflation remaining above 2.5%. Futures markets now price only one cut in 2026, down from three a month ago.",
    },
    {
        "title": "Apple unveils M5 chip with 50% faster neural engine",
        "url": "https://example.com/apple-m5",
        "content": "Apple announced the M5 chip family at a special event, claiming 50% faster machine learning performance over M4. The new chips debut in the MacBook Pro lineup starting at $1,999, with Mac Pro versions arriving in Q3.",
    },
    {
        "title": "EU approves sweeping AI liability directive",
        "url": "https://example.com/eu-ai",
        "content": "The European Parliament passed the AI Liability Directive with a 412-89 vote, requiring developers of high-risk AI systems to compensate users for damages. Companies have 24 months to comply or face fines up to 6% of global revenue.",
    },
    {
        "title": "Tesla recalls 120,000 vehicles over Autopilot failure",
        "url": "https://example.com/tesla-recall",
        "content": "Tesla issued a voluntary recall of 120,000 Model 3 and Model Y vehicles after the NHTSA found that the Autopilot system incorrectly handled highway merge scenarios in 23 documented incidents. A software OTA fix is expected within two weeks.",
    },
]

HOURLY_BRIEF_ITEMS = [
    {"source": "Reuters", "title": item["title"], "link": item["url"]}
    for item in SAMPLE_NEWS_ARTICLES
]


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

@dataclass
class Result:
    kind: str
    success: bool
    latency: float
    error: str = ""
    tokens: int = 0


@dataclass
class Stats:
    results: list[Result] = field(default_factory=list)

    def add(self, r: Result):
        self.results.append(r)

    def summary(self) -> str:
        if not self.results:
            return "no results"
        by_kind: dict[str, list[Result]] = {}
        for r in self.results:
            by_kind.setdefault(r.kind, []).append(r)
        lines = []
        total_ok = sum(1 for r in self.results if r.success)
        lines.append(f"\n{'='*60}")
        lines.append(f"STRESS TEST RESULTS  ({len(self.results)} total, {total_ok} ok, {len(self.results)-total_ok} failed)")
        lines.append(f"{'='*60}")
        for kind, rs in sorted(by_kind.items()):
            ok = [r for r in rs if r.success]
            fail = [r for r in rs if not r.success]
            lats = sorted(r.latency for r in ok)
            p50 = lats[len(lats)//2] if lats else 0
            p95 = lats[int(len(lats)*0.95)] if lats else 0
            lines.append(f"\n  [{kind}]  {len(ok)}/{len(rs)} ok")
            if lats:
                lines.append(f"    latency  min={lats[0]:.1f}s  p50={p50:.1f}s  p95={p95:.1f}s  max={lats[-1]:.1f}s")
            for r in fail:
                lines.append(f"    FAIL: {r.error[:120]}")
        lines.append(f"{'='*60}\n")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Request helpers
# ---------------------------------------------------------------------------

async def submit_and_wait_job(client: httpx.AsyncClient, kind: str, payload: dict) -> Result:
    t0 = time.time()
    try:
        r = await client.post(
            f"{WATCHDOG}/v1/jobs",
            json={"kind": kind, **payload},
            timeout=30,
        )
        r.raise_for_status()
        job_id = (r.json().get("job") or r.json())["id"]

        # poll until done
        while True:
            wait_r = await client.get(
                f"{WATCHDOG}/v1/jobs/{job_id}/wait",
                params={"timeout_seconds": 60},
                timeout=90,
            )
            wait_r.raise_for_status()
            snap_outer = wait_r.json()
            snap = snap_outer.get("job") or snap_outer
            status = snap.get("status")
            if status == "completed":
                latency = time.time() - t0
                result_text = snap.get("result", {}) or {}
                resp = result_text.get("response") or {}
                content = ""
                if isinstance(resp, dict):
                    for choice in resp.get("choices", []):
                        content += choice.get("message", {}).get("content", "")
                tokens = resp.get("usage", {}).get("total_tokens", len(content)//4) if isinstance(resp, dict) else 0
                print(f"  ✓ {kind} [{job_id[:8]}] {latency:.1f}s  {tokens}tok  {content[:80]!r}")
                return Result(kind=kind, success=True, latency=latency, tokens=tokens)
            elif status == "failed":
                err = (snap.get("error") or {}).get("message", "unknown")
                latency = time.time() - t0
                print(f"  ✗ {kind} [{job_id[:8]}] FAILED: {err[:100]}")
                return Result(kind=kind, success=False, latency=latency, error=err)
            elif status in ("queued", "running"):
                await asyncio.sleep(2)
            else:
                return Result(kind=kind, success=False, latency=time.time()-t0, error=f"unexpected status: {status}")
    except Exception as e:
        return Result(kind=kind, success=False, latency=time.time()-t0, error=str(e))


async def chat_completion(client: httpx.AsyncClient, profile: str, messages: list[dict], model: str = "") -> Result:
    t0 = time.time()
    body: dict = {
        "messages": messages,
        "profile": profile,
        "source": f"stress_test/{profile}",
    }
    if model:
        body["model"] = model
    try:
        r = await client.post(
            f"{WATCHDOG}/v1/chat/completions",
            json=body,
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        latency = time.time() - t0
        content = ""
        for choice in data.get("choices", []):
            content += choice.get("message", {}).get("content", "")
        tokens = data.get("usage", {}).get("total_tokens", len(content)//4)
        print(f"  ✓ chat/{profile} {latency:.1f}s  {tokens}tok  {content[:80]!r}")
        return Result(kind=f"chat/{profile}", success=True, latency=latency, tokens=tokens)
    except Exception as e:
        latency = time.time() - t0
        print(f"  ✗ chat/{profile} FAILED in {latency:.1f}s: {str(e)[:120]}")
        return Result(kind=f"chat/{profile}", success=False, latency=latency, error=str(e))


# ---------------------------------------------------------------------------
# Test scenarios
# ---------------------------------------------------------------------------

async def test_hourly_brief(client: httpx.AsyncClient, items_count: int = 4) -> Result:
    """Simulate worldmonitor-hourly sending an hourly brief job."""
    items = HOURLY_BRIEF_ITEMS[:items_count]
    return await submit_and_wait_job(client, "hourly_news_brief", {
        "input": {
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "items": items,
        }
    })


async def test_news_article_summary(client: httpx.AsyncClient, article_idx: int = 0) -> Result:
    """Simulate news service summarizing an article."""
    article = SAMPLE_NEWS_ARTICLES[article_idx % len(SAMPLE_NEWS_ARTICLES)]
    return await chat_completion(client, "news_article_summary", [
        {"role": "user", "content": f"Title: {article['title']}\n\n{article['content']}"}
    ])


async def test_generic_chat(client: httpx.AsyncClient, idx: int = 0) -> Result:
    """Simulate a generic chat request from UI."""
    prompts = [
        "What are the key risks in today's global markets?",
        "Summarize the most important tech news this week.",
        "What is the significance of the latest Fed decision?",
        "Explain what an OPEC+ production cut means for consumers.",
    ]
    return await chat_completion(client, "generic_chat", [
        {"role": "user", "content": prompts[idx % len(prompts)]}
    ])


# ---------------------------------------------------------------------------
# Test suites
# ---------------------------------------------------------------------------

async def run_sequential(client: httpx.AsyncClient, stats: Stats):
    """Sequential baseline: one request at a time."""
    print("\n[Phase 1] Sequential baseline (3 requests)")
    r = await test_hourly_brief(client, items_count=4)
    stats.add(r)
    r = await test_news_article_summary(client, 0)
    stats.add(r)
    r = await test_news_article_summary(client, 1)
    stats.add(r)


async def run_concurrent(client: httpx.AsyncClient, stats: Stats, n: int = 4):
    """Concurrent requests: simulates news service + worldmonitor running together."""
    print(f"\n[Phase 2] Concurrent burst ({n} parallel requests)")
    tasks = []
    for i in range(n):
        if i % 3 == 0:
            tasks.append(test_hourly_brief(client, items_count=4))
        elif i % 3 == 1:
            tasks.append(test_news_article_summary(client, i))
        else:
            tasks.append(test_generic_chat(client, i))
    results = await asyncio.gather(*tasks)
    for r in results:
        stats.add(r)


async def run_queue_saturation(client: httpx.AsyncClient, stats: Stats, n: int = 8):
    """Flood the queue to test back-pressure and ordering."""
    print(f"\n[Phase 3] Queue saturation ({n} requests fired simultaneously)")
    tasks = [test_news_article_summary(client, i) for i in range(n)]
    results = await asyncio.gather(*tasks)
    for r in results:
        stats.add(r)


async def run_restart_resilience(client: httpx.AsyncClient, stats: Stats):
    """Trigger an exo restart mid-flight and verify jobs eventually complete."""
    print("\n[Phase 4] Restart resilience")
    print("  Sending 2 requests, then triggering restart via watchdog...")

    # Fire two jobs before restart
    tasks = [
        test_news_article_summary(client, 2),
        test_news_article_summary(client, 3),
    ]
    # Trigger restart 3s after firing
    async def delayed_restart():
        await asyncio.sleep(3)
        print("  → triggering restart via POST /restart ...")
        try:
            r = await client.post(f"{WATCHDOG}/restart", timeout=15)
            print(f"  → restart response: {r.status_code} {r.text[:100]}")
        except Exception as e:
            print(f"  → restart request error (may be expected): {e}")

    results = await asyncio.gather(*tasks, delayed_restart(), return_exceptions=True)
    for r in results:
        if isinstance(r, Result):
            stats.add(r)
        elif isinstance(r, BaseException):
            pass  # delayed_restart doesn't return a Result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--restart", action="store_true", help="include restart resilience test")
    parser.add_argument("--concurrent", type=int, default=4, help="concurrent burst size")
    parser.add_argument("--rounds", type=int, default=1, help="repeat the full test N times")
    parser.add_argument("--phase", choices=["seq","concurrent","flood","restart","all"], default="all")
    args = parser.parse_args()

    print(f"Stress test target: {WATCHDOG}")
    print(f"Started at: {datetime.now().isoformat()}")

    # quick health check
    async with httpx.AsyncClient() as c:
        try:
            h = await c.get(f"{WATCHDOG}/health", timeout=5)
            print(f"Watchdog health: {h.json()}")
        except Exception as e:
            print(f"ERROR: watchdog not reachable: {e}")
            sys.exit(1)

    stats = Stats()
    async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT)) as client:
        for round_i in range(args.rounds):
            if args.rounds > 1:
                print(f"\n{'='*40} ROUND {round_i+1}/{args.rounds} {'='*40}")

            if args.phase in ("seq", "all"):
                await run_sequential(client, stats)

            if args.phase in ("concurrent", "all"):
                await run_concurrent(client, stats, n=args.concurrent)

            if args.phase in ("flood", "all"):
                await run_queue_saturation(client, stats, n=args.concurrent * 2)

            if args.phase == "restart" or (args.phase == "all" and args.restart):
                await run_restart_resilience(client, stats)

    print(stats.summary())


if __name__ == "__main__":
    asyncio.run(main())
