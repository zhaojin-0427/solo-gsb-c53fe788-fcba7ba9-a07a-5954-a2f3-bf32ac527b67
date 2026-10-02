"""竞态修复验证：载荷对象 GC 物理删除 × 同摘要并发提交/恢复/迁移的引用互斥。

复现的缺陷（修复前）：
  对象存储里已有某 SHA-256 对象、数据库尚无元数据行（--rescan-store 孤儿候选
  过宽限）时，GC 的最终复核只查 ref_count/pending（且无元数据行可加行锁），
  与新提交“PUT 对象 -> 终结事务”交错可产生**悬空引用**：
    GC:   复核零引用通过 ──► 物理 DELETE 对象 ──► （无元数据可删）提交
  提交:                PUT 对象（被删）──────────► 终结事务建引用（引用一个
                                                    存储里已不存在的对象）
  => 事件引用已提交、Worker 取回必失败（payload_integrity）。

修复后不变式（本测试断言）：
  1. 任何交错结束后，每个 events.payload_digest 指向的对象物理上必然存在；
  2. 无论元数据行是否存在，对象一旦被引用都不会被删除；
  3. 对象删除失败可安全重试，不破坏提交幂等/状态机与 GC 幂等；
  4. GET /api/v1/payloads/gc 可查询候选/宽限/引用守卫状态。

非 Docker：pgserver 便携 PostgreSQL + 本地文件对象后端。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pgserver

ROOT = Path(__file__).resolve().parent
VENV_PY = os.environ.get("VENV_PY", "/tmp/venv/bin/python")
PAYLOAD_DIR = ROOT / ".test-gcrace-store"

COMMON_ENV = {
    "LEASE_SECONDS": "3",
    "HTTP_TIMEOUT_SECONDS": "2",
    "WORKER_POLL_SECONDS": "0.1",
    "MAX_ATTEMPTS": "6",
    "BACKOFF_BASE_SECONDS": "1",
    "BACKOFF_CAP_SECONDS": "8",
    "RECEIVER_SECRET": "dev-signing-secret",
    "RECEIVER_TIMESTAMP_TOLERANCE_SECONDS": "600",
    "WEBHOOK_ALLOWED_HOSTS": "127.0.0.1,localhost",
    "WEBHOOK_KEYRING_JSON": '{"active_key_id":"k1","keys":{"k1":"MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="}}',
    "PAYLOAD_STORE_MODE": "local",
    "PAYLOAD_LOCAL_DIR": str(PAYLOAD_DIR),
    "PAYLOAD_STALE_AGE_SECONDS": "0",
    "PAYLOAD_RECOVER_INTERVAL_SECONDS": "3600",
    "PAYLOAD_GC_GRACE_SECONDS": "0",
    "PYTHONUNBUFFERED": "1",
}


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def wait_http(url, timeout=20):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=1).status_code == 200:
                return
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(0.2)
    raise RuntimeError(f"not ready: {url} ({last})")


def canon(payload: dict) -> tuple[str, bytes]:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(data).hexdigest(), data


class Harness:
    def __init__(self):
        self.procs = []

    def admin(self, *args):
        env = os.environ.copy()
        env.update(COMMON_ENV)
        env["DATABASE_URL"] = self.dsn
        return subprocess.run(
            [VENV_PY, "-m", "app.payloadadmin", *args],
            cwd=ROOT, env=env, capture_output=True, text=True, timeout=180,
        )

    def start(self):
        import shutil
        for stale in (ROOT / ".test-gcrace-pgdata", PAYLOAD_DIR):
            shutil.rmtree(stale, ignore_errors=True)
        self.pg = pgserver.get_server(str(ROOT / ".test-gcrace-pgdata"),
                                      cleanup_mode="stop")
        self.dsn = self.pg.get_uri()
        PAYLOAD_DIR.mkdir(exist_ok=True)
        # 仅起 api（本测试不依赖 worker 投递；悬空引用由直接取对象断言）
        ap = free_port()
        self.api = f"http://127.0.0.1:{ap}"
        env = os.environ.copy()
        env.update(COMMON_ENV)
        env["DATABASE_URL"] = self.dsn
        log = open(ROOT / "test-gcrace-api.log", "w")
        p = subprocess.Popen(
            [VENV_PY, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1",
             "--port", str(ap)], cwd=ROOT, env=env,
            stdout=log, stderr=subprocess.STDOUT,
        )
        self.procs.append(p)
        wait_http(f"{self.api}/health")

    def stop(self):
        for p in self.procs:
            p.send_signal(signal.SIGTERM)
        for p in self.procs:
            try:
                p.wait(timeout=8)
            except subprocess.TimeoutExpired:
                p.kill()
        try:
            self.pg.cleanup()
        except Exception:  # noqa: BLE001
            pass


async def _pg(dsn):
    import asyncpg
    conn = await asyncpg.connect(dsn)
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads,
                              schema="pg_catalog")
    return conn


def main() -> int:
    h = Harness()
    h.start()
    failures = []

    def check(name, ok, detail=""):
        print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")
        if not ok:
            failures.append(name)

    # 直接在应用层跑交错（同一事件循环 + 真实 pgserver + 真实文件对象后端），
    # 用栅栏精确控制“GC 复核通过后、物理删除前后”与“并发提交 PUT/终结”的时序。
    sys.path.insert(0, str(ROOT))

    async def scenario():
        from app import payloads, repository as repo
        from app.db import create_pool
        from app.objectstore import LocalObjectStore

        pool = await create_pool(h.dsn)
        store = LocalObjectStore(str(PAYLOAD_DIR))
        await store.ensure_bucket()

        results = {}

        # ---- 场景 1a：提交持锁在先（PUT 完成、终结前暂停），GC 必须等待 -------
        # 修复前缺陷窗口正是“PUT 后、终结提交前”：旧 GC 不持摘要锁，此刻物理
        # 删除会留下悬空引用。修复后 GC 在摘要咨询锁上阻塞，待提交建引用提交后
        # 复核必为 reanimate。
        payload1 = {"race": "submit-then-gc", "n": 1}
        dig1, data1 = canon(payload1)
        await store.put_object(dig1, data1)
        async with pool.acquire() as c:
            await c.execute(
                "INSERT INTO payload_gc_candidates (digest, first_seen_at) "
                "VALUES ($1, now() - interval '2 hours') ON CONFLICT DO NOTHING",
                dig1,
            )

        gate = {"put_done": asyncio.Event(), "allow_return": asyncio.Event()}
        lock_class = 91248
        lock_key = int(dig1[:8], 16) & 0x7FFFFFFF

        class PauseAfterPutStore(LocalObjectStore):
            """提交路径 PUT 完成后挂起（此时已持摘要会话锁、尚未终结）。"""

            async def put_object(self, key, data):  # type: ignore[override]
                await super().put_object(key, data)
                if key == dig1:
                    gate["put_done"].set()
                    await asyncio.wait_for(gate["allow_return"].wait(), 10)

        pause_store = PauseAfterPutStore(str(PAYLOAD_DIR))
        results: dict = {}

        async def wait_lock_waiters(pool, expect: int, timeout: float = 5.0):
            """轮询 pg_locks：提交方持会话锁期间，GC 事务应在同一锁上等待。"""
            deadline = asyncio.get_event_loop().time() + timeout
            seen_max = 0
            while asyncio.get_event_loop().time() < deadline:
                async with pool.acquire() as c:
                    n_granted = await c.fetchval(
                        "SELECT COUNT(*) FROM pg_locks "
                        "WHERE locktype='advisory' AND classid=$1 AND objid=$2 "
                        "AND granted", lock_class, lock_key)
                    n_waiting = await c.fetchval(
                        "SELECT COUNT(*) FROM pg_locks "
                        "WHERE locktype='advisory' AND classid=$1 AND objid=$2 "
                        "AND NOT granted", lock_class, lock_key)
                seen_max = max(seen_max, n_waiting)
                if n_granted >= 1 and n_waiting >= expect:
                    return n_waiting
                await asyncio.sleep(0.02)
            return seen_max

        async def do_submit():
            async with pool.acquire() as c:
                row, dup = await payloads.submit_event_payload(
                    c, pause_store, source="race", event_id="evt-1",
                    payload=payload1, owner_id="test",
                )
                results["submit_digest"] = row["payload_digest"]
                results["submit_dup"] = dup

        async def do_gc():
            # 等提交方持锁挂起后再发起删除事务：它必然阻塞在摘要锁上
            await asyncio.wait_for(gate["put_done"].wait(), 10)
            # 先确认提交方确实持有该摘要锁
            async with pool.acquire() as c:
                granted = await c.fetchval(
                    "SELECT COUNT(*) FROM pg_locks "
                    "WHERE locktype='advisory' AND classid=$1 AND objid=$2 "
                    "AND granted", lock_class, lock_key)
            results["submit_holds_lock"] = granted >= 1
            gc_started = asyncio.Event()

            async def run_delete():
                gc_started.set()
                async with pool.acquire() as c:
                    d = await payloads._delete_one_guarded(c, pause_store, dig1)
                    results["gc_deleted"] = d

            delete_task = asyncio.create_task(run_delete())
            await gc_started.wait()
            # 实证：提交放锁之前，删除事务在同一咨询锁上等待
            results["observed_waiting_gc"] = await wait_lock_waiters(pool, 1)
            gate["allow_return"].set()  # 提交继续 -> 终结建引用 -> 释放锁
            await delete_task

        sub_task = asyncio.create_task(do_submit())
        gc_task = asyncio.create_task(do_gc())
        await asyncio.gather(sub_task, gc_task)

        check("场景1a 提交成功且非重复",
              results.get("submit_digest") == dig1
              and results.get("submit_dup") is False, str(results))
        check("场景1a 提交暂停期间确实持有摘要咨询锁",
              results.get("submit_holds_lock") is True)
        check("场景1a GC 删除事务在摘要锁上等待（pg_locks 实证）",
              results.get("observed_waiting_gc", 0) >= 1,
              f"waiting={results.get('observed_waiting_gc')}")
        check("场景1a GC 放锁后复核复活（deleted=0），对象与引用保留",
              results.get("gc_deleted") == 0
              and (PAYLOAD_DIR / dig1).exists(),
              f"deleted={results.get('gc_deleted')}")

        async with pool.acquire() as c:
            ev = await repo.find_event_by_key(c, "race", "evt-1")
            obj_meta = await c.fetchrow(
                "SELECT ref_count FROM event_payload_objects WHERE digest=$1", dig1)
            cand = await c.fetchval(
                "SELECT 1 FROM payload_gc_candidates WHERE digest=$1", dig1)
        check("场景1a 事件引用摘要正确",
              ev is not None and ev["payload_digest"] == dig1)
        loaded = await payloads.load_event_payload(store, ev)
        check("场景1a Worker 可取回并通过摘要复核", loaded == payload1)
        check("场景1a ref_count=1 且候选已移除（复活）",
              obj_meta is not None and obj_meta["ref_count"] == 1 and cand is None)

        # ---- 场景 1b：GC 在先（孤儿已物理删除），随后同摘要新提交 ---------------
        # 覆盖 --rescan-store 孤儿“无元数据行”情形：GC 先持锁删完对象后，新提交
        # 拿到锁的幂等 PUT 必须把同一不可变对象原样写回，再建引用。
        payload1b = {"race": "gc-then-submit", "n": 1}
        dig1b, data1b = canon(payload1b)
        await store.put_object(dig1b, data1b)
        async with pool.acquire() as c:
            await c.execute(
                "INSERT INTO payload_gc_candidates (digest, first_seen_at) "
                "VALUES ($1, now()-interval '2h')", dig1b)
            deleted_b = await payloads._delete_one_guarded(c, store, dig1b)
        check("场景1b GC 先删除过宽限孤儿",
              deleted_b == 1 and not (PAYLOAD_DIR / dig1b).exists())
        async with pool.acquire() as c:
            row_b, dup_b = await payloads.submit_event_payload(
                c, store, source="race", event_id="evt-1b",
                payload=payload1b, owner_id="test")
            ev_b = await repo.find_event_by_key(c, "race", "evt-1b")
        check("场景1b 后到的新提交幂等重建对象并建立引用（无悬空引用）",
              dup_b is False and row_b["payload_digest"] == dig1b
              and (PAYLOAD_DIR / dig1b).exists(),
              f"dup={dup_b} file={(PAYLOAD_DIR / dig1b).exists()}")
        loaded_b = await payloads.load_event_payload(store, ev_b)
        check("场景1b Worker 取回重建对象并通过摘要复核", loaded_b == payload1b)

        # ---- 场景 2：并发提交幂等不被 GC/锁破坏（同键重复 + 不同事件复用） -----
        async with pool.acquire() as c:
            row2, dup2 = await payloads.submit_event_payload(
                c, store, source="race", event_id="evt-1",
                payload=payload1, owner_id="test")
        check("场景2 同 (source,event_id) 重复提交幂等 200 语义",
              dup2 is True and row2["payload_digest"] == dig1)
        async with pool.acquire() as c:
            row3, dup3 = await payloads.submit_event_payload(
                c, store, source="race", event_id="evt-2",
                payload=payload1, owner_id="test")
            rc = await c.fetchval(
                "SELECT ref_count FROM event_payload_objects WHERE digest=$1", dig1)
        check("场景2 不同事件复用对象 ref_count=2",
              dup3 is False and rc == 2, f"rc={rc}")

        # ---- 场景 3：删除失败可安全重试，状态机/幂等不受影响 -------------------
        payload3 = {"race": "delete-retry", "n": 3}
        dig3, data3 = canon(payload3)
        await store.put_object(dig3, data3)
        async with pool.acquire() as c:
            await c.execute(
                "INSERT INTO payload_gc_candidates (digest, first_seen_at) "
                "VALUES ($1, now() - interval '2 hours')", dig3)

        class FlakyStore(LocalObjectStore):
            def __init__(self, root, fail_times):
                super().__init__(root)
                self.fail_times = fail_times
                self.calls = 0

            async def delete_object(self, key):  # type: ignore[override]
                if key == dig3:
                    self.calls += 1
                    if self.calls in self.fail_times:
                        from app.objectstore import ObjectStoreError
                        raise ObjectStoreError("注入的瞬时删除失败", key=key)
                await super().delete_object(key)

        flaky = FlakyStore(str(PAYLOAD_DIR), fail_times={1})
        async with pool.acquire() as c:
            d1 = await payloads._delete_one_guarded(c, flaky, dig3)
            # 第一次失败：对象仍在、候选宽限重置
            still_exists = (PAYLOAD_DIR / dig3).exists()
            cand_after_fail = await c.fetchval(
                "SELECT 1 FROM payload_gc_candidates WHERE digest=$1", dig3)
        check("场景3 删除失败：对象保留、候选仍在（宽限重置）",
              d1 == 0 and still_exists and cand_after_fail is not None)

        # 重置宽限后重试：成功删除（幂等安全）
        async with pool.acquire() as c:
            await c.execute(
                "UPDATE payload_gc_candidates SET first_seen_at=now()-interval '2h'"
                " WHERE digest=$1", dig3)
            d2 = await payloads._delete_one_guarded(c, flaky, dig3)
            meta_gone = await c.fetchval(
                "SELECT 1 FROM event_payload_objects WHERE digest=$1", dig3)
        check("场景3 删除重试成功且元数据/候选清理",
              d2 == 1 and meta_gone is None
              and not (PAYLOAD_DIR / dig3).exists())

        # 对已删除对象再跑一轮 GC：复核无引用无候选，幂等无异常（404 即成功）
        async with pool.acquire() as c:
            # 手工再插候选模拟重复调度（对象已不在存储）
            await c.execute(
                "INSERT INTO payload_gc_candidates (digest, first_seen_at) "
                "VALUES ($1, now()-interval '2h') ON CONFLICT DO NOTHING", dig3)
            d3 = await payloads._delete_one_guarded(c, flaky, dig3)
            cand_gone = await c.fetchval(
                "SELECT 1 FROM payload_gc_candidates WHERE digest=$1", dig3)
        check("场景3 对象已删后 GC 再跑幂等（404 视为成功）",
              d3 == 1 and cand_gone is None)

        # ---- 场景 4：恢复路径 × GC 交错（崩溃残留 pending） --------------------
        payload4 = {"race": "recover-vs-gc", "n": 4}
        dig4, data4 = canon(payload4)
        # 模拟崩溃：pending 残留（updated_at 很旧），对象在存储里、无元数据，
        # 同时 GC 已把它登记成过宽限孤儿候选。
        await store.put_object(dig4, data4)
        async with pool.acquire() as c:
            await c.execute(
                "INSERT INTO payload_upload_pending "
                "(digest, source, event_id, payload_bytes, owner_id, status, updated_at) "
                "VALUES ($1,'race','evt-4',$2,'dead','submitting', now()-interval '1 hour')",
                dig4, data4)
            await c.execute(
                "INSERT INTO payload_gc_candidates (digest, first_seen_at) "
                "VALUES ($1, now()-interval '2h') ON CONFLICT DO NOTHING", dig4)
            n = await payloads.recover_pending(
                c, store, owner_id="test-recover", stale_age_seconds=0)
        check("场景4 恢复接管 1 条", n == 1, f"n={n}")
        async with pool.acquire() as c:
            ev4 = await c.fetchrow(
                "SELECT payload_digest FROM events "
                "WHERE source='race' AND event_id='evt-4'")
            pending_left = await c.fetchval(
                "SELECT COUNT(*) FROM payload_upload_pending WHERE digest=$1", dig4)
            cand4 = await c.fetchval(
                "SELECT 1 FROM payload_gc_candidates WHERE digest=$1", dig4)
        check("场景4 恢复后引用建立、对象存在、pending 清空、候选复活移除",
              ev4 is not None and ev4["payload_digest"] == dig4
              and (PAYLOAD_DIR / dig4).exists()
              and pending_left == 0 and cand4 is None)
        # 再跑 GC：已被引用 -> 复活，绝不删
        async with pool.acquire() as c:
            await payloads.run_gc(c, store, grace_seconds=0,
                                  batch_size=100, rescan_store=True)
        check("场景4 GC 在恢复引用后不删除对象",
              (PAYLOAD_DIR / dig4).exists())

        # ---- 场景 5：高并发随机交错不变式（多摘要 × 提交/GC 混合） -------------
        N_SUBMIT = 24
        N_GC = 12
        payloads_n = [
            {"race": "stress", "i": i, "pad": "x" * (i % 50)}
            for i in range(8)  # 8 种内容 -> 高复用/去重
        ]
        digs_n = [canon(p)[0] for p in payloads_n]
        for d, p in zip(digs_n, payloads_n):
            await store.put_object(d, canon(p)[1])
        # 预置“过宽限候选”（模拟对象此前经历零引用）：终结事务建引用时必须
        # 立刻把候选删除（复活），这正是并发 GC 看到引用、拒绝物理删除的依据。
        async with pool.acquire() as c:
            for d in digs_n:
                await c.execute(
                    "INSERT INTO payload_gc_candidates (digest, first_seen_at) "
                    "VALUES ($1, now()-interval '2h') ON CONFLICT DO NOTHING", d)

        async def submitter(idx):
            p = payloads_n[idx % len(payloads_n)]
            async with pool.acquire() as c:
                await payloads.submit_event_payload(
                    c, store, source="stress",
                    event_id=f"s-{idx}", payload=p, owner_id="stress")

        async def gc_runner():
            async with pool.acquire() as c:
                await payloads.run_gc(c, store, grace_seconds=0,
                                      batch_size=50, rescan_store=True)

        tasks = [asyncio.create_task(submitter(i)) for i in range(N_SUBMIT)]
        tasks += [asyncio.create_task(gc_runner()) for _ in range(N_GC)]
        random.Random(42).shuffle(tasks)
        await asyncio.gather(*tasks)

        # 不变式：所有事件引用的对象物理上都存在
        async with pool.acquire() as c:
            rows = await c.fetch(
                "SELECT event_id, payload_digest FROM events "
                "WHERE source='stress'")
            missing = [
                (r["event_id"], r["payload_digest"])
                for r in rows
                if not (PAYLOAD_DIR / r["payload_digest"]).exists()
            ]
            # ref_count 与真实引用数一致
            drift = await c.fetch(
                """
                SELECT o.digest FROM event_payload_objects o
                WHERE o.ref_count <>
                    (SELECT COUNT(*) FROM events e
                     WHERE e.payload_digest = o.digest)
                """
            )
            # 候选表里不得残留任何仍被引用的摘要
            bad_cand = await c.fetch(
                """
                SELECT c.digest FROM payload_gc_candidates c
                JOIN events e ON e.payload_digest = c.digest
                """
            )
        check(f"场景5 {len(rows)} 个压力事件全部无悬空引用",
              not missing, f"missing={missing[:3]}")
        check("场景5 ref_count 与真实事件引用数一致", not drift,
              f"drift={[r['digest'][:8] for r in drift][:3]}")
        check("场景5 被引用摘要无残留 GC 候选", not bad_cand,
              f"bad={[r['digest'][:8] for r in bad_cand][:3]}")

        await pool.close()

    try:
        asyncio.run(scenario())
    except Exception:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        failures.append("场景异常")

    # ---- HTTP API：GC 状态查询（真实 uvicorn 进程） ---------------------------
    try:
        api = httpx.Client(base_url=h.api, timeout=10)
        # 经 API 制造一个零引用候选：提交 -> SQL 释放引用 -> gc 标记
        sub = api.post("/api/v1/subscriptions", json={
            "source": "apigc", "target_url": "http://127.0.0.1:1/receive",
            "secret": "dev-signing-secret",
        })
        check("HTTP 订阅创建", sub.status_code in (200, 201), sub.text)
        p = {"apigc": 7}
        dig_a, _ = canon(p)
        r = api.post("/api/v1/events", json={
            "source": "apigc", "event_id": "g-1", "payload": p})
        check("HTTP 事件提交", r.status_code == 201, r.text[:200])

        async def drop_ref(d):
            conn = await _pg(h.dsn)
            await conn.execute(
                "DELETE FROM deliveries WHERE event_id_fk IN "
                "(SELECT id FROM events WHERE source='apigc')")
            await conn.execute("DELETE FROM events WHERE source='apigc'")
            await conn.execute(
                "UPDATE event_payload_objects SET ref_count=0 WHERE digest=$1", d)
            await conn.close()
        asyncio.run(drop_ref(dig_a))
        # 用非零宽限只标记、不删除，确保随后状态查询里能看到该候选明细
        out = h.admin("gc", "--grace-seconds", "3600")
        check("HTTP 前置 gc 标记成功", out.returncode == 0, out.stderr)

        r = api.get("/api/v1/payloads/gc")
        check("GET /api/v1/payloads/gc 200", r.status_code == 200, r.text[:300])
        body = r.json()
        check("GC 状态含宽限配置/计数/运行记录",
              isinstance(body["grace_seconds"], int)
              and body["total_candidates"] >= 1
              and len(body["runs"]) >= 1
              and body["runs"][0]["status"] in ("completed", "failed"),
              json.dumps({k: body[k] for k in
                          ("grace_seconds", "total_candidates")}, ensure_ascii=False))
        cand = next((c for c in body["candidates"]
                     if c["digest"] == dig_a), None)
        check("GC 候选明细含本摘要及引用守卫字段",
              cand is not None and set(
                  ("digest", "first_seen_at", "past_grace", "has_metadata",
                   "ref_count", "referenced", "submission_in_flight")
              ).issubset(cand),
              json.dumps(cand, ensure_ascii=False) if cand else "candidate missing")

        # 复活路径也可经 API 观察：重新提交同内容 -> 候选消失
        r2 = api.post("/api/v1/events", json={
            "source": "apigc", "event_id": "g-2", "payload": p})
        check("HTTP 复活提交成功", r2.status_code == 201, r2.text[:200])
        body2 = api.get("/api/v1/payloads/gc").json()
        check("复活后该摘要不再是 GC 候选且对象存在",
              all(c["digest"] != dig_a for c in body2["candidates"])
              and (PAYLOAD_DIR / dig_a).exists())
    except Exception:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        failures.append("HTTP 场景异常")
    finally:
        h.stop()

    print()
    if failures:
        print(f"失败 {len(failures)} 项: {failures}")
        return 1
    print("GC × 并发提交引用互斥全部检查通过 ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
