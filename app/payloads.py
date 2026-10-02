"""事件载荷提交状态机、崩溃恢复、在线迁移与垃圾回收的编排层。

提交（submit_event_payload）：
  见 repository.py 文件头的五步状态机。本模块负责事务边界与对象存储调用的
  交错；任何抛出异常的位置之后，状态都可由 recover_pending 断点续跑。

恢复（recover_pending）：
  扫描 payload_upload_pending 中超过 stale 阈值的残留行（崩溃于“登记后/
  上传失败/终结事务失败”的提交），幂等补传对象并重放终结事务；
  事件已存在（重复提交完成）的残留行直接清除。

在线迁移（migrate_legacy_payloads）：
  旧 JSONB 事件按批链接到（必要时上传的）内容寻址对象，双读不中断，
  进度持久化、可断点续跑、可并发；最后可用 clear_legacy_payloads 回收
  JSONB 空间（此时所有读路径都已只认对象）。

垃圾回收（run_gc）：
  标记（零引用且无进行中提交）→ 宽限期 → 逐对象取摘要咨询锁并在行锁内复核
  引用（防并发提交/恢复/迁移复用同一内容对象）→ 删对象 → 删元数据；
  --rescan-store 额外清理“对象存储里有、元数据里没有”的孤儿（崩溃于上传后、
  终结前的对象，或手工残留），同样先过宽限候选。

引用互斥（修复对象物理删除与并发提交竞态的关键）：
  每个摘要一把 PostgreSQL 咨询锁。提交/恢复以会话级锁覆盖“登记 pending →
  事务外 PUT 对象 → 终结事务提交”全程，在线迁移以事务级锁覆盖批次内
  “PUT 对象 → 挂引用”，GC 以事务级锁覆盖“最终复核 → 物理删除 → 删元数据”。
  因此：对象一旦被引用（无论元数据行当时是否存在）都不会被删除；GC 抢先
  物理删除后，持锁引用方的幂等 PUT 会把同一不可变对象写回，不留悬空引用；
  对象删除失败仅重置该候选宽限，重试幂等，不触碰提交状态机语义。
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg

from app import objectstore, repository as repo
from app.config import settings
from app.objectstore import (
    ObjectNotFound,
    ObjectStore,
    ObjectStoreError,
    canonical_payload,
    canonical_payload_bytes,
    content_digest,
    verify_bytes,
)

log = logging.getLogger("webhook.payloads")


class PayloadSubmitConflict(Exception):
    """同一 (source, event_id) 正以另一载荷提交中（并发且内容不同）。"""


class PayloadTooLarge(Exception):
    def __init__(self, size: int, limit: int):
        super().__init__(f"事件载荷 {size} 字节超过上限 {limit}")
        self.size = size
        self.limit = limit


# -- 提交状态机 -------------------------------------------------------------------

async def submit_event_payload(
    conn: asyncpg.Connection,
    store: ObjectStore,
    *,
    source: str,
    event_id: str,
    payload: dict[str, Any],
    owner_id: str,
) -> tuple[asyncpg.Record, bool]:
    """执行一次提交，返回 (event 行, 是否重复)。

    成功返回时对象与数据库引用均已就绪；任何失败都不产生可见半成品。
    `conn` 由调用方提供，但本函数不把“对象上传”包进数据库事务
    （见模块/仓库文件头的状态机说明）。
    """
    # 第 1 步：快速查重（常见路径，零对象存储调用）。
    existing = await repo.find_event_by_key(conn, source, event_id)
    if existing is not None:
        return existing, True

    digest, data = canonical_payload(payload)
    if len(data) > settings.payload_max_bytes:
        raise PayloadTooLarge(len(data), settings.payload_max_bytes)

    # 第 2 步：登记持久化状态机行（独立事务，确保崩溃后字节不丢）。
    # digest 引用互斥锁在登记事务内取得（保证 GC 看不到“pending 已可见但锁未
    # 取得”的中间态），在第 3/4 步全程持有，函数退出时释放；进程崩溃则由
    # PostgreSQL 在连接关闭时自动回收会话锁，不残留死锁。
    digest_locked = False
    try:
        async with conn.transaction():
            # 再查一次（事务内拿到提交顺序视图）+ 检查进行中的同键提交。
            existing = await repo.find_event_by_key(conn, source, event_id)
            if existing is not None:
                return existing, True
            in_flight = await repo.get_pending_submission_for_event(
                conn, source, event_id
            )
            if in_flight is not None:
                # 同键已有进行中提交：内容一致则等其完成后按重复返回原事件
                # （极端情况下它崩溃了会由后台恢复接管，调用方稍后重试即可）；
                # 内容不一致 -> 明确冲突（业务键不能同时是两个载荷）。
                if in_flight["digest"] == digest:
                    row = await _wait_for_event(conn, source, event_id)
                    return row, True
                raise PayloadSubmitConflict(
                    f"事件 ({source}, {event_id}) 正以不同载荷提交中"
                )
            # 摘要引用互斥锁（会话级，覆盖“登记 pending → 事务外 PUT → 终结事务
            # 提交”全程）：与 GC 删除事务的同一把摘要锁互斥，杜绝“对象已在存储、
            # 元数据尚不存在”或“pending 刚登记”的窗口里被 GC 物理删除而产生
            # 悬空引用。同一摘要被多个不同事件并发提交时在这把锁上串行——内容
            # 寻址对象不可变、PUT 幂等，串行只影响极端重复内容的吞吐，不影响正确性。
            await repo.acquire_payload_lock(conn, digest)
            digest_locked = True
            await repo.register_pending_submission(
                conn,
                digest=digest,
                source=source,
                event_id=event_id,
                payload_bytes=data,
                owner_id=owner_id,
            )
    except asyncpg.UniqueViolationError as exc:
        # 与上面的检查并发：另一个同键提交刚插入。登记事务已回滚（会话锁仍在），
        # 事务外释放锁后按提交冲突返回，调用方可重试。
        if digest_locked:
            await repo.release_payload_lock(conn, digest)
        raise PayloadSubmitConflict(
            f"事件 ({source}, {event_id}) 正并发提交，请稍后重试"
        ) from exc
    except BaseException:
        if digest_locked:
            await repo.release_payload_lock(conn, digest)
        raise

    try:
        # 第 3 步：持锁幂等上传不可变对象。若 GC 此前刚物理删除过该对象（孤儿
        # 候选），这里的幂等 PUT 会把同一不可变对象原样写回，随后终结事务建立
        # 引用；反之 GC 必须等本函数释放锁后才能复核删除。
        await store.put_object(digest, data)

        # 第 4 步：终结事务（对象元数据 + 引用计数 + 事件 + 投递 + 清 pending）。
        try:
            return await repo.finalize_event_submission(
                conn,
                source=source,
                event_id=event_id,
                digest=digest,
                size_bytes=len(data),
            )
        except Exception:
            # 终结失败：pending 行保留，由 recover_pending 续跑；对象即便已上传
            # 也是零引用孤儿，最终由 GC 的 --rescan-store 清理（恢复会先于 GC
            # 取同一摘要锁补传并终结）。
            log.exception(
                "提交终结失败，保留状态机行待恢复: source=%s event_id=%s digest=%s",
                source, event_id, digest,
            )
            raise
    finally:
        if digest_locked:
            await repo.release_payload_lock(conn, digest)


async def _wait_for_event(
    conn: asyncpg.Connection, source: str, event_id: str, *, timeout: float = 10.0
) -> asyncpg.Record:
    """并发同内容提交时等待进行者把事件提交出来。"""
    deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout)
    while True:
        row = await repo.find_event_by_key(conn, source, event_id)
        if row is not None:
            return row
        if datetime.now(timezone.utc) >= deadline:
            # 进行者疑似崩溃：交给恢复流程；本次按冲突返回（调用方 409，可重试）。
            raise PayloadSubmitConflict(
                f"事件 ({source}, {event_id}) 的并发提交尚未完成，请稍后重试"
            )
        await asyncio.sleep(0.05)


# -- Worker 读路径：取对象 + 摘要校验 ----------------------------------------------

class PayloadIntegrityError(Exception):
    """对象取回失败或摘要复核不通过；Worker 据此拒绝签名。"""


async def load_event_payload(
    store: ObjectStore,
    row: dict[str, Any] | asyncpg.Record,
) -> dict[str, Any]:
    """解析投递上下文中的事件载荷（双读 + 摘要复核）。

    - 新格式（payload_digest 非空）：从对象存储取回，先比对摘要再 JSON 解析；
      对象缺失或摘要不符抛 PayloadIntegrityError（绝不签名/投递坏数据）。
    - 旧格式（迁移未完成，payload_digest 为空）：回退读 JSONB 列。
    """
    digest = row["payload_digest"]
    if digest is None:
        payload = row["payload"]
        return dict(payload) if payload is not None else {}
    try:
        data = await store.get_object(digest)
    except ObjectNotFound as exc:
        raise PayloadIntegrityError(
            f"载荷对象缺失（digest={digest}）：拒绝签名"
        ) from exc
    except ObjectStoreError as exc:
        raise PayloadIntegrityError(f"读取载荷对象失败: {exc}") from exc
    if not verify_bytes(data, digest):
        raise PayloadIntegrityError(
            f"载荷摘要复核失败（期望 {digest}，实际 {content_digest(data)}）："
            "对象可能损坏或被篡改，拒绝签名"
        )
    try:
        payload = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PayloadIntegrityError(f"载荷对象不是合法 UTF-8 JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise PayloadIntegrityError("载荷对象不是 JSON 对象")
    return payload


# -- 崩溃恢复 ----------------------------------------------------------------------

async def recover_pending(
    conn: asyncpg.Connection,
    store: ObjectStore,
    *,
    owner_id: str,
    stale_age_seconds: float | None = None,
    batch_size: int = 100,
) -> int:
    """续跑/清理残留的提交状态机行，返回本轮处理行数。

    - 事件已存在（别的进程已完成或重复提交）：直接删除 pending；
    - 其余：必要时幂等补传对象，再重放终结事务。
    """
    age = (
        settings.payload_stale_age_seconds
        if stale_age_seconds is None
        else stale_age_seconds
    )
    stale_before = datetime.now(timezone.utc) - timedelta(seconds=age)
    rows = await repo.list_stale_pending(
        conn, stale_before=stale_before, limit=batch_size
    )
    handled = 0
    for row in rows:
        digest = row["digest"]
        data = bytes(row["payload_bytes"])
        # 内容寻址校验：pending 行内字节必须与其键一致（防任何不一致状态）。
        if content_digest(data) != digest:
            log.error(
                "丢弃损坏的提交状态机行 source=%s event_id=%s digest=%s"
                "（字节与摘要不符）",
                row["source"], row["event_id"], digest,
            )
            await repo.delete_pending_submission(
                conn, row["source"], row["event_id"]
            )
            handled += 1
            continue
        try:
            existing = await repo.find_event_by_key(
                conn, row["source"], row["event_id"]
            )
            if existing is not None:
                await repo.delete_pending_submission(
                    conn, row["source"], row["event_id"]
                )
                handled += 1
                continue
            await repo.adopt_pending_submission(conn, row["id"], owner_id)
            # 与提交路径同一把摘要会话锁：覆盖“补传对象 → 终结事务”全程，与
            # GC 删除互斥（GC 即便已把对象物理删除，持锁后的幂等 PUT 也会把同内容
            # 不可变对象原样写回）；与并发的在线提交（同一 (source,event_id)
            # 只能存在一个 pending 行）在 pending 唯一约束上已先串行。
            locked = False
            try:
                await repo.acquire_payload_lock(conn, digest)
                locked = True
                await store.put_object(digest, data)
                await repo.finalize_event_submission(
                    conn,
                    source=row["source"],
                    event_id=row["event_id"],
                    digest=digest,
                    size_bytes=len(data),
                )
            finally:
                if locked:
                    await repo.release_payload_lock(conn, digest)
            handled += 1
        except ObjectStoreError:
            # 对象存储暂不可用：保留该行，下轮/下个进程继续。
            log.warning("恢复提交 digest=%s 时对象存储不可用，稍后重试", digest)
            continue
        except Exception:  # noqa: BLE001 - 单条恢复失败不能拖垮整轮
            log.exception(
                "恢复提交状态机行失败 source=%s event_id=%s",
                row["source"], row["event_id"],
            )
            continue
    return handled


async def recovery_loop(owner_id: str) -> None:
    """API 进程后台任务：周期接管崩溃残留的提交。"""
    store = objectstore.get_object_store()
    pool = None
    from app.db import get_pool

    try:
        pool = get_pool()
    except RuntimeError:
        return
    interval = settings.payload_recover_interval_seconds
    log.info("提交状态机后台恢复任务已启动（间隔 %ss）", interval)
    while True:
        await asyncio.sleep(interval)
        try:
            async with pool.acquire() as conn:
                n = await recover_pending(conn, store, owner_id=owner_id)
            if n:
                log.info("后台恢复处理了 %s 条残留提交", n)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("后台恢复轮询失败")


# -- 在线迁移（旧 JSONB -> 对象引用） -----------------------------------------------

async def migrate_legacy_payloads(
    conn: asyncpg.Connection,
    store: ObjectStore,
    *,
    batch_size: int = 200,
) -> int:
    """跑一批旧事件迁移并推进持久化进度；返回本批处理行数（0 表示完成）。

    每个事件：规范化旧 JSONB -> 摘要 -> 幂等上传对象 -> 同事务挂引用并计数。
    双读期间新旧数据都可投递（Worker 的 load_event_payload 自动选择）。
    """
    state = await repo.get_migration_state(conn)
    # 以“是否仍有未挂对象引用的旧事件”为准，而非盲信启动引导写入的状态：
    # init_schema 建表后可能立刻/并发出现旧 JSONB 事件（新库 bootstrap 的
    # NOT EXISTS 只反映建表瞬间），此时状态虽为 completed 也必须重开迁移，
    # 否则这些事件永远停留在“只在 JSONB”、且后续 gc/clear 语义对不上。
    remaining = await repo.count_legacy_events(conn)
    if state["status"] == "completed" and remaining == 0:
        return 0
    after_id = state["last_event_id"] if state["status"] != "completed" else 0

    # 整批一个事务：取批（FOR UPDATE SKIP LOCKED，可与其他迁移者并发分批），
    # 先按摘要排序取摘要咨询锁（锁序全局一致，杜绝并发迁移者间死锁），持锁完成
    # “上传对象 → 挂引用 → 推进进度”。摘要锁与 GC 删除事务的同一把锁互斥，
    # 覆盖事务外/事务内 PUT，不存在“迁移刚幂等 PUT 后被 GC 删除”的窗口；
    # 行锁只针对这批旧事件行，不阻塞新提交与投递；上传失败整批回滚、进度不前，
    # 重跑即断点续跑。
    async with conn.transaction():
        rows = await repo.fetch_legacy_events_batch(
            conn, after_id=after_id, limit=batch_size
        )
        if not rows:
            await repo.set_migration_status(conn, "completed")
            return 0
        last_id = after_id
        # 先算出本批全部摘要并按固定顺序加锁（不同事件可能去重为同一摘要）。
        batch: list[tuple[asyncpg.Record, str, bytes]] = []
        for row in rows:
            payload = row["payload"] if row["payload"] is not None else {}
            mdata = canonical_payload_bytes(payload)
            batch.append((row, content_digest(mdata), mdata))
        for mdigest in sorted({item[1] for item in batch}):
            await repo.acquire_payload_xact_lock(conn, mdigest)
        for row, digest, data in batch:
            try:
                await store.put_object(digest, data)
            except ObjectStoreError as exc:
                await repo.set_migration_status(
                    conn,
                    "failed",
                    f"event_id={row['id']} 对象上传失败（重跑自动续跑）: {exc}",
                )
                raise
            await repo.attach_payload_object_to_event(
                conn,
                event_id_fk=row["id"],
                digest=digest,
                size_bytes=len(data),
            )
            last_id = row["id"]
        await repo.update_migration_progress(
            conn, last_event_id=last_id, processed_delta=len(rows)
        )
        if len(rows) < batch_size:
            await repo.set_migration_status(conn, "completed")
    return len(rows)


async def gc_status(conn: asyncpg.Connection) -> dict[str, Any]:
    """汇总 GC 可观测状态（供 HTTP API；只含计数与候选明细，绝不含载荷内容）。"""
    grace = settings.payload_gc_grace_seconds
    candidate_rows = await repo.list_gc_candidate_rows(
        conn, grace_seconds=grace, limit=200
    )
    candidates = [
        {
            "digest": r["digest"],
            "first_seen_at": r["first_seen_at"],
            "past_grace": bool(r["past_grace"]),
            "has_metadata": bool(r["has_metadata"]),
            "ref_count": int(r["ref_count"]),
            "referenced": bool(r["referenced"]),
            "submission_in_flight": bool(r["submission_in_flight"]),
        }
        for r in candidate_rows
    ]
    total_candidates = await repo.count_payload_candidates(conn)
    past_grace = sum(1 for c in candidates if c["past_grace"])
    # list 有 LIMIT 截断时只统计返回部分，调用方可据 candidates_truncated 判断
    truncated = total_candidates > len(candidates)
    if truncated:
        # 被截断时不给出可能偏小的到期计数，避免误导运维（总数仍准确）
        past_grace = None
    return {
        "grace_seconds": grace,
        "total_candidates": total_candidates,
        "candidates_past_grace": past_grace,
        "candidates_truncated": truncated,
        "zero_reference_objects": await repo.count_zero_ref_payload_objects(conn),
        "pending_submissions": await repo.count_pending_submissions(conn),
        "candidates": candidates,
        "runs": [dict(r) for r in await repo.list_gc_runs(conn)],
    }


async def migration_status(conn: asyncpg.Connection) -> dict[str, Any]:
    state = await repo.get_migration_state(conn)
    remaining = await repo.count_legacy_events(conn)
    return {
        "status": state["status"],
        "processed": state["processed"],
        "last_event_id": state["last_event_id"],
        "remaining": remaining,
        "last_error": state["last_error"],
        "updated_at": state["updated_at"],
    }


# -- 垃圾回收 ----------------------------------------------------------------------

async def run_gc(
    conn: asyncpg.Connection,
    store: ObjectStore,
    *,
    grace_seconds: int | None = None,
    batch_size: int = 100,
    rescan_store: bool = False,
) -> dict[str, int]:
    """执行一轮 GC：标记 -> 宽限复核 -> 删除。返回统计。

    幂等：候选表记录 first_seen_at，重复运行只推进真正过期的对象；
    删除前先取摘要咨询锁、再在对象元数据行锁内复核 events 实际引用、ref_count
    与进行中提交，与并发提交/恢复/迁移建立引用全程互斥，防误删。
    """
    grace = settings.payload_gc_grace_seconds if grace_seconds is None else grace_seconds
    run_id = await repo.start_gc_run(conn, rescanned_store=rescan_store)
    deleted = 0
    candidates = 0
    try:
        candidates = await repo.mark_gc_candidates(conn, grace)
        grace_before = datetime.now(timezone.utc) - timedelta(seconds=grace)
        digests = await repo.list_gc_candidates(
            conn, grace_before=grace_before, limit=batch_size
        )
        for digest in digests:
            deleted += await _delete_one_guarded(conn, store, digest)

        if rescan_store:
            deleted += await _sweep_store_orphans(conn, store)

        await repo.update_gc_run(
            conn, run_id,
            candidates=candidates, deleted=deleted, status="completed",
        )
    except Exception as exc:  # noqa: BLE001
        await repo.update_gc_run(
            conn, run_id,
            candidates=candidates, deleted=deleted, status="failed", error=str(exc),
        )
        raise
    return {"candidates": candidates, "deleted": deleted}


async def _delete_one_guarded(conn, store: ObjectStore, digest: str) -> int:
    """在单个数据库事务内完成“摘要锁 → 锁定复核 → 删对象 → 删元数据”。

    摘要咨询锁（pg_advisory_xact_lock）在整个事务内持有，是与提交/恢复/迁移
    建立引用全程互斥的关键——尤其覆盖 --rescan-store 登记的存储孤儿（数据库无
    元数据行，行锁无处可加）：

    - 引用方（提交/恢复/迁移）在幂等 PUT 对象之前就持有同一摘要锁直到引用事务
      提交，因此本事务持锁期间不可能有新引用建立；
    - 若引用方先持锁，本函数在取锁处等待；拿到锁后其引用/pending 已提交可见，
      复核必为 reanimate；
    - 若 GC 先持锁完成物理删除，引用方拿到锁后的幂等 PUT 会把同一不可变对象
      原样写回，再建引用，不留悬空引用。

    对象删除本身失败则事务回滚（xact 锁释放），候选宽限重新计时，下轮重试：
    删除接口幂等（404 视为成功），重试安全，不影响提交状态机与事件幂等语义。
    """
    async with conn.transaction():
        await repo.acquire_payload_xact_lock(conn, digest)
        verdict = await repo.recheck_gc_candidate(conn, digest)
        if verdict == "reanimate":
            await repo.remove_gc_candidate(conn, digest)
            return 0
        try:
            await store.delete_object(digest)
        except ObjectStoreError:
            # 对象删除失败：宽限重新计时并提交本事务（下轮 GC 重试），
            # 不拖垮整轮回收。
            log.warning("GC 删除对象失败，宽限重置，下轮重试: %s", digest)
            await repo.rollback_gc_candidate(conn, digest)
            return 0
        await repo.delete_payload_metadata_locked(conn, digest)
        return 1


async def _sweep_store_orphans(conn, store: ObjectStore) -> int:
    """登记“存储里存在、元数据里不存在”的对象（上传后崩溃的残留）。

    本函数只登记候选（first_seen_at 起算宽限期）；真正的删除由下一轮 GC 的
    主 sweep 在摘要锁 + 行锁复核后统一完成——并发提交/恢复/迁移正在重新建立
    引用的对象（此刻可能已有 pending 行、元数据行尚未 upsert）会被跳过并在
    终结事务中复活，不会被误删。本函数始终返回 0（本轮不直接产生删除）。
    """
    registered = 0
    seen = 0
    async for key in store.iter_all_keys():
        seen += 1
        obj = await repo.get_payload_object(conn, key)
        if obj is not None:
            continue  # 元数据存在：正常对象（零引用由主 sweep 处理）
        in_flight = await conn.fetchval(
            "SELECT 1 FROM payload_upload_pending WHERE digest = $1 LIMIT 1", key
        )
        if in_flight is not None:
            # 提交/恢复正在上传同一摘要：引用建立由摘要锁与终结事务保护，
            # 绝不登记为孤儿（否则与提交并发的 GC 会制造无意义的复活抖动）。
            continue
        await conn.execute(
            """
            INSERT INTO payload_gc_candidates (digest)
            VALUES ($1) ON CONFLICT (digest) DO NOTHING
            """,
            key,
        )
        registered += 1
    log.info(
        "存储孤儿扫描完成：枚举 %s 个对象，新登记孤儿候选 %s 个"
        "（过宽限期后由下一轮 GC 删除）",
        seen, registered,
    )
    return 0
