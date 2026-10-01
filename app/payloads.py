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
  标记（零引用且无进行中提交）→ 宽限期 → 逐对象行锁复核引用（防并发
  提交/重放复用同一内容对象）→ 删对象 → 删元数据；
  --rescan-store 额外清理“对象存储里有、元数据里没有”的孤儿（崩溃于上传后、
  终结前的对象，或手工残留），同样先过宽限候选。
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
    async with conn.transaction():
        # 再查一次（事务内拿到提交顺序视图）+ 检查进行中的同键提交。
        existing = await repo.find_event_by_key(conn, source, event_id)
        if existing is not None:
            return existing, True
        in_flight = await repo.get_pending_submission_for_event(conn, source, event_id)
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
        try:
            await repo.register_pending_submission(
                conn,
                digest=digest,
                source=source,
                event_id=event_id,
                payload_bytes=data,
                owner_id=owner_id,
            )
        except asyncpg.UniqueViolationError as exc:
            # 与上面的检查并发：另一个同键提交刚插入。按提交冲突返回，调用方可重试。
            raise PayloadSubmitConflict(
                f"事件 ({source}, {event_id}) 正并发提交，请稍后重试"
            ) from exc

    # 第 3 步：事务外幂等上传不可变对象。内容寻址 => 重传/并发传同一键安全。
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
        # 也是零引用孤儿，最终由 GC 的 --rescan-store 清理。
        log.exception(
            "提交终结失败，保留状态机行待恢复: source=%s event_id=%s digest=%s",
            source, event_id, digest,
        )
        raise


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
            await store.put_object(digest, data)
            await repo.finalize_event_submission(
                conn,
                source=row["source"],
                event_id=row["event_id"],
                digest=digest,
                size_bytes=len(data),
            )
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
    after_id = state["last_event_id"] if state["status"] != "completed" else 0
    if state["status"] == "completed":
        return 0

    # 整批一个事务：取批（FOR UPDATE SKIP LOCKED，可与其他迁移者并发分批），
    # 持锁完成“上传对象 → 挂引用 → 推进进度”。对象存储是幂等快速操作，且锁只
    # 针对这批旧事件行，不阻塞新提交与投递；上传失败整批回滚、进度不前，重跑
    # 即断点续跑。
    async with conn.transaction():
        rows = await repo.fetch_legacy_events_batch(
            conn, after_id=after_id, limit=batch_size
        )
        if not rows:
            await repo.set_migration_status(conn, "completed")
            return 0
        last_id = after_id
        for row in rows:
            payload = row["payload"] if row["payload"] is not None else {}
            data = canonical_payload_bytes(payload)
            digest = content_digest(data)
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
    删除前在对象元数据行锁内复核 ref_count 与进行中提交，防并发误删。
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
    """在单个数据库事务内完成“锁定复核 → 删对象 → 删元数据”。

    行锁在整个对象物理删除期间保持：并发提交的终结事务在 upsert/锁对象行处
    阻塞，直到本事务提交（对象与元数据一起消失）或回滚（对象仍在），因此
    不存在“复核零引用后、删除前被并发建引用”而误删在用对象的窗口。
    对象删除本身失败则整个事务回滚，候选宽限重新计时，下轮重试。
    """
    async with conn.transaction():
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
    主 sweep 在行锁复核后统一完成——并发提交或重放重新建立引用的对象会被
    复活而不会被误删。本函数始终返回 0（本轮不直接产生删除）。
    """
    registered = 0
    seen = 0
    async for key in store.iter_all_keys():
        seen += 1
        obj = await repo.get_payload_object(conn, key)
        if obj is not None:
            continue  # 元数据存在：正常对象（零引用由主 sweep 处理）
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
