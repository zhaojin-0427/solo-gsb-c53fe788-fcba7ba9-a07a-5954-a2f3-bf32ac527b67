"""事件载荷对象存储管理命令行：在线迁移、垃圾回收、崩溃恢复与状态查看。

用法（容器内）：

    python -m app.payloadadmin status [--verbose]
        # 查看对象/引用/待迁移/待恢复/GC 运行状态

    python -m app.payloadadmin recover [--batch-size N]
        # 立即续跑崩溃/中断残留的提交状态机行（API 进程也会后台自动执行）

    python -m app.payloadadmin migrate [--batch-size N]
        # 把旧 JSONB 事件在线链接到内容寻址对象（幂等、断点续跑、可并发）；
        # 迁移期间双读，投递不中断。

    python -m app.payloadadmin clear-legacy [--batch-size N]
        # 迁移完成后清空事件行内冗余 JSONB（分批，可重复运行直到为 0）

    python -m app.payloadadmin gc [--grace-seconds N] [--rescan-store] [--batch-size N]
        # 一阶段标记 + 宽限复核的幂等垃圾回收：
        # 首次运行标记零引用候选；超过宽限期后再次运行才真正删除对象。
        # --rescan-store 同时登记对象存储里有、数据库元数据里没有的孤儿
        # （同样需要再过一轮宽限才会删除）。

    python -m app.payloadadmin gc-status
        # 与 GET /api/v1/payloads/gc 相同的回收状态（候选/宽限/引用守卫/运行记录）

所有命令与在线提交/投递并发安全：标记不阻塞写入，删除前先取“按摘要的咨询锁”、
再在对象元数据行锁内复核 events 实际引用与进行中提交，与同摘要的新事件提交、
崩溃恢复、在线迁移建立引用全程互斥；对象一旦被引用都不会被删除，正在复用的
对象会被复活。对象删除失败只重置该候选宽限，可安全重试，不影响提交状态机与
事件/投递幂等语义。
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from app import payloads, repository as repo
from app.config import settings
from app.db import close_pool, init_db
from app.objectstore import init_object_store

log = logging.getLogger("webhook.payloadadmin")


# -- status ----------------------------------------------------------------------

async def _status(verbose: bool) -> int:
    pool = await init_db()
    store = await init_object_store()
    try:
        async with pool.acquire() as conn:
            total_events = await conn.fetchval("SELECT COUNT(*) FROM events")
            legacy = await repo.count_legacy_events(conn)
            objects = await repo.count_payload_objects(conn)
            candidates = await repo.count_payload_candidates(conn)
            pending = await repo.count_pending_submissions(conn)
            migration = await payloads.migration_status(conn)
            gc_runs = await repo.list_gc_runs(conn)
            ref_sum = await conn.fetchval(
                "SELECT COALESCE(SUM(ref_count), 0) FROM event_payload_objects"
            )
            bytes_sum = await conn.fetchval(
                "SELECT COALESCE(SUM(size_bytes), 0) FROM event_payload_objects"
            )
    finally:
        await close_pool()

    print(f"对象存储后端：mode={store.mode}")
    if store.mode == "s3":
        print(f"桶：{settings.s3_bucket}（endpoint={settings.s3_endpoint_url}）")
    else:
        print(f"目录：{settings.local_objectstore_dir}")
    print(
        f"事件总数={total_events}，待迁移(仍只在 JSONB)={legacy}，"
        f"去重对象数={objects}，引用总数={ref_sum}，对象总字节={bytes_sum}"
    )
    print(
        f"提交状态机残留={pending}，GC 候选={candidates}，"
        f"GC 宽限期={settings.payload_gc_grace_seconds}s"
    )
    print(
        "迁移状态：status={status} processed={processed} "
        "last_event_id={last_event_id} remaining={remaining}".format(**migration)
    )
    if migration["last_error"]:
        print(f"  last_error: {migration['last_error']}")
    if gc_runs:
        print("最近 GC 运行：")
        for r in gc_runs:
            print(
                f"  #{r['id']} [{r['status']}] candidates={r['candidates']} "
                f"deleted={r['deleted_objects']} rescan_store={r['rescanned_store']}"
                + (f" error={r['last_error']}" if r["last_error"] else "")
            )
    else:
        print("GC 运行：无")
    return 0


# -- recover ---------------------------------------------------------------------

async def _recover(batch_size: int) -> int:
    pool = await init_db()
    store = await init_object_store()
    total = 0
    try:
        while True:
            async with pool.acquire() as conn:
                # CLI 立即接管：stale 年龄给 0（显式人工恢复，不等后台阈值）。
                n = await payloads.recover_pending(
                    conn, store, owner_id="payloadadmin-recover",
                    stale_age_seconds=0, batch_size=batch_size,
                )
            total += n
            if n < batch_size:
                break
        print(f"恢复完成：处理残留提交 {total} 条")
        return 0
    finally:
        await close_pool()


# -- migrate ---------------------------------------------------------------------

async def _migrate(batch_size: int) -> int:
    pool = await init_db()
    store = await init_object_store()
    total = 0
    try:
        while True:
            async with pool.acquire() as conn:
                try:
                    n = await payloads.migrate_legacy_payloads(
                        conn, store, batch_size=batch_size
                    )
                except Exception:  # noqa: BLE001 - 进度已持久化为 failed
                    log.exception("迁移批次失败（已持久化进度，重跑续跑）")
                    return 1
            total += n
            if n:
                log.info("本批迁移 %d 个事件（累计 %d）", n, total)
            if n < batch_size:
                break
        async with pool.acquire() as conn:
            status = await payloads.migration_status(conn)
        print(
            f"迁移完成：共链接 {total} 个事件到对象存储；"
            f"状态={status['status']}，剩余待迁移={status['remaining']}"
        )
        if status["remaining"] == 0:
            print("提示：可运行 'clear-legacy' 回收事件行内的旧 JSONB 字节")
        return 0
    finally:
        await close_pool()


# -- clear-legacy ----------------------------------------------------------------

async def _clear_legacy(batch_size: int) -> int:
    pool = await init_db()
    total = 0
    try:
        while True:
            async with pool.acquire() as conn:
                n = await repo.clear_legacy_payloads(conn, batch_size)
            total += n
            if n < batch_size:
                break
        async with pool.acquire() as conn:
            remaining = await repo.count_legacy_events(conn)
        print(f"清理完成：本命令清空 {total} 行旧 JSONB；仍待迁移事件={remaining}")
        if remaining:
            print("提示：仍有事件未迁移，请先运行 'migrate'")
        return 0
    finally:
        await close_pool()


# -- gc --------------------------------------------------------------------------

async def _gc(grace_seconds: int, rescan_store: bool, batch_size: int) -> int:
    pool = await init_db()
    store = await init_object_store()
    try:
        async with pool.acquire() as conn:
            stats = await payloads.run_gc(
                conn,
                store,
                grace_seconds=grace_seconds,
                batch_size=batch_size,
                rescan_store=rescan_store,
            )
        print(
            f"GC 完成：本轮标记候选 {stats['candidates']} 个，"
            f"删除过期对象 {stats['deleted']} 个"
            + ("（已扫描对象存储孤儿，下一轮 GC 过宽限后删除）" if rescan_store else "")
        )
        print(
            "提示：新标记的候选需超过宽限期 "
            f"{grace_seconds}s 后再次运行 gc 才会被删除（防并发提交/重放误删）"
        )
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"GC 失败（已记录运行，重跑幂等）: {exc}", file=sys.stderr)
        return 1
    finally:
        await close_pool()


# -- gc-status --------------------------------------------------------------------

async def _gc_status() -> int:
    pool = await init_db()
    try:
        async with pool.acquire() as conn:
            status = await payloads.gc_status(conn)
    finally:
        await close_pool()
    past = status["candidates_past_grace"]
    print(
        f"GC 宽限期={status['grace_seconds']}s，候选总数={status['total_candidates']}"
        + ("（已过宽限：统计被截断，见 HTTP API）" if past is None
           else f"，已过宽限={past}")
    )
    print(
        f"零引用对象={status['zero_reference_objects']}，"
        f"进行中提交状态机={status['pending_submissions']}"
    )
    for c in status["candidates"]:
        flags = []
        flags.append("已过宽限" if c["past_grace"] else "宽限中")
        flags.append("有元数据" if c["has_metadata"] else "存储孤儿")
        if c["referenced"]:
            flags.append("已被事件引用(将复活)")
        if c["submission_in_flight"]:
            flags.append("提交进行中(将复活)")
        print(
            f"  {c['digest']} first_seen={c['first_seen_at'].isoformat()} "
            f"ref_count={c['ref_count']} [{' / '.join(flags)}]"
        )
    if status["candidates_truncated"]:
        print("  （候选超过 200 条，仅显示最早的 200 条；完整状态见 HTTP API）")
    if status["runs"]:
        print("最近 GC 运行：")
        for r in status["runs"]:
            print(
                f"  #{r['id']} [{r['status']}] candidates={r['candidates']} "
                f"deleted={r['deleted_objects']} rescan_store={r['rescanned_store']}"
                + (f" error={r['last_error']}" if r["last_error"] else "")
            )
    else:
        print("GC 运行：无")
    return 0


# -- entrypoint ------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        prog="payloadadmin", description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_status = sub.add_parser("status", help="对象存储/迁移/GC 状态")
    p_status.add_argument("--verbose", action="store_true")

    p_recover = sub.add_parser("recover", help="续跑崩溃残留的提交状态机")
    p_recover.add_argument("--batch-size", type=int, default=100)

    p_migrate = sub.add_parser("migrate", help="旧 JSONB 事件在线迁移到对象引用")
    p_migrate.add_argument("--batch-size", type=int, default=200)

    p_clear = sub.add_parser(
        "clear-legacy", help="迁移完成后清空事件行内旧 JSONB"
    )
    p_clear.add_argument("--batch-size", type=int, default=500)

    p_gc = sub.add_parser("gc", help="标记/回收零引用载荷对象（宽限+复核）")
    p_gc.add_argument(
        "--grace-seconds", type=int, default=settings.payload_gc_grace_seconds,
        help="引用归零后至少等待多久才删除（默认取 PAYLOAD_GC_GRACE_SECONDS）",
    )
    p_gc.add_argument(
        "--rescan-store", action="store_true",
        help="同时登记对象存储中存在但数据库无元数据的孤儿（下一轮过宽限删除）",
    )
    p_gc.add_argument("--batch-size", type=int, default=100)

    sub.add_parser("gc-status", help="GC 候选/宽限/引用守卫/运行状态")

    args = parser.parse_args()
    if args.command == "status":
        code = asyncio.run(_status(args.verbose))
    elif args.command == "recover":
        code = asyncio.run(_recover(args.batch_size))
    elif args.command == "migrate":
        code = asyncio.run(_migrate(args.batch_size))
    elif args.command == "clear-legacy":
        code = asyncio.run(_clear_legacy(args.batch_size))
    elif args.command == "gc":
        code = asyncio.run(_gc(args.grace_seconds, args.rescan_store, args.batch_size))
    elif args.command == "gc-status":
        code = asyncio.run(_gc_status())
    else:  # pragma: no cover
        parser.error("未知命令")
        code = 2
    sys.exit(code)


if __name__ == "__main__":
    main()
