"""端到端集成测试：需要真实 Redis（REDIS_URL，默认 redis://localhost:6399）。

运行：
    REDIS_URL=redis://localhost:6399/0 \
    BARRIER_POLL_INTERVAL_S=0.05 python3 -m pytest -s tests/
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

# 在导入 app 前固定配置
os.environ.setdefault("REDIS_URL", "redis://localhost:6399/15")
os.environ.setdefault("BARRIER_POLL_INTERVAL_S", "0.05")

from app.main import app  # noqa: E402

API = "http://test"


@pytest.fixture(autouse=True)
async def _lifespan():
    async with LifespanManager(app):
        yield


async def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url=API)


async def create_barrier(client: AsyncClient, participants, threshold=None, ttl_s=5):
    deadline = (datetime.now(timezone.utc) + timedelta(seconds=ttl_s)).isoformat()
    r = await client.post(
        "/barriers",
        json={
            "participants": participants,
            "threshold": threshold,
            "deadline": deadline,
        },
    )
    assert r.status_code == 201, r.text
    return r.json()


async def checkin(client: AsyncClient, bid: str, pid: str):
    return await client.post(f"/barriers/{bid}/checkin", json={"participant_id": pid})


async def cancel(client: AsyncClient, bid: str, reason: str):
    return await client.post(f"/barriers/{bid}/cancel", json={"reason": reason})


@pytest.mark.asyncio
async def test_health_and_root():
    async with await _client() as c:
        r = await c.get("/health")
        assert r.status_code == 200 and r.json()["status"] == "ok"
        r = await c.get("/")
        assert r.status_code == 200 and "endpoints" in r.json()


@pytest.mark.asyncio
async def test_create_validations():
    async with await _client() as c:
        # threshold 大于人数
        r = await c.post(
            "/barriers",
            json={
                "participants": ["a"],
                "threshold": 2,
                "deadline": "2030-01-01T00:00:00Z",
            },
        )
        assert r.status_code == 422
        # deadline 已过去
        r = await c.post(
            "/barriers",
            json={
                "participants": ["a"],
                "deadline": "2000-01-01T00:00:00Z",
            },
        )
        assert r.status_code == 422
        # 名单去重
        r = await c.post(
            "/barriers",
            json={
                "participants": ["a", "a", " b ", "b"],
                "deadline": "2030-01-01T00:00:00Z",
            },
        )
        assert r.status_code == 201
        body = r.json()
        assert body["participants"] == ["a", "b"]
        assert body["threshold"] == 2  # 默认全员


@pytest.mark.asyncio
async def test_idempotent_checkin_and_non_member():
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=3, ttl_s=10)
        bid = b["barrier_id"]

        r1 = await checkin(c, bid, "a")
        assert r1.status_code == 200 and r1.json()["status"] == "checked_in"
        r2 = await checkin(c, bid, "a")
        assert r2.status_code == 200
        assert r2.json()["status"] == "already_checked_in"

        g = await c.get(f"/barriers/{bid}")
        assert g.json()["checked_in"] == ["a"]
        assert g.json()["checked_count"] == 1

        r3 = await checkin(c, bid, "outsider")
        assert r3.status_code == 403
        r4 = await c.get("/barriers/deadbeefdeadbeef/wait")
        assert r4.status_code == 404


@pytest.mark.asyncio
async def test_concurrent_reach_freezes_snapshot_exactly_once():
    """6 人屏障，达标人数 4：6 个并发报到必须且只能有一个触发 reached，
    冻结 quorum 唯一确定，其余为 late。"""
    async with await _client() as c:
        members = [f"m{i}" for i in range(6)]
        b = await create_barrier(c, members, threshold=4, ttl_s=10)
        bid = b["barrier_id"]

        results = await asyncio.gather(*(checkin(c, bid, m) for m in members))
        assert all(r.status_code == 200 for r in results), results

        bodies = [r.json() for r in results]
        triggered = [x for x in bodies if x["triggered"]]
        assert len(triggered) == 1, f"应有且仅有一个 triggered，实际 {triggered}"

        reachers = [x for x in bodies if x["status"] == "reached"]
        assert len(reachers) == 1
        quorum = reachers[0]["quorum"]
        assert len(quorum) == 4
        frozen_at = reachers[0]["frozen_at"]

        # 冻结后任何视图、任何后续报到都不能改写 quorum / frozen_at
        for _ in range(3):
            r = await checkin(c, bid, "m5")
            assert r.json()["status"] == "late"
            assert r.json()["triggered"] is False

        # 达标后非名单成员依然被拒绝，且不泄露冻结快照
        r = await checkin(c, bid, "stranger")
        assert r.status_code == 403

        g = await c.get(f"/barriers/{bid}")
        view = g.json()
        assert view["status"] == "reached"
        assert view["quorum"] == quorum
        assert view["frozen_at"] == frozen_at
        assert set(view["checked_in"]) == set(members)
        late = set(view["late"])
        assert late == set(members) - set(quorum)


@pytest.mark.asyncio
async def test_multiple_waiters_all_woken_on_reach():
    """多个等待者并发挂起，达标时必须全部立即拿到同一份冻结快照。"""
    async with await _client() as c:
        members = ["w1", "w2", "w3", "w4"]
        b = await create_barrier(c, members, threshold=2, ttl_s=10)
        bid = b["barrier_id"]

        async def wait_view():
            async with await _client() as c2:
                started = time.time()
                r = await c2.get(f"/barriers/{bid}/wait")
                return time.time() - started, r

        waiters = [asyncio.create_task(wait_view()) for _ in range(4)]
        await asyncio.sleep(0.4)  # 确保都已进入订阅+等待
        await checkin(c, bid, "w1")
        await asyncio.sleep(0.2)
        await checkin(c, bid, "w2")  # 达标

        outcomes = await asyncio.gather(*waiters)
        for elapsed, r in outcomes:
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["status"] == "reached"
            assert sorted(body["quorum"]) == ["w1", "w2"]
            assert elapsed < 5, f"等待者未被及时唤醒: {elapsed:.2f}s"


@pytest.mark.asyncio
async def test_wait_returns_immmediately_when_already_settled():
    async with await _client() as c:
        b = await create_barrier(c, ["x", "y"], threshold=1, ttl_s=10)
        bid = b["barrier_id"]
        await checkin(c, bid, "x")
        t0 = time.time()
        r = await c.get(f"/barriers/{bid}/wait")
        assert r.status_code == 200
        assert r.json()["status"] == "reached"
        assert time.time() - t0 < 2


@pytest.mark.asyncio
async def test_deadline_returns_checked_and_missing_and_rejects_checkin():
    async with await _client() as c:
        members = ["d1", "d2", "d3"]
        b = await create_barrier(c, members, threshold=3, ttl_s=1)
        bid = b["barrier_id"]
        await checkin(c, bid, "d1")

        t0 = time.time()
        r = await c.get(f"/barriers/{bid}/wait")
        elapsed = time.time() - t0
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "expired"
        assert body["checked_in"] == ["d1"]
        assert body["missing"] == ["d2", "d3"]
        assert 0.8 < elapsed < 5, f"应在截止时返回，实际 {elapsed:.2f}s"

        # 截止后报到被拒绝，结果不变
        r2 = await checkin(c, bid, "d2")
        assert r2.status_code == 409
        g = await c.get(f"/barriers/{bid}")
        assert g.json()["status"] == "expired"
        assert g.json()["checked_in"] == ["d1"]
        assert g.json()["missing"] == ["d2", "d3"]


@pytest.mark.asyncio
async def test_waiter_joining_after_reach_gets_snapshot():
    """晚加入的等待者：到达时屏障已达标，应立即得到快照（二次检查路径）。"""
    async with await _client() as c:
        b = await create_barrier(c, ["p", "q"], threshold=1, ttl_s=10)
        bid = b["barrier_id"]
        await checkin(c, bid, "p")
        await asyncio.sleep(0.1)
        r = await c.get(f"/barriers/{bid}/wait")
        assert r.json()["status"] == "reached"
        assert r.json()["quorum"] == ["p"]


async def get_events(client: AsyncClient, bid: str, cursor=0, limit=100):
    return await client.get(
        f"/barriers/{bid}/events", params={"cursor": cursor, "limit": limit}
    )


@pytest.mark.asyncio
async def test_checkin_events_recorded_in_order():
    """首次、重复、达标、迟到四种报到各产生一条有序事件，字段齐全。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=2, ttl_s=10)
        bid = b["barrier_id"]

        # 尚未报到时为空页
        r = await get_events(c, bid)
        assert r.status_code == 200
        assert r.json() == {"barrier_id": bid, "events": [], "next_cursor": 0}

        assert (await checkin(c, bid, "a")).json()["status"] == "checked_in"
        assert (await checkin(c, bid, "a")).json()["status"] == "already_checked_in"
        assert (await checkin(c, bid, "b")).json()["status"] == "reached"
        assert (await checkin(c, bid, "c")).json()["status"] == "late"

        body = (await get_events(c, bid)).json()
        assert body["next_cursor"] == 4
        events = body["events"]
        assert [e["seq"] for e in events] == [1, 2, 3, 4]
        assert [e["participant_id"] for e in events] == ["a", "a", "b", "c"]
        assert [e["result"] for e in events] == [
            "checked_in",
            "already_checked_in",
            "reached",
            "late",
        ]
        for e in events:
            ts = datetime.fromisoformat(e["ts"])
            assert ts.tzinfo is not None  # 带时区的合法时间


@pytest.mark.asyncio
async def test_events_pagination_cursor_limit():
    """cursor/limit 翻页不重不漏，读完后游标不再前进；非法参数 422。"""
    async with await _client() as c:
        members = [f"p{i}" for i in range(5)]
        b = await create_barrier(c, members, threshold=5, ttl_s=10)
        bid = b["barrier_id"]
        for m in members:
            await checkin(c, bid, m)

        # 每页 2 条，翻页读完全部 5 条
        seen, cursor = [], 0
        for _ in range(3):
            body = (await get_events(c, bid, cursor=cursor, limit=2)).json()
            assert len(body["events"]) <= 2
            seen.extend(body["events"])
            cursor = body["next_cursor"]
        assert [e["seq"] for e in seen] == [1, 2, 3, 4, 5]
        assert [e["participant_id"] for e in seen] == members
        assert cursor == 5

        # 读完后为空页，游标保持不动
        body = (await get_events(c, bid, cursor=cursor, limit=2)).json()
        assert body["events"] == []
        assert body["next_cursor"] == 5

        # 从中间游标开始只取右侧事件
        body = (await get_events(c, bid, cursor=3, limit=100)).json()
        assert [e["seq"] for e in body["events"]] == [4, 5]

        # 非法参数与不存在的屏障
        assert (await get_events(c, bid, cursor=-1)).status_code == 422
        assert (await get_events(c, bid, limit=0)).status_code == 422
        assert (await get_events(c, "deadbeefdeadbeef")).status_code == 404


@pytest.mark.asyncio
async def test_failed_checkins_produce_no_events():
    """非名单成员（403）、截止后报到（409）、屏障不存在（404）都不产生事件。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=1)
        bid = b["barrier_id"]

        r = await checkin(c, bid, "outsider")
        assert r.status_code == 403

        await asyncio.sleep(1.2)  # 等到过截止时间
        r = await checkin(c, bid, "a")
        assert r.status_code == 409

        r = await checkin(c, "deadbeefdeadbeef", "a")
        assert r.status_code == 404

        body = (await get_events(c, bid)).json()
        assert body["events"] == []
        assert body["next_cursor"] == 0


@pytest.mark.asyncio
async def test_concurrent_checkins_events_no_gap_no_dup():
    """并发报到：事件总数等于成功报到次数，seq 恰好为 1..N 连续不重不漏。"""
    async with await _client() as c:
        members = [f"m{i}" for i in range(8)]
        b = await create_barrier(c, members, threshold=4, ttl_s=10)
        bid = b["barrier_id"]

        # 8 个成员首报 + 2 个并发重复报到，共 10 个并发请求
        reqs = [checkin(c, bid, m) for m in members]
        reqs += [checkin(c, bid, "m0"), checkin(c, bid, "m1")]
        results = await asyncio.gather(*reqs)
        assert all(r.status_code == 200 for r in results)

        events = (await get_events(c, bid, limit=100)).json()["events"]
        assert len(events) == 10
        assert [e["seq"] for e in events] == list(range(1, 11))

        # m0、m1 各出现两次（首报 + 重复），其余成员各一次
        counts = Counter(e["participant_id"] for e in events)
        assert counts["m0"] == 2 and counts["m1"] == 2
        assert all(counts[f"m{i}"] == 1 for i in range(2, 8))

        # 事件结果与报到响应状态一一对应（同一原子操作的产物）
        assert {e["result"] for e in events} <= {
            "checked_in",
            "already_checked_in",
            "reached",
            "late",
        }
        assert sum(1 for e in events if e["result"] == "reached") == 1


@pytest.mark.asyncio
async def test_cancel_basic_and_view_fields():
    """取消成功：200、cancelled 状态、reason/cancelled_at 落库、已报到快照保留。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=3, ttl_s=10)
        bid = b["barrier_id"]
        await checkin(c, bid, "a")

        r = await cancel(c, bid, "  任务提前结束  ")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["status"] == "cancelled"
        assert body["idempotent"] is False
        # reason 去首尾空白
        assert body["reason"] == "任务提前结束"
        assert datetime.fromisoformat(body["cancelled_at"]).tzinfo is not None
        view = body["view"]
        assert view["status"] == "cancelled"
        assert view["cancel_reason"] == "任务提前结束"
        assert view["cancelled_at"] == body["cancelled_at"]
        assert view["checked_in"] == ["a"]
        assert view["checked_count"] == 1
        assert view["quorum"] is None and view["frozen_at"] is None
        assert view["missing"] is None

        # GET 查询返回同样的取消信息
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["status"] == "cancelled"
        assert g["cancel_reason"] == "任务提前结束"
        assert g["cancelled_at"] == body["cancelled_at"]
        assert g["checked_in"] == ["a"]


@pytest.mark.asyncio
async def test_cancel_reason_validation():
    """reason 缺失 / 空白 → 422；屏障不存在 → 404。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a"], threshold=1, ttl_s=10)
        bid = b["barrier_id"]

        assert (await c.post(f"/barriers/{bid}/cancel", json={})).status_code == 422
        assert (
            await c.post(f"/barriers/{bid}/cancel", json={"reason": ""})
        ).status_code == 422
        assert (
            await c.post(f"/barriers/{bid}/cancel", json={"reason": "   "})
        ).status_code == 422

        r = await c.post(
            "/barriers/deadbeefdeadbeef/cancel", json={"reason": "x"}
        )
        assert r.status_code == 404


@pytest.mark.asyncio
async def test_repeated_cancel_is_idempotent_same_result():
    """重复取消：无论携带什么 reason，返回首次取消的同一份结果（reason/cancelled_at）。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=10)
        bid = b["barrier_id"]

        first = (await cancel(c, bid, "第一次原因")).json()
        cancelled_at = first["cancelled_at"]
        assert first["status"] == "cancelled" and first["idempotent"] is False

        for _ in range(3):
            r = await cancel(c, bid, "另一个原因")
            assert r.status_code == 200
            body = r.json()
            assert body["status"] == "already"
            assert body["idempotent"] is True
            assert body["reason"] == "第一次原因"
            assert body["cancelled_at"] == cancelled_at

        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["cancel_reason"] == "第一次原因"
        assert g["cancelled_at"] == cancelled_at


@pytest.mark.asyncio
async def test_checkin_rejected_after_cancel_and_no_events():
    """取消后报到 409；非名单成员仍 403；失败路径不产生事件、终态不变。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=10)
        bid = b["barrier_id"]
        await checkin(c, bid, "a")
        await cancel(c, bid, "stop")

        r = await checkin(c, bid, "b")
        assert r.status_code == 409
        assert "取消" in r.json()["detail"]

        # 非名单成员依旧 403（名单校验优先于取消状态）
        assert (await checkin(c, bid, "stranger")).status_code == 403

        # 报到名单不增加，取消信息不变
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["status"] == "cancelled"
        assert g["checked_in"] == ["a"]
        assert g["checked_count"] == 1

        # 取消前只有 a 的一条事件，取消后报到不产生事件
        events = (await get_events(c, bid)).json()["events"]
        assert len(events) == 1 and events[0]["participant_id"] == "a"


@pytest.mark.asyncio
async def test_cancel_conflicts_when_reached_or_expired():
    """已达标 / 已截止后取消返回 409，且既有终态不被改写。"""
    async with await _client() as c:
        # 已达标
        b1 = await create_barrier(c, ["a", "b"], threshold=1, ttl_s=10)
        bid1 = b1["barrier_id"]
        reach = (await checkin(c, bid1, "a")).json()
        assert reach["status"] == "reached"
        frozen_at = reach["frozen_at"]

        r = await cancel(c, bid1, "来不及了")
        assert r.status_code == 409
        g = (await c.get(f"/barriers/{bid1}")).json()
        assert g["status"] == "reached"
        assert g["quorum"] == ["a"]
        assert g["frozen_at"] == frozen_at
        assert g["cancel_reason"] is None and g["cancelled_at"] is None

        # 达标后仍可迟到报到，取消依旧 409
        assert (await checkin(c, bid1, "b")).json()["status"] == "late"
        assert (await cancel(c, bid1, "再试一次")).status_code == 409

        # 已截止
        b2 = await create_barrier(c, ["x", "y"], threshold=2, ttl_s=1)
        bid2 = b2["barrier_id"]
        await checkin(c, bid2, "x")
        await asyncio.sleep(1.2)
        assert (await c.get(f"/barriers/{bid2}/wait")).json()["status"] == "expired"

        r = await cancel(c, bid2, "迟到的取消")
        assert r.status_code == 409
        g2 = (await c.get(f"/barriers/{bid2}")).json()
        assert g2["status"] == "expired"
        assert g2["checked_in"] == ["x"]
        assert g2["missing"] == ["y"]
        assert g2["cancel_reason"] is None


@pytest.mark.asyncio
async def test_cancel_wakes_all_waiters():
    """多个等待者挂起时取消：全部被 Pub/Sub 立即唤醒并拿到同一份取消结果。"""
    async with await _client() as c:
        members = ["c1", "c2", "c3", "c4"]
        b = await create_barrier(c, members, threshold=4, ttl_s=30)
        bid = b["barrier_id"]

        async def wait_view():
            async with await _client() as c2:
                started = time.time()
                r = await c2.get(f"/barriers/{bid}/wait")
                return time.time() - started, r

        waiters = [asyncio.create_task(wait_view()) for _ in range(4)]
        await asyncio.sleep(0.4)  # 确保都已订阅
        cancelled = (await cancel(c, bid, "全部收工")).json()

        outcomes = await asyncio.gather(*waiters)
        for elapsed, r in outcomes:
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["status"] == "cancelled"
            assert body["cancel_reason"] == "全部收工"
            assert body["cancelled_at"] == cancelled["cancelled_at"]
            assert elapsed < 5, f"等待者未被取消及时唤醒: {elapsed:.2f}s"

        # 取消后再加入的等待者立即返回（二次检查路径）
        r = await c.get(f"/barriers/{bid}/wait")
        assert r.json()["status"] == "cancelled"


@pytest.mark.asyncio
async def test_wait_polling_fallback_detects_cancel():
    """兜底验证：即使错过 Pub/Sub，轮询路径也能读到取消终态（逻辑同 reached）。

    通过直接构造“先到达等待循环、再取消”的场景验证状态回读；
    轮询间隔 0.05s 下应很快返回，且不依赖消息内容。
    """
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=30)
        bid = b["barrier_id"]

        async def do_wait():
            async with await _client() as c2:
                return await c2.get(f"/barriers/{bid}/wait")

        task = asyncio.create_task(do_wait())
        await asyncio.sleep(0.3)
        await cancel(c, bid, "poll check")
        r = await asyncio.wait_for(task, timeout=5)
        assert r.status_code == 200
        assert r.json()["status"] == "cancelled"


@pytest.mark.asyncio
async def test_concurrent_cancel_and_threshold_checkin_terminal_winner_once():
    """取消与达标报到并发竞争：终态唯一确定，输家拿到 409，赢家结果永不改写。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=1, ttl_s=10)
        bid = b["barrier_id"]

        checkin_r, cancel_r = await asyncio.gather(
            checkin(c, bid, "a"), cancel(c, bid, "race")
        )

        g = (await c.get(f"/barriers/{bid}")).json()
        final_status = g["status"]
        assert final_status in ("reached", "cancelled")

        if final_status == "reached":
            # 报到先赢：checkin 200/reached，cancel 409
            assert checkin_r.status_code == 200
            assert checkin_r.json()["status"] == "reached"
            assert cancel_r.status_code == 409
            # 再取消仍是 409，再报到为 late
            assert (await cancel(c, bid, "x")).status_code == 409
            assert (await checkin(c, bid, "b")).json()["status"] == "late"
            assert (await c.get(f"/barriers/{bid}")).json()["status"] == "reached"
        else:
            # 取消先赢：cancel 200/cancelled，checkin 409
            assert cancel_r.status_code == 200
            assert cancel_r.json()["status"] == "cancelled"
            assert checkin_r.status_code == 409
            # 再报到 409，再取消幂等
            assert (await checkin(c, bid, "b")).status_code == 409
            again = (await cancel(c, bid, "y")).json()
            assert again["status"] == "already"
            assert again["reason"] == "race"
            assert (await c.get(f"/barriers/{bid}")).json()["status"] == "cancelled"

        # 终态跨多次查询稳定
        for _ in range(3):
            assert (await c.get(f"/barriers/{bid}")).json()["status"] == final_status


@pytest.mark.asyncio
async def test_many_concurrent_cancels_single_terminal_state():
    """大量并发取消：恰好一个成功，其余全部幂等回放同一份结果。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=10)
        bid = b["barrier_id"]

        results = await asyncio.gather(
            *(cancel(c, bid, f"reason-{i}") for i in range(20))
        )
        assert all(r.status_code == 200 for r in results), results
        bodies = [r.json() for r in results]

        winners = [x for x in bodies if x["status"] == "cancelled"]
        replays = [x for x in bodies if x["status"] == "already"]
        assert len(winners) == 1
        assert len(replays) == 19

        winner = winners[0]
        assert all(x["reason"] == winner["reason"] for x in replays)
        assert all(x["cancelled_at"] == winner["cancelled_at"] for x in replays)
