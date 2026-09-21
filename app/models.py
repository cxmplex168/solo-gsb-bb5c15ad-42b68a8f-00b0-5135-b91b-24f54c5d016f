"""Pydantic 请求 / 响应模型。"""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator, model_validator


class CreateBarrierRequest(BaseModel):
    participants: list[str] = Field(
        ..., min_length=1, description="屏障参与者标识名单（如主机名、任务 id）"
    )
    threshold: int | None = Field(
        default=None,
        ge=1,
        description="达标所需报到人数；缺省为全体参与者人数",
    )
    deadline: datetime = Field(
        ...,
        description="截止时间，ISO-8601；不带时区偏移时按 UTC 处理",
    )
    creator_id: str | None = Field(default=None, description="创建者标识，可选")

    @model_validator(mode="after")
    def _validate(self) -> "CreateBarrierRequest":
        # 名单去重并保持顺序；空标识无意义
        cleaned: list[str] = []
        for p in self.participants:
            name = p.strip()
            if not name:
                raise ValueError("participants 不得包含空字符串")
            if name not in cleaned:
                cleaned.append(name)
        if not cleaned:
            raise ValueError("participants 至少需要一个成员")
        self.participants = cleaned

        if self.threshold is not None and self.threshold > len(cleaned):
            raise ValueError("threshold 不能大于参与者人数")
        return self

    def deadline_ts(self) -> float:
        d = self.deadline
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc).timestamp()


class BarrierView(BaseModel):
    barrier_id: str
    status: str = Field(description="waiting | reached | cancelled | expired")
    participants: list[str]
    threshold: int
    deadline: datetime
    created_at: datetime
    checked_in: list[str]
    # waiting 阶段的当前报到人数；其余状态为快照人数
    checked_count: int
    # 达标后冻结的成员名单（仅 reached）
    quorum: list[str] | None = None
    frozen_at: datetime | None = None
    # 主动取消的原因与时间（仅 cancelled）
    cancel_reason: str | None = None
    cancelled_at: datetime | None = None
    # 截止时仍未报到的成员（仅 expired）
    missing: list[str] | None = None
    # 达标冻结之后才报到的成员（reached 状态的补充信息，不改写结果）
    late: list[str] | None = None
    creator_id: str | None = None


class CheckinRequest(BaseModel):
    participant_id: str = Field(..., min_length=1, description="参与者标识")


class CancelRequest(BaseModel):
    reason: str = Field(
        ...,
        min_length=1,
        max_length=2000,
        description="取消原因，非空（去首尾空白后仍须非空）",
    )

    @field_validator("reason")
    @classmethod
    def _reason_not_blank(cls, v: str) -> str:
        reason = v.strip()
        if not reason:
            raise ValueError("reason 不得为空白")
        return reason


class CancelResponse(BaseModel):
    barrier_id: str
    status: str = Field(
        description="cancelled（本次取消成功）| already（重复取消的幂等回放）"
    )
    reason: str = Field(description="取消原因；重复取消时返回首次提交的原因")
    cancelled_at: datetime
    # 重复取消（携带不同原因）也返回首次取消结果；true 表示本次为幂等回放
    idempotent: bool = False
    view: BarrierView


class CheckinResponse(BaseModel):
    barrier_id: str
    status: str = Field(
        description=(
            "checked_in（已报到，继续等待）| already_checked_in（幂等重复报到）"
            "| reached（本次报到触发达标）| late（达标后报到，快照未变）"
        )
    )
    triggered: bool
    quorum: list[str] | None = None
    frozen_at: datetime | None = None
    view: BarrierView


class CheckinEvent(BaseModel):
    seq: int = Field(description="事件序号，从 1 开始单调递增，只追加不回改")
    participant_id: str = Field(description="报到的参与者标识")
    result: str = Field(
        description="checked_in | already_checked_in | reached | late"
    )
    ts: datetime = Field(description="事件发生时间（UTC）")


class BarrierEventsPage(BaseModel):
    barrier_id: str
    events: list[CheckinEvent]
    next_cursor: int = Field(
        description="下一页游标（本页最大 seq）；以其为 cursor 继续查询，无新事件时与入参 cursor 相同"
    )
