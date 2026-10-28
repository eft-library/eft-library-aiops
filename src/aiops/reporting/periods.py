from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

SEOUL = ZoneInfo("Asia/Seoul")
ReportType = Literal["daily", "weekly", "monthly"]


@dataclass(frozen=True)
class Period:
    kind: ReportType
    start: datetime
    end: datetime

    @property
    def label(self):
        return self.start.date().isoformat()

    @property
    def days(self):
        return [
            (self.start + timedelta(days=i)).date().isoformat()
            for i in range((self.end - self.start).days)
        ]

    def as_dict(self):
        return {
            "type": self.kind,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "timezone": "Asia/Seoul",
            "end_exclusive": True,
        }


def for_period(kind: ReportType, target: str | None = None, *, now=None) -> Period:
    now = now or datetime.now(SEOUL)
    if now.tzinfo is None:
        raise ValueError("now must include timezone")
    today = now.astimezone(SEOUL).date()
    chosen = date.fromisoformat(target) if target else today
    if kind == "daily":
        start = chosen if target else today - timedelta(days=1)
        end = start + timedelta(days=1)
    elif kind == "weekly":
        start = chosen - timedelta(days=chosen.weekday())
        if target is None:
            start -= timedelta(days=7)
        end = start + timedelta(days=7)
    elif kind == "monthly":
        start = chosen.replace(day=1)
        if target is None:
            start = (start - timedelta(days=1)).replace(day=1)
        end = date(start.year + (start.month == 12), start.month % 12 + 1, 1)
    else:
        raise ValueError("unsupported report type")
    result = Period(
        kind,
        datetime.combine(start, time.min, SEOUL),
        datetime.combine(end, time.min, SEOUL),
    )
    if result.end > now:
        raise ValueError("only closed calendar periods can be reported")
    return result


def day_for(timestamp: datetime) -> str:
    try:
        return timestamp.astimezone(SEOUL).date().isoformat()
    except (OverflowError, ValueError):
        raise ValueError("timestamp is outside supported calendar range") from None
