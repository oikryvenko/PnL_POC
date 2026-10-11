"""Lightweight NYSE session calendar shared by price ingestion and validation."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
from pandas.tseries.holiday import (
    AbstractHolidayCalendar,
    Holiday,
    USLaborDay,
    USMartinLutherKingJr,
    USMemorialDay,
    USPresidentsDay,
    USThanksgivingDay,
    nearest_workday,
)
from pandas.tseries.offsets import CustomBusinessDay, Day, Easter


class _NyseHolidayCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday("New Year's Day", month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        Holiday("Good Friday", month=1, day=1, offset=[Easter(), Day(-2)]),
        USMemorialDay,
        Holiday(
            "Juneteenth National Independence Day",
            month=6,
            day=19,
            start_date="2022-01-01",
            observance=nearest_workday,
        ),
        Holiday("Independence Day", month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday("Christmas Day", month=12, day=25, observance=nearest_workday),
    ]


_NYSE_TRADING_DAY = CustomBusinessDay(calendar=_NyseHolidayCalendar())


def expected_trading_sessions(start: date, end: date) -> list[date]:
    """Return NYSE sessions in the half-open range [start, end)."""
    if start >= end:
        return []
    sessions = pd.date_range(start=start, end=end - timedelta(days=1), freq=_NYSE_TRADING_DAY)
    return [session.date() for session in sessions]


def requested_sessions(start: date, end: date, as_of: date | None = None) -> list[date]:
    """Expected NYSE sessions in the inclusive [start, end] range, excluding future dates."""
    cutoff = as_of or date.today()
    return [s for s in expected_trading_sessions(start, end + timedelta(days=1)) if s <= cutoff]
