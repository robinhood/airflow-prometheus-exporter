import datetime
import pytest
from dateutil import tz

from ..prometheus_exporter import sla_check


def hours_from_now(hours):
    utc_now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    return utc_now + datetime.timedelta(hours=hours)


def sla_time(hours):
    return hours_from_now(hours).astimezone(tz.gettz("America/Los_Angeles")).strftime("%H:%M")


@pytest.mark.parametrize(
    "sla_check_kwargs, expected_result",
    [
        (
            {
                "sla_interval": "1h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-3),
                "cadence": "triggered",
                "latest_sla_miss_state": True
            },
            True,
        ),
        (
            {
                "sla_interval": "1h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-3),
                "cadence": "triggered",
                "latest_sla_miss_state": False
            },
            True,
        ),
        (
            {
                "sla_interval": "1h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-3),
                "cadence": "intraday",
                "latest_sla_miss_state": True
            },
            True,
        ),
        (
            {
                "sla_interval": "1h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-3),
                "cadence": "intraday",
                "latest_sla_miss_state": False
            },
            True,
        ),
        (
            {
                "sla_interval": "3h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-3),
                "cadence": "triggered",
                "latest_sla_miss_state": True
            },
            False,
        ),
        (
            {
                "sla_interval": "3h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-3),
                "cadence": "triggered",
                "latest_sla_miss_state": False
            },
            False,
        ),
        (
            {
                "sla_interval": "3h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-3),
                "cadence": "intraday",
                "latest_sla_miss_state": True
            },
            False,
        ),
        (
            {
                "sla_interval": "3h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-3),
                "cadence": "intraday",
                "latest_sla_miss_state": False
            },
            False,
        ),
        (
            {
                "sla_interval": "1h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-2),
                "cadence": "triggered",
                "latest_sla_miss_state": True
            },
            True,
        ),
        (
            {
                "sla_interval": "1h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-2),
                "cadence": "triggered",
                "latest_sla_miss_state": False
            },
            True,
        ),
        (
            {
                "sla_interval": "1h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-2),
                "cadence": "intraday",
                "latest_sla_miss_state": True
            },
            True,
        ),
        (
            {
                "sla_interval": "1h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-2),
                "cadence": "intraday",
                "latest_sla_miss_state": False
            },
            True,
        ),
        (
            {
                "sla_interval": "3h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-2),
                "cadence": "triggered",
                "latest_sla_miss_state": True
            },
            False,
        ),
        (
            {
                "sla_interval": "3h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-2),
                "cadence": "triggered",
                "latest_sla_miss_state": False
            },
            False,
        ),
        (
            {
                "sla_interval": "3h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-2),
                "cadence": "intraday",
                "latest_sla_miss_state": True
            },
            False,
        ),
        (
            {
                "sla_interval": "3h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-2),
                "cadence": "intraday",
                "latest_sla_miss_state": False
            },
            False,
        ),
        (
            {
                "sla_interval": "3h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-3),
                "cadence": "triggered",
                "latest_sla_miss_state": True
            },
            False,
        ),
        (
            {
                "sla_interval": "3h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-3),
                "cadence": "triggered",
                "latest_sla_miss_state": False
            },
            False,
        ),
        (
            {
                "sla_interval": "3h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-3),
                "cadence": "intraday",
                "latest_sla_miss_state": True
            },
            True,
        ),
        (
            {
                "sla_interval": "3h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-3),
                "cadence": "intraday",
                "latest_sla_miss_state": False
            },
            False,
        ),
    ]
)
def test_sla_check(sla_check_kwargs, expected_result):
    assert sla_check(**sla_check_kwargs) == expected_result

