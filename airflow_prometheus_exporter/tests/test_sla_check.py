import datetime
import pytest
from dateutil import tz

from ..prometheus_exporter import sla_check


def hours_from_now(hours):
    utc_now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    return utc_now + datetime.timedelta(hours=hours)


def sla_time(hours):
    return (
        hours_from_now(hours)
        .astimezone(tz.gettz("America/Los_Angeles"))
        .strftime("%H:%M")
    )


@pytest.mark.parametrize(
    "sla_check_kwargs, expected_result",
    [
        (
            # 0.
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-4),
                "latest_sla_miss_state": True,
            },
            True,
        ),
        (
            # 1.
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-4),
                "latest_sla_miss_state": False,
            },
            False,
        ),
        (
            # 2. First check on a new alert
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-4),
                "latest_sla_miss_state": None,
            },
            False,
        ),
        (
            # 3.
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-1),
                "latest_sla_miss_state": True,
            },
            True,
        ),
        (
            # 4.
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-1),
                "latest_sla_miss_state": False,
            },
            False,
        ),
        (
            # 5. First check on a new alert
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-1),
                "latest_sla_miss_state": None,
            },
            False,
        ),
    ],
)
def test_sla_check_before_sla_time(sla_check_kwargs, expected_result):
    assert sla_check(**sla_check_kwargs) == expected_result
    

@pytest.mark.parametrize(
    "sla_check_kwargs, expected_result",
    [
        (
            # 0. max_execution_date -> checkpoint -> now
            {
                "sla_interval": "2h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-4),
                "latest_sla_miss_state": True,
            },
            True,
        ),
        (
            # 1. max_execution_date -> checkpoint -> now
            {
                "sla_interval": "2h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-4),
                "latest_sla_miss_state": False,
            },
            True,
        ),
        (
            # 2. max_execution_date -> checkpoint -> now
            {
                "sla_interval": "2h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-4),
                "latest_sla_miss_state": None,
            },
            True,
        ),
        (
            # 3. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-1),
                "latest_sla_miss_state": True,
            },
            False,
        ),
        (
            # 4. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-1),
                "latest_sla_miss_state": False,
            },
            False,
        ),
        (
            # 5. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-1),
                "latest_sla_miss_state": None,
            },
            False,
        ),
        (
            # 6. max_execution_date -> checkpoint -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-4),
                "latest_sla_miss_state": True,
            },
            True,
        ),
        (
            # 7. max_execution_date -> checkpoint -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-4),
                "latest_sla_miss_state": False,
            },
            True,
        ),
        (
            # 8. max_execution_date -> checkpoint -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-4),
                "latest_sla_miss_state": None,
            },
            True,
        ),
        (
            # 9. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-1),
                "latest_sla_miss_state": True,
            },
            False,
        ),
        (
            # 10. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-1),
                "latest_sla_miss_state": False,
            },
            False,
        ),
        (
            # 11. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-1),
                "latest_sla_miss_state": None,
            },
            False,
        ),
    ],
)
def test_sla_check_after_sla_time(sla_check_kwargs, expected_result):
    assert sla_check(**sla_check_kwargs) == expected_result
