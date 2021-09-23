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
        # Group 1:
        #    - After SLA time or SLA time is None
        #    - return max_execution_date < checkpoint
        (
            # 0. max_execution_date -> checkpoint -> now
            {
                "sla_interval": "2h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-4),
                "cadence": "intraday",
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
                "cadence": "intraday",
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
                "cadence": "intraday",
                "latest_sla_miss_state": None,
            },
            True,
        ),
        (
            # 3. max_execution_date -> checkpoint -> now
            {
                "sla_interval": "2h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-4),
                "cadence": "triggered",
                "latest_sla_miss_state": True,
            },
            True,
        ),
        (
            # 4. max_execution_date -> checkpoint -> now
            {
                "sla_interval": "2h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-4),
                "cadence": "triggered",
                "latest_sla_miss_state": False,
            },
            True,
        ),
        (
            # 5. max_execution_date -> checkpoint -> now
            {
                "sla_interval": "2h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-4),
                "cadence": "triggered",
                "latest_sla_miss_state": None,
            },
            True,
        ),
        (
            # 6. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-1),
                "cadence": "intraday",
                "latest_sla_miss_state": True,
            },
            False,
        ),
        (
            # 7. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-1),
                "cadence": "intraday",
                "latest_sla_miss_state": False,
            },
            False,
        ),
        (
            # 8. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-1),
                "cadence": "intraday",
                "latest_sla_miss_state": None,
            },
            False,
        ),
        (
            # 9. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-1),
                "cadence": "triggered",
                "latest_sla_miss_state": True,
            },
            False,
        ),
        (
            # 10. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-1),
                "cadence": "triggered",
                "latest_sla_miss_state": False,
            },
            False,
        ),
        (
            # 11. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": sla_time(-1),
                "max_execution_date": hours_from_now(-1),
                "cadence": "triggered",
                "latest_sla_miss_state": None,
            },
            False,
        ),
        (
            # 12. max_execution_date -> checkpoint -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-4),
                "cadence": "intraday",
                "latest_sla_miss_state": True,
            },
            True,
        ),
        (
            # 13. max_execution_date -> checkpoint -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-4),
                "cadence": "intraday",
                "latest_sla_miss_state": False,
            },
            True,
        ),
        (
            # 14. max_execution_date -> checkpoint -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-4),
                "cadence": "intraday",
                "latest_sla_miss_state": None,
            },
            True,
        ),
        (
            # 15. max_execution_date -> checkpoint -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-4),
                "cadence": "triggered",
                "latest_sla_miss_state": True,
            },
            True,
        ),
        (
            # 16. max_execution_date -> checkpoint -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-4),
                "cadence": "triggered",
                "latest_sla_miss_state": False,
            },
            True,
        ),
        (
            # 17. max_execution_date -> checkpoint -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-4),
                "cadence": "triggered",
                "latest_sla_miss_state": None,
            },
            True,
        ),
        (
            # 18. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-1),
                "cadence": "intraday",
                "latest_sla_miss_state": True,
            },
            False,
        ),
        (
            # 19. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-1),
                "cadence": "intraday",
                "latest_sla_miss_state": False,
            },
            False,
        ),
        (
            # 20. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-1),
                "cadence": "intraday",
                "latest_sla_miss_state": None,
            },
            False,
        ),
        (
            # 21. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-1),
                "cadence": "triggered",
                "latest_sla_miss_state": True,
            },
            False,
        ),
        (
            # 22. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-1),
                "cadence": "triggered",
                "latest_sla_miss_state": False,
            },
            False,
        ),
        (
            # 23. checkpoint -> max_execution_date -> now
            {
                "sla_interval": "2h",
                "sla_time": None,
                "max_execution_date": hours_from_now(-1),
                "cadence": "triggered",
                "latest_sla_miss_state": None,
            },
            False,
        ),
        #Group 2. 
        #    - Before SLA time and not triggered DAG
        #    - return latest_sla_miss_state or False
        (
            # 24.
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-4),
                "cadence": "intraday",
                "latest_sla_miss_state": True,
            },
            True,
        ),
        (
            # 25.
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-4),
                "cadence": "intraday",
                "latest_sla_miss_state": False,
            },
            False,
        ),
        (
            # 26. First check on a new alert
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-4),
                "cadence": "intraday",
                "latest_sla_miss_state": None,
            },
            False,
        ),
        (
            # 27.
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-1),
                "cadence": "intraday",
                "latest_sla_miss_state": True,
            },
            True,
        ),
        (
            # 28.
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-1),
                "cadence": "intraday",
                "latest_sla_miss_state": False,
            },
            False,
        ),
        (
            # 29. First check on a new alert
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-1),
                "cadence": "intraday",
                "latest_sla_miss_state": None,
            },
            False,
        ),
        #Group 3. 
        #    - Everything else
        #    - return False
        (
            # 30.
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-4),
                "cadence": "triggered",
                "latest_sla_miss_state": True,
            },
            False,
        ),
        (
            # 31.
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-4),
                "cadence": "triggered",
                "latest_sla_miss_state": False,
            },
            False,
        ),
        (
            # 32.
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-4),
                "cadence": "triggered",
                "latest_sla_miss_state": None,
            },
            False,
        ),
        (
            # 33.
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-1),
                "cadence": "triggered",
                "latest_sla_miss_state": True,
            },
            False,
        ),
        (
            # 34.
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-1),
                "cadence": "triggered",
                "latest_sla_miss_state": False,
            },
            False,
        ),
        (
            # 35.
            {
                "sla_interval": "2h",
                "sla_time": sla_time(1),
                "max_execution_date": hours_from_now(-1),
                "cadence": "triggered",
                "latest_sla_miss_state": None,
            },
            False,
        ),
    ],
)
def test_sla_check(sla_check_kwargs, expected_result):
    assert sla_check(**sla_check_kwargs) == expected_result
