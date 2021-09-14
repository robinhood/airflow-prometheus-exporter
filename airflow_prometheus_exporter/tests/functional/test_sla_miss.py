import datetime

from ...prometheus_exporter import sla_check


def test_sla_check():

    kwargs = {
        "sla_interval": "2h",
        "sla_time": "9:00",
        "cadence": "intraday",
        "max_execution_date": datetime.datetime.utcnow().replace(
            tzinfo=datetime.timezone.utc
        ),
        "latest_sla_miss_state": True,
    }
    s = sla_check(**kwargs)
    print(s)
    assert s == False
