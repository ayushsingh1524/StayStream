"""
test_alerts.py – Unit tests for the anomaly detection module.
"""
import json
from unittest.mock import patch

from src.streaming.alerts import detect_anomalies, _cooldowns


def _make_row(city, bookings, cancellations, revenue=10000.0):
    """Build a dict mimicking a collected Spark Row.asDict(recursive=True)."""
    return {
        "window": {"start": "2026-11-27T10:00:00", "end": "2026-11-27T10:01:00"},
        "city": city,
        "bookings": bookings,
        "cancellations": cancellations,
        "revenue": revenue,
        "avg_latency_ms": 50.0,
    }


class TestDetectAnomalies:

    def setup_method(self):
        """Clear cooldowns between tests."""
        _cooldowns.clear()

    @patch("src.streaming.alerts._write_alerts")
    @patch("src.streaming.alerts.config")
    def test_high_cancel_rate_triggers_alert(self, mock_config, mock_write):
        mock_config.ALERT_CANCEL_RATE_THRESHOLD = 0.25
        mock_config.ALERT_COOLDOWN_MINUTES = 5
        mock_config.DB_DSN = "host=localhost dbname=test"

        # 3 bookings + 2 cancellations = 40% cancel rate > 25% threshold
        rows = [_make_row("Goa", bookings=3, cancellations=2)]
        detect_anomalies(rows)

        mock_write.assert_called_once()
        alerts = mock_write.call_args[0][0]
        assert len(alerts) == 1
        assert alerts[0][0] == "high_cancellation_rate"
        assert alerts[0][1] == "Goa"
        details = json.loads(alerts[0][4])
        assert details["cancel_rate"] == 0.4

    @patch("src.streaming.alerts._write_alerts")
    @patch("src.streaming.alerts.config")
    def test_low_cancel_rate_no_alert(self, mock_config, mock_write):
        mock_config.ALERT_CANCEL_RATE_THRESHOLD = 0.25
        mock_config.ALERT_COOLDOWN_MINUTES = 5

        # 9 bookings + 1 cancellation = 10% cancel rate < 25% threshold
        rows = [_make_row("Mumbai", bookings=9, cancellations=1)]
        detect_anomalies(rows)

        mock_write.assert_not_called()

    @patch("src.streaming.alerts._write_alerts")
    @patch("src.streaming.alerts.config")
    def test_cooldown_prevents_duplicate_alerts(self, mock_config, mock_write):
        mock_config.ALERT_CANCEL_RATE_THRESHOLD = 0.25
        mock_config.ALERT_COOLDOWN_MINUTES = 5

        rows = [_make_row("Delhi", bookings=2, cancellations=3)]

        # First call: should alert
        detect_anomalies(rows)
        assert mock_write.call_count == 1

        # Second call: should be suppressed by cooldown
        detect_anomalies(rows)
        assert mock_write.call_count == 1  # still 1

    @patch("src.streaming.alerts._write_alerts")
    @patch("src.streaming.alerts.config")
    def test_zero_total_events_no_alert(self, mock_config, mock_write):
        mock_config.ALERT_CANCEL_RATE_THRESHOLD = 0.25
        mock_config.ALERT_COOLDOWN_MINUTES = 5

        rows = [_make_row("Chennai", bookings=0, cancellations=0)]
        detect_anomalies(rows)

        mock_write.assert_not_called()

    @patch("src.streaming.alerts._write_alerts")
    @patch("src.streaming.alerts.config")
    def test_multiple_cities_independent_alerts(self, mock_config, mock_write):
        mock_config.ALERT_CANCEL_RATE_THRESHOLD = 0.25
        mock_config.ALERT_COOLDOWN_MINUTES = 5

        rows = [
            _make_row("Goa", bookings=1, cancellations=4),    # 80% → alert
            _make_row("Mumbai", bookings=9, cancellations=1),  # 10% → no alert
            _make_row("Delhi", bookings=2, cancellations=3),   # 60% → alert
        ]
        detect_anomalies(rows)

        mock_write.assert_called_once()
        alerts = mock_write.call_args[0][0]
        assert len(alerts) == 2
        cities = {a[1] for a in alerts}
        assert cities == {"Goa", "Delhi"}
