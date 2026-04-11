import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.akshare_client import fetch_etf_kline, fetch_etf_list, test_kline_availability  # noqa: E402


def _kline_df():
    return pd.DataFrame([
        {
            "date": "2026-04-01",
            "open": 1.0,
            "high": 1.1,
            "low": 0.9,
            "close": 1.05,
            "volume": 1000.0,
            "amount": 1200.0,
        }
    ])


class KlineSourceFallbackTests(unittest.IsolatedAsyncioTestCase):
    @patch("services.akshare_client._fetch_daily_akshare_fallbacks", new_callable=AsyncMock)
    @patch("services.akshare_client._fetch_daily_mootdx_with_diagnostics", new_callable=AsyncMock)
    async def test_kline_probe_marks_fallback_as_available(self, mootdx_mock, fallback_mock):
        mootdx_mock.return_value = (
            None,
            None,
            {
                "ok": False,
                "attempts": [{"server": "119.147.212.81:7709", "error_type": "ResponseHeaderRecvFails"}],
                "summary": "mootdx ResponseHeaderRecvFails (1服)",
            },
        )
        fallback_mock.return_value = (
            _kline_df(),
            "tx",
            {"ok": True, "attempts": [{"source": "tx", "ok": True}]},
        )

        probe = await test_kline_availability()

        self.assertTrue(probe["available"])
        self.assertEqual(probe["effective_source"], "tx")
        self.assertIn("tx fallback", probe["detail"])
        self.assertIn("mootdx", probe["detail"])

    @patch("services.akshare_client._fetch_daily_akshare_fallbacks", new_callable=AsyncMock)
    @patch("services.akshare_client._fetch_daily_mootdx_with_diagnostics", new_callable=AsyncMock)
    async def test_fetch_etf_kline_falls_back_when_mootdx_unavailable(self, mootdx_mock, fallback_mock):
        mootdx_mock.return_value = (
            None,
            None,
            {
                "ok": False,
                "attempts": [{"server": "119.147.212.81:7709", "error_type": "timeout"}],
                "summary": "mootdx timeout (1服)",
            },
        )
        fallback_mock.return_value = (
            _kline_df(),
            "tx",
            {"ok": True, "attempts": [{"source": "tx", "ok": True}]},
        )

        df, source = await fetch_etf_kline("159695", "20260320", "20260410")

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)
        self.assertEqual(source, "tx")

    @patch("services.akshare_client._fetch_etf_list_ths", new_callable=AsyncMock)
    @patch("services.akshare_client._fetch_etf_list_mootdx", new_callable=AsyncMock)
    async def test_fetch_etf_list_falls_back_to_ths(self, mootdx_mock, ths_mock):
        fallback_rows = [
            {"code": "159695", "name": "通信ETF", "market": "sz", "asset_type": "etf"},
            {"code": "512010", "name": "医药ETF", "market": "sh", "asset_type": "etf"},
        ]
        mootdx_mock.return_value = []
        ths_mock.return_value = fallback_rows

        rows = await fetch_etf_list()

        self.assertEqual(rows, fallback_rows)


if __name__ == "__main__":
    unittest.main()