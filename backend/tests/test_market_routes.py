import pytest
from fastapi.testclient import TestClient
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from main import app

client = TestClient(app)

def test_market_status():
    res = client.get("/api/inst/market/status")
    assert res.status_code == 200
    data = res.json()
    assert "current_stocks" in data
    assert "latest_notice_date" in data
