import sys
from pathlib import Path
import pytest
from unittest.mock import patch, AsyncMock
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from main import app
from routers.updater import check_connectivity

client = TestClient(app)

@pytest.mark.asyncio
async def test_api_check_connectivity():
    with patch("routers.updater.check_connectivity", new_callable=AsyncMock) as mock_comp:
        mock_comp.return_value = {
            "all_healthy": True,
            "reports": {"em": True},
            "holdings": {"sina": True},
            "kline": {"tx": True},
            "industry": {"sw": True}
        }
        res = client.get("/api/inst/update/connectivity")
        assert res.status_code == 200
        data = res.json()
        assert data["all_healthy"] is True
