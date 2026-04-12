import sys
from pathlib import Path
import sqlite3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.db import get_enabled_modules

def test_get_enabled_modules():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE app_settings (key TEXT, value TEXT, updated_at TEXT)")
    conn.execute("INSERT INTO app_settings VALUES ('module_qlib_enabled', '1', '2026')")
    conn.execute("INSERT INTO app_settings VALUES ('module_akquant_enabled', '0', '2026')")
    conn.execute("INSERT INTO app_settings VALUES ('module_etf_enabled', '0', '2026')")
    
    modules = get_enabled_modules(conn)
    assert modules["qlib"] is True
    assert modules["akquant"] is False
    assert modules["etf"] is False
    
    conn.close()
