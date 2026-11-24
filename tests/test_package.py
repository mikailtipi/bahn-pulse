import json
from pathlib import Path

import bahn_pulse

FIXTURES = Path(__file__).parent / "fixtures"


def test_package_has_version() -> None:
    assert bahn_pulse.__version__


def test_openboards_fixtures_are_valid_json() -> None:
    files = sorted((FIXTURES / "openboards").glob("*.json"))
    assert files, "expected OpenBoards sample responses in tests/fixtures/openboards"
    for path in files:
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert "departures" in payload
