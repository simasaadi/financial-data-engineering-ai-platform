"""
Smoke test for the Streamlit dashboard module.
"""

from pathlib import Path


def test_streamlit_dashboard_file_exists() -> None:
    assert Path("dashboards/streamlit_app.py").exists()


def test_streamlit_dashboard_contains_main_function() -> None:
    dashboard_code = Path("dashboards/streamlit_app.py").read_text(encoding="utf-8")
    assert "def main()" in dashboard_code
    assert "Financial Data Engineering AI Platform" in dashboard_code
