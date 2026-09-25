from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
HTML = (ROOT / "index.html").read_text(encoding="utf-8")
CSS = (ROOT / "css" / "app.css").read_text(encoding="utf-8")
JS = (ROOT / "js" / "app.js").read_text(encoding="utf-8")

REQUIRED = {"overview","nodes","objects","repairs","integrity","rebalance","events","policies"}

def test_required_files_exist():
    assert (ROOT / "index.html").is_file()
    assert (ROOT / "css" / "app.css").is_file()
    assert (ROOT / "js" / "app.js").is_file()

def test_every_view_has_navigation():
    nav = set(re.findall(r'data-view="([^"]+)"', HTML))
    views = set(re.findall(r'id="view-([^"]+)"', HTML))
    assert REQUIRED <= nav
    assert REQUIRED <= views

def test_local_stylesheet_exists():
    assert (ROOT / "css" / "app.css").is_file()

def test_api_boundary_is_centralized():
    assert "fetch(" in JS
    assert "fetch(" not in HTML
    assert "const API =" in JS

def test_demo_mode_exists():
    assert 'mode: "mock"' in JS
    assert 'baseUrl: "/api/v1"' in JS

def test_core_vault_concepts_are_visible():
    text = (HTML + CSS + JS).lower()
    for token in ("replica topology", "integrity", "rebalance", "repair", "checksum", "quorum"):
        assert token in text

def test_accessibility_contract():
    assert 'aria-label="Primary navigation"' in HTML
    assert 'aria-label="Search nodes"' in HTML
    assert 'aria-label="Search objects"' in HTML
    assert 'aria-hidden="true"' in HTML

def test_no_frontend_framework_dependency():
    text = (HTML + CSS + JS).lower()
    for framework in ("react", "vue", "angular", "jquery"):
        assert framework not in text

if __name__ == "__main__":
    tests = [v for k, v in globals().items() if k.startswith("test_") and callable(v)]
    for test in tests:
        test()
    print("frontend verifier: %d checks passed" % len(tests))
