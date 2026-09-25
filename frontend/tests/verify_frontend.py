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
    assert "const API=" in JS

def test_live_contract_is_mapped_to_part_b():
    for route in (
        '"/health"',
        '"/nodes"',
        '"/objects"',
        '"/admin/repair"',
        '"/admin/integrity/check"',
        '"/admin/rebalance"',
        '"/admin/repair/"',
        '"/admin/integrity/check/"',
        '"/admin/rebalance/"',
        '"/objects/"+encoded+"/metadata"',
        '"/objects/"+encoded+"/versions"',
    ):
        assert route in JS, route
    assert 'method:"PUT"' in JS
    assert 'method:"POST"' in JS

def test_no_unsupported_live_object_post():
    assert '"/objects/"+encodeURIComponent(objectName),{method:"POST"' not in JS
    assert '"/objects/"+encodeURIComponent(objectName),{method:"PUT"' in JS

def test_demo_mode_exists():
    # Live API is the production/default mode. Demo mode is an explicit URL opt-in.
    assert 'const mode = params.get("mode")' in HTML
    assert 'params.get("mode") === "mock"' in HTML
    assert 'CONFIG.mode==="mock"' in JS
    assert '/api/v1' in HTML

def test_safe_demo_is_explicit():
    assert "Demo-only control" in JS
    assert "SAFE DEMO SCENARIO" in HTML

def test_live_actions_do_not_mutate_demo_state():
    start = JS.index("async function runAction")
    end = JS.index("async function selectObject")
    block = JS[start:end]
    assert 'if(CONFIG.mode==="api")' in block
    assert "await runDemoAction(name)" in block
    assert "DATA.events.unshift" not in block

def test_job_polling_exists():
    assert "async function pollJob" in JS
    assert "await API.job(name,id)" in JS
    assert "Operation still running" in JS

def test_accessibility_contract():
    for token in (
        'aria-label="Primary navigation"',
        'aria-label="Search nodes"',
        'aria-label="Search objects"',
        'aria-hidden="true"',
        'aria-live="polite"',
        'aria-label="Open command palette"',
    ):
        assert token in HTML

def test_frontend_framework_free():
    text = (HTML + CSS + JS).lower()
    for framework in ("react","vue","angular","jquery"):
        assert framework not in text

def test_command_palette_and_drill_are_connected():
    assert 'data-action="drill"' in HTML
    assert 'data-action="run-drill"' in HTML
    assert 'id="command-modal"' in HTML
    assert "const commands=" in JS
    assert "openCommand" in JS

if __name__ == "__main__":
    tests = [v for k,v in globals().items() if k.startswith("test_") and callable(v)]
    for test in tests:
        test()
    print("frontend verifier: %d checks passed" % len(tests))
