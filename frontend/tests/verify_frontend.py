from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
HTML = (ROOT / "index.html").read_text(encoding="utf-8")
CSS = (ROOT / "css" / "app.css").read_text(encoding="utf-8")
JS = (ROOT / "js" / "app.js").read_text(encoding="utf-8")
CONFIG = (ROOT / "js" / "config.js").read_text(encoding="utf-8")

REQUIRED = {
    "overview",
    "nodes",
    "objects",
    "repairs",
    "integrity",
    "rebalance",
    "events",
    "policies",
    "guide",
    "about",
}


def test_required_files_exist():
    assert (ROOT / "index.html").is_file()
    assert (ROOT / "css" / "app.css").is_file()
    assert (ROOT / "js" / "app.js").is_file()
    assert (ROOT / "js" / "config.js").is_file()
    assert (ROOT / "favicon.svg").is_file()


def test_every_view_has_navigation():
    nav = set(re.findall(r'data-view="([^"]+)"', HTML))
    views = set(re.findall(r'id="view-([^"]+)"', HTML))
    assert REQUIRED <= nav
    assert REQUIRED <= views


def test_live_mode_is_the_only_runtime_mode():
    assert 'window.VAULT_CONFIG = { mode: "api", baseUrl };' in CONFIG
    assert "requestedMode" not in CONFIG
    assert "hostedStatic" not in CONFIG
    assert "mock" not in CONFIG.lower()
    assert 'mode==="mock"' not in JS
    assert "runDemoAction" not in JS
    assert "SAFE DEMO SCENARIO" not in HTML
    assert "Demo-only control" not in JS
    assert 'data-action="drill"' not in HTML


def test_api_boundary_is_centralized():
    assert "fetch(" in JS
    assert "fetch(" not in HTML
    assert "const API=" in JS


def test_live_contract_is_mapped_to_part_b():
    for route in (
        '"/health"',
        '"/nodes"',
        '"/objects"',
        '"/policies"',
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
    assert 'X-Admin-Key' in JS
    assert "async jobs(name)" in JS


def test_object_catalog_uses_backend_summary_fields():
    for field in (
        "healthy_replicas",
        "size_bytes",
        "current_version",
        "version_state",
        "checksum",
    ):
        assert field in JS
    assert "list_object_summaries" not in JS


def test_replica_details_are_backend_driven():
    assert "current.replicas" in JS
    assert "replicaRows" in JS
    assert "PROTECTED COPIES" in JS
    assert "current.replicas" in JS


def test_admin_controls_are_contextual():
    assert "data-action='repair'" in JS
    assert "data-action='rebalance'" in JS
    assert "Repair this version" in JS
    assert "Rebalance placement" in JS
    assert 'data-action="refresh"' in HTML
    assert "Run repair pass" not in HTML
    assert "Start rebalance" not in HTML


def test_user_facing_branding_and_help():
    assert "<title>VaultGuard — Distributed Storage Console</title>" in HTML
    assert 'href="./favicon.svg?v=3"' in HTML
    assert "VAULTGUARD" in HTML
    assert "Distributed Storage Console" in HTML
    assert "User guide" in HTML
    assert "About us" in HTML
    assert "Live cluster topology" not in HTML
    assert 'id="view-guide"' in HTML
    assert 'id="view-about"' in HTML


def test_topology_is_live_data_driven():
    assert 'id="topology-node-grid"' in HTML
    assert "DATA.nodes.map" in JS
    assert "topology-node-count" in HTML
    assert "topology-replication" in HTML
    assert "replica relationship" not in HTML


def test_settings_reflect_backend_and_admin_key():
    for token in (
        'id="policy-rf"',
        'id="policy-wq"',
        'id="policy-rq"',
        'id="policy-heartbeat"',
        'id="policy-suspect"',
        'id="policy-unavailable"',
        'id="policy-parallel"',
        'id="admin-key-input"',
        'id="admin-key-status"',
    ):
        assert token in HTML
    assert 'sessionStorage.getItem("vault_admin_key")' in JS
    assert 'sessionStorage.setItem("vault_admin_key",key)' in JS


def test_no_fabricated_operational_metrics():
    for token in (
        "12,842",
        "98.7",
        "99.4",
        "38,526",
        "1.44 TB",
        "2 repairs today",
        "repair-203",
        "obj_8fd21a",
    ):
        assert token not in HTML
        assert token not in JS


def test_upload_is_live_and_meaningful():
    assert 'method:"PUT"' in JS
    assert 'Uploading through Part B' in JS
    assert 'live object catalog' in JS
    assert "File exceeds the configured upload limit." in JS


def test_job_polling_exists():
    assert "async function pollJob" in JS
    assert "await API.job(name,id)" in JS
    assert "Operation still running" in JS


def test_accessibility_contract():
    for token in (
        'class="skip-link"',
        'href="#main-content"',
        'id="main-content"',
        'aria-label="Primary navigation"',
        'aria-label="Search nodes"',
        'aria-label="Search files"',
        'aria-hidden="true"',
        'aria-live="polite"',
        'aria-atomic="true"',
        'aria-label="Open command palette"',
        'scope="col"',
        'role="progressbar"',
        'aria-valuemin="0"',
        'aria-valuemax="100"',
        'caption class="sr-only"',
    ):
        assert token in HTML
    assert 'prefers-reduced-motion' in CSS
    assert '.skip-link:focus' in CSS
    assert '.sr-only' in CSS
    assert 'Content-Security-Policy' in HTML
    assert 'resolveApiBaseUrl' in JS
    assert 'MAX_UPLOAD_BYTES' in JS
    assert 'id="file-input" type="file" aria-label="Select object file"' in HTML
    assert "connect-src 'self' https:" in HTML


def test_frontend_framework_free():
    text = (HTML + CSS + JS).lower()
    for framework in ("react", "vue", "angular", "jquery"):
        assert framework not in text


def test_command_palette_is_connected():
    assert 'id="command-modal"' in HTML
    assert "const commands=" in JS
    assert "openCommand" in JS


def test_client_hardening_contract():
    assert 'Cache-Control","no-cache' in JS
    assert "validateUploadFile(file)" in JS
    assert "Unsupported API protocol." in JS
    assert "File exceeds the configured upload limit." in JS
    assert "escapeHtml" in JS


def test_security_and_performance_metadata():
    assert "Content-Security-Policy" in HTML
    assert 'name="referrer"' in HTML
    assert re.search(r'<script src="./js/config\.js" defer></script>', HTML)
    assert re.search(r'<script src="./js/app\.js\?v=[^"]+" defer></script>', HTML)
    assert '<meta name="description"' in HTML
    assert '<meta name="viewport"' in HTML


def test_no_render_blocking_inline_javascript():
    assert "<script>" not in HTML
    assert "<script " in HTML
    assert "eval(" not in JS
    assert "new Function" not in JS
    assert not re.search(r'\bon(?:click|load|error|mouseover|keydown)\s*=', HTML, re.I)


def test_accessible_overlay_contract():
    assert 'aria-modal="true"' in HTML
    assert 'aria-describedby=' in HTML
    assert "let activeOverlay=null" in JS
    assert "shell.inert=true" in JS
    assert "shell.inert=false" in JS
    assert "FOCUSABLE_SELECTOR" in JS


def test_modal_accessibility_contract():
    assert 'let activeOverlay=null' in JS
    assert "FOCUSABLE_SELECTOR" in JS
    assert "trapOverlayFocus" in JS
    assert "shell.inert=true" in JS
    assert "shell.inert=false" in JS
    assert "restoreFocus" in JS


if __name__ == "__main__":
    tests = [v for k, v in globals().items() if k.startswith("test_") and callable(v)]
    for test in tests:
        test()
    print("frontend verifier: %d checks passed" % len(tests))
