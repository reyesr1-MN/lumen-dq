"""
Validation CLI exit-code tests.

Runs `python -m lumen_dq.validate <data_dir>` as a subprocess to verify:
  • Exit 0 on clean data (no BLOCKING failures).
  • Exit 1 on data with BLOCKING failures.
  • Exit 2 on missing directory or missing parquet files.
  • JSON output contains expected top-level keys.
  • Human-readable output shows FAIL lines when checks fail.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from lumen_dq.generate import generate_all, generate_buggy_branch_reference


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def clean_dir(tmp_path_factory):
    """Baseline (clean) dataset directory."""
    d = tmp_path_factory.mktemp("cli_clean")
    generate_all(d)
    return d


@pytest.fixture(scope="module")
def cli_buggy_dir(tmp_path_factory, clean_dir):
    """Buggy dataset: branch_reference has 5 duplicate branch_ids."""
    import shutil
    d = tmp_path_factory.mktemp("cli_buggy")
    for f in clean_dir.glob("*.parquet"):
        shutil.copy(f, d)
    generate_buggy_branch_reference(clean_dir).to_parquet(
        d / "branch_reference.parquet", index=False
    )
    return d


def _run(data_dir: Path, extra_args: list[str] | None = None) -> subprocess.CompletedProcess:
    """
    Run lumen-validate as a subprocess.

    Python 3.14 skips .pth files whose names start with '_', so the hatchling
    editable-install file '_editable_impl_lumen_dq.pth' is silently ignored and
    lumen_dq is not on sys.path in subprocess context.  We compensate by
    propagating the src/ directory via PYTHONPATH.  A proper wheel install would
    not need this workaround.
    """
    import os
    env = os.environ.copy()
    src_dir = Path(__file__).parent.parent / "src"
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(src_dir) + (":" + existing if existing else "")
    cmd = [sys.executable, "-m", "lumen_dq.validate", str(data_dir)]
    if extra_args:
        cmd.extend(extra_args)
    return subprocess.run(cmd, capture_output=True, text=True, env=env)


# ── Exit-code tests ───────────────────────────────────────────────────────────


class TestExitCodes:
    def test_exit_0_on_clean_data(self, clean_dir):
        """All BLOCKING checks pass on the clean baseline → exit 0."""
        result = _run(clean_dir)
        assert result.returncode == 0, (
            f"Expected exit 0 on clean data; got {result.returncode}.\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

    def test_exit_1_on_buggy_data(self, cli_buggy_dir):
        """Duplicate branch_ids trigger BLOCKING failures → exit 1."""
        result = _run(cli_buggy_dir)
        assert result.returncode == 1, (
            f"Expected exit 1 on buggy data; got {result.returncode}.\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

    def test_exit_2_on_missing_directory(self, tmp_path):
        """Non-existent directory → usage error → exit 2."""
        result = _run(tmp_path / "does_not_exist")
        assert result.returncode == 2

    def test_exit_2_on_empty_directory(self, tmp_path):
        """Directory exists but has no parquet files → exit 2."""
        result = _run(tmp_path)
        assert result.returncode == 2

    def test_exit_2_on_partial_parquet_files(self, tmp_path, clean_dir):
        """Only some parquet files present → exit 2."""
        import shutil
        # Copy only branch_reference.parquet (3 are missing)
        shutil.copy(clean_dir / "branch_reference.parquet", tmp_path)
        result = _run(tmp_path)
        assert result.returncode == 2


# ── Output content tests ──────────────────────────────────────────────────────


class TestHumanReadableOutput:
    def test_clean_shows_pass_result(self, clean_dir):
        result = _run(clean_dir)
        assert "PASS" in result.stdout or "passed" in result.stdout.lower()

    def test_buggy_shows_fail_result(self, cli_buggy_dir):
        result = _run(cli_buggy_dir)
        assert "FAIL" in result.stdout

    def test_buggy_output_mentions_branch(self, cli_buggy_dir):
        """Human-readable output must reference the failing table."""
        result = _run(cli_buggy_dir)
        assert "branch_reference" in result.stdout.lower() or "branch" in result.stdout.lower()

    def test_clean_output_has_layer_headers(self, clean_dir):
        result = _run(clean_dir)
        out = result.stdout.lower()
        assert "layer 1" in out or "schema" in out

    def test_clean_output_has_summary_line(self, clean_dir):
        result = _run(clean_dir)
        assert "passed" in result.stdout.lower()


class TestJsonOutput:
    def test_json_flag_produces_valid_json(self, clean_dir, tmp_path):
        out_file = tmp_path / "results.json"
        result = _run(clean_dir, ["--out", str(out_file)])
        assert result.returncode == 0
        assert out_file.exists()
        data = json.loads(out_file.read_text())
        assert isinstance(data, dict)

    def test_json_has_required_top_level_keys(self, clean_dir, tmp_path):
        out_file = tmp_path / "results.json"
        _run(clean_dir, ["--out", str(out_file)])
        data = json.loads(out_file.read_text())
        for key in ("total", "passed", "failed", "blocking_failures", "checks"):
            assert key in data, f"Missing top-level key: {key}"

    def test_json_check_has_required_fields(self, clean_dir, tmp_path):
        out_file = tmp_path / "results.json"
        _run(clean_dir, ["--out", str(out_file)])
        data = json.loads(out_file.read_text())
        check = data["checks"][0]
        for field in (
            "check_id", "severity", "layer", "table", "passed",
            "expected", "observed", "failing_rows", "affected_keys",
            "pipeline_health", "next_step",
        ):
            assert field in check, f"Missing check field: {field}"

    def test_json_all_checks_pass_on_clean(self, clean_dir, tmp_path):
        out_file = tmp_path / "results.json"
        result = _run(clean_dir, ["--out", str(out_file)])
        assert result.returncode == 0
        data = json.loads(out_file.read_text())
        assert data["blocking_failures"] == 0

    def test_json_blocking_failures_nonzero_on_buggy(self, cli_buggy_dir, tmp_path):
        out_file = tmp_path / "results.json"
        result = _run(cli_buggy_dir, ["--out", str(out_file)])
        assert result.returncode == 1
        data = json.loads(out_file.read_text())
        assert data["blocking_failures"] > 0

    def test_json_check_ids_are_stable(self, clean_dir, tmp_path):
        """check_id values must be stable identifiers (L{n}.<name>.<table>)."""
        out_file = tmp_path / "results.json"
        _run(clean_dir, ["--out", str(out_file)])
        data = json.loads(out_file.read_text())
        for check in data["checks"]:
            cid = check["check_id"]
            assert cid.startswith("L"), f"check_id must start with 'L': {cid}"

    def test_json_severity_values_are_valid(self, clean_dir, tmp_path):
        out_file = tmp_path / "results.json"
        _run(clean_dir, ["--out", str(out_file)])
        data = json.loads(out_file.read_text())
        valid = {"BLOCKING", "WARNING", "INFO"}
        for check in data["checks"]:
            assert check["severity"] in valid, (
                f"Unexpected severity: {check['severity']}"
            )

    def test_json_layer_values_are_1_through_5(self, clean_dir, tmp_path):
        out_file = tmp_path / "results.json"
        _run(clean_dir, ["--out", str(out_file)])
        data = json.loads(out_file.read_text())
        layers = {check["layer"] for check in data["checks"]}
        assert layers == {1, 2, 3, 4, 5}, (
            f"Expected layers 1-5; got {sorted(layers)}"
        )

    def test_json_buggy_failing_check_has_pipeline_health(self, cli_buggy_dir, tmp_path):
        out_file = tmp_path / "results.json"
        _run(cli_buggy_dir, ["--out", str(out_file)])
        data = json.loads(out_file.read_text())
        failing = [c for c in data["checks"] if not c["passed"]]
        assert failing, "Expected at least one failing check"
        for c in failing:
            assert len(c["pipeline_health"]) > 0, (
                f"pipeline_health must be non-empty for failed check {c['check_id']}"
            )

    def test_json_buggy_failing_check_has_next_step(self, cli_buggy_dir, tmp_path):
        out_file = tmp_path / "results.json"
        _run(cli_buggy_dir, ["--out", str(out_file)])
        data = json.loads(out_file.read_text())
        failing = [c for c in data["checks"] if not c["passed"]]
        for c in failing:
            assert len(c["next_step"]) > 0, (
                f"next_step must be non-empty for failed check {c['check_id']}"
            )
