from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).parents[1]
JOB_YAML = (ROOT / "jobs" / "resources" / "trade_pipeline.job.yml").read_text(encoding="utf-8")
PYPROJECT = (ROOT / "pyproject.toml").read_text(encoding="utf-8")


def test_every_job_entry_point_is_a_packaged_script() -> None:
    scripts = re.search(r"\[project\.scripts\](.*?)\n\[", PYPROJECT, re.S).group(1)
    packaged = set(re.findall(r"^(\w+)\s*=", scripts, re.M))
    entry_points = re.findall(r"entry_point: (\w+)", JOB_YAML)
    assert len(entry_points) == 6
    assert set(entry_points) == packaged
    assert set(re.findall(r"package_name: (\w+)", JOB_YAML)) == {"trade_pipeline"}


def test_job_uses_the_bundle_built_wheel_not_a_manual_upload() -> None:
    assert "../../dist/*.whl" in JOB_YAML
    assert "/Workspace/Users" not in JOB_YAML and ".whl" not in JOB_YAML.replace("../../dist/*.whl", "")
    bundle = (ROOT / "databricks.yml").read_text(encoding="utf-8")
    assert "type: whl" in bundle and "pip wheel . --no-deps --wheel-dir dist" in bundle


def test_job_has_no_cluster_settings_for_serverless_workspaces() -> None:
    for unsupported in ("new_cluster", "job_cluster", "existing_cluster_id", "node_type_id", "spark_version"):
        assert unsupported not in JOB_YAML
    assert JOB_YAML.count("environment_key: default_env") == 7  # definition + six tasks


def test_deploy_script_supports_the_documented_commands() -> None:
    script = (ROOT / "scripts" / "deploy.sh").read_text(encoding="utf-8")
    assert "set -euo pipefail" in script
    assert 'TARGET="${DATABRICKS_TARGET:-dev}"' in script
    assert 'JOB="trade_pipeline"' in script
    assert "\r" not in script  # bash cannot run CRLF scripts
    for command in ("build)", "validate)", "deploy)", "run)"):
        assert command in script
    assert "token" not in script.lower()  # credentials come from the CLI profile
