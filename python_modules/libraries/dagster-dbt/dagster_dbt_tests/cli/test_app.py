import shutil
import sys
from pathlib import Path

import pytest
from dagster_dbt.cli.app import app
from typer.testing import CliRunner

test_dagster_metadata_dbt_project_path = Path(__file__).parent.joinpath(
    "..", "dbt_projects", "test_dagster_metadata"
)

runner = CliRunner()


@pytest.fixture(name="tmp_project_dir")
def tmp_project_dir_fixture(tmp_path: Path) -> Path:
    shutil.copytree(
        test_dagster_metadata_dbt_project_path,
        tmp_path.joinpath("test_dagster_metadata"),
        dirs_exist_ok=True,
    )

    return tmp_path


@pytest.mark.skipif(sys.version_info < (3, 8), reason="requires python3.8")
def test_project_scaffold_command(monkeypatch: pytest.MonkeyPatch, tmp_project_dir: Path) -> None:
    monkeypatch.chdir(tmp_project_dir)

    project_name = "test_dagster_scaffold"
    dagster_project_dir = tmp_project_dir.joinpath(project_name)

    result = runner.invoke(
        app,
        [
            "project",
            "scaffold",
            "--project-name",
            project_name,
            "--dbt-project-dir",
            tmp_project_dir.joinpath("test_dagster_metadata").as_posix(),
        ],
    )

    assert result.exit_code == 0
    assert f"Initializing Dagster project {project_name}" in result.stdout
    assert "Your Dagster project has been initialized" in result.stdout
    assert dagster_project_dir.exists()
    assert not any(path.suffix == ".jinja" for path in dagster_project_dir.glob("**/*"))
