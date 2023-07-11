import json
import shutil
from pathlib import Path
from typing import List, Optional

import pytest
from dagster import AssetObservation, FloatMetadataValue, Output, TextMetadataValue, materialize
from dagster_dbt import DagsterDbtTranslator, dbt_assets
from dagster_dbt.core.resources_v2 import (
    PARTIAL_PARSE_FILE_NAME,
    DbtCli,
    DbtCliEventMessage,
)
from dagster_dbt.errors import DagsterDbtCliRuntimeError

from ..conftest import TEST_PROJECT_DIR

pytest.importorskip("dbt.version", minversion="1.4")


manifest_path = Path(TEST_PROJECT_DIR).joinpath("manifest.json")
with open(manifest_path, "r") as f:
    manifest = json.load(f)


@pytest.mark.parametrize("global_config_flags", [[], ["--debug"]])
@pytest.mark.parametrize("command", ["run", "parse"])
def test_dbt_cli(global_config_flags: List[str], command: str) -> None:
    dbt = DbtCli(project_dir=TEST_PROJECT_DIR, global_config_flags=global_config_flags)
    dbt_cli_task = dbt.cli([command], manifest=manifest)

    dbt_cli_task.wait()

    assert dbt_cli_task.process.args == ["dbt", *global_config_flags, command]
    assert dbt_cli_task.is_successful()
    assert dbt_cli_task.process.returncode == 0


def test_dbt_cli_failure() -> None:
    dbt = DbtCli(project_dir=TEST_PROJECT_DIR)
    dbt_cli_task = dbt.cli(["run", "--profiles-dir", "nonexistent"], manifest=manifest)

    with pytest.raises(DagsterDbtCliRuntimeError):
        dbt_cli_task.wait()

    assert not dbt_cli_task.is_successful()
    assert dbt_cli_task.process.returncode == 2


def test_dbt_cli_get_artifact() -> None:
    dbt = DbtCli(project_dir=TEST_PROJECT_DIR)

    dbt_cli_task_1 = dbt.cli(["run"], manifest=manifest)
    dbt_cli_task_1.wait()

    dbt_cli_task_2 = dbt.cli(["compile"], manifest=manifest)
    dbt_cli_task_2.wait()

    # `dbt run` produces a manifest.json and run_results.json
    manifest_json_1 = dbt_cli_task_1.get_artifact("manifest.json")
    assert manifest_json_1
    assert dbt_cli_task_1.get_artifact("run_results.json")

    # `dbt compile` produces a manifest.json and run_results.json
    manifest_json_2 = dbt_cli_task_2.get_artifact("manifest.json")
    assert manifest_json_2
    assert dbt_cli_task_2.get_artifact("run_results.json")

    # `dbt compile` does not produce a sources.json
    with pytest.raises(Exception):
        dbt_cli_task_2.get_artifact("sources.json")

    # Artifacts are stored in separate paths by manipulating DBT_TARGET_PATH.
    # As a result, their contents should be different, and newer artifacts
    # should not overwrite older ones.
    assert manifest_json_1 != manifest_json_2


def test_dbt_profile_configuration(monkeypatch) -> None:
    dbt = DbtCli(project_dir=TEST_PROJECT_DIR, profile="duckdb", target="dev")

    dbt_cli_task = dbt.cli(["parse"], manifest=manifest)
    dbt_cli_task.wait()

    assert dbt_cli_task.process.args == ["dbt", "parse", "--profile", "duckdb", "--target", "dev"]
    assert dbt_cli_task.is_successful()


def test_dbt_without_partial_parse() -> None:
    dbt = DbtCli(project_dir=TEST_PROJECT_DIR)

    dbt.cli(["clean"], manifest=manifest).wait()

    dbt_cli_compile_without_partial_parse_task = dbt.cli(["compile"], manifest=manifest)

    assert dbt_cli_compile_without_partial_parse_task.is_successful()
    assert any(
        "Unable to do partial parsing" in event.raw_event["info"]["msg"]
        for event in dbt_cli_compile_without_partial_parse_task.stream_raw_events()
    )


def test_dbt_with_partial_parse() -> None:
    dbt = DbtCli(project_dir=TEST_PROJECT_DIR)

    dbt.cli(["clean"], manifest=manifest).wait()

    # Run `dbt compile` to generate the partial parse file
    dbt_cli_compile_task = dbt.cli(["compile"], manifest=manifest)
    dbt_cli_compile_task.wait()

    # Copy the partial parse file to the target directory
    partial_parse_file_path = Path(
        TEST_PROJECT_DIR, dbt_cli_compile_task.target_path, PARTIAL_PARSE_FILE_NAME
    )
    original_target_path = Path(TEST_PROJECT_DIR, "target", PARTIAL_PARSE_FILE_NAME)

    original_target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(partial_parse_file_path, Path(TEST_PROJECT_DIR, "target", PARTIAL_PARSE_FILE_NAME))

    # Assert that partial parsing was used.
    dbt_cli_compile_with_partial_parse_task = dbt.cli(["compile"], manifest=manifest)
    dbt_cli_compile_with_partial_parse_task.wait()

    assert dbt_cli_compile_with_partial_parse_task.is_successful()
    assert not any(
        "Unable to do partial parsing" in event.raw_event["info"]["msg"]
        for event in dbt_cli_compile_with_partial_parse_task.stream_raw_events()
    )


def test_dbt_cli_subsetted_execution() -> None:
    @dbt_assets(
        manifest=manifest,
        select=(
            "fqn:dagster_dbt_test_project.subdir.least_caloric"
            " fqn:dagster_dbt_test_project.sort_by_calories"
        ),
    )
    def my_dbt_assets(context):
        dbt = DbtCli(project_dir=TEST_PROJECT_DIR)
        dbt_cli_task = dbt.cli(["run"], context=context)

        dbt_cli_task.wait()

        assert dbt_cli_task.process.args == [
            "dbt",
            "run",
            "--select",
            (
                "fqn:dagster_dbt_test_project.subdir.least_caloric"
                " fqn:dagster_dbt_test_project.sort_by_calories"
            ),
        ]
        assert dbt_cli_task.process.returncode is not None

        yield from dbt_cli_task.stream()

    assert materialize([my_dbt_assets]).success


@pytest.mark.parametrize("exclude", [None, "fqn:dagster_dbt_test_project.subdir.least_caloric"])
def test_dbt_cli_default_selection(exclude: Optional[str]) -> None:
    @dbt_assets(manifest=manifest, exclude=exclude)
    def my_dbt_assets(context):
        dbt = DbtCli(project_dir=TEST_PROJECT_DIR)
        dbt_cli_task = dbt.cli(["run"], context=context)

        dbt_cli_task.wait()

        expected_args = ["dbt", "run", "--select", "fqn:*"]
        if exclude:
            expected_args += ["--exclude", exclude]

        assert dbt_cli_task.process.args == expected_args
        assert dbt_cli_task.process.returncode is not None

        yield from dbt_cli_task.stream()

    assert materialize([my_dbt_assets]).success


@pytest.mark.parametrize(
    "data",
    [
        {},
        {
            "node_info": {
                "unique_id": "a.b.c",
                "resource_type": "model",
                "node_status": "failure",
                "node_finished_at": "2024-01-01T00:00:00Z",
            }
        },
        {
            "node_info": {
                "unique_id": "a.b.c",
                "resource_type": "macro",
                "node_status": "success",
                "node_finished_at": "2024-01-01T00:00:00Z",
            }
        },
        {
            "node_info": {
                "unique_id": "a.b.c",
                "resource_type": "model",
                "node_status": "failure",
            }
        },
        {
            "node_info": {
                "unique_id": "a.b.c",
                "resource_type": "test",
                "node_status": "success",
            }
        },
    ],
    ids=[
        "node info missing",
        "node status failure",
        "not refable",
        "not successful execution",
        "not finished test execution",
    ],
)
def test_no_default_asset_events_emitted(data: dict) -> None:
    asset_events = DbtCliEventMessage(raw_event={"data": data}).to_default_asset_events(
        manifest={}, dagster_dbt_translator=DagsterDbtTranslator()
    )

    assert list(asset_events) == []


def test_to_default_asset_output_events() -> None:
    raw_event = {
        "data": {
            "node_info": {
                "unique_id": "a.b.c",
                "resource_type": "model",
                "node_status": "success",
                "node_started_at": "2024-01-01T00:00:00Z",
                "node_finished_at": "2024-01-01T00:01:00Z",
            }
        }
    }
    asset_events = list(
        DbtCliEventMessage(raw_event=raw_event).to_default_asset_events(
            manifest={}, dagster_dbt_translator=DagsterDbtTranslator()
        )
    )

    assert len(asset_events) == 1
    assert all(isinstance(e, Output) for e in asset_events)
    assert asset_events[0].metadata == {
        "unique_id": TextMetadataValue("a.b.c"),
        "Execution Duration": FloatMetadataValue(60.0),
    }


def test_to_default_asset_observation_events() -> None:
    manifest = {
        "nodes": {
            "a.b.c.d": {
                "resource_type": "model",
                "config": {},
                "name": "model",
            }
        },
        "sources": {
            "a.b.c.d.e": {
                "resource_type": "source",
                "source_name": "test",
                "name": "source",
            }
        },
        "parent_map": {
            "a.b.c": [
                "a.b.c.d",
                "a.b.c.d.e",
            ]
        },
    }
    raw_event = {
        "data": {
            "node_info": {
                "unique_id": "a.b.c",
                "resource_type": "test",
                "node_status": "success",
                "node_finished_at": "2024-01-01T00:00:00Z",
            }
        }
    }
    asset_events = list(
        DbtCliEventMessage(raw_event=raw_event).to_default_asset_events(
            manifest=manifest, dagster_dbt_translator=DagsterDbtTranslator()
        )
    )

    assert len(asset_events) == 2
    assert all(isinstance(e, AssetObservation) for e in asset_events)
