import json
from pathlib import Path
from typing import AbstractSet, Any, Mapping, Optional

import pytest
from dagster import (
    AssetKey,
    AutoMaterializePolicy,
    DailyPartitionsDefinition,
    FreshnessPolicy,
    PartitionsDefinition,
    materialize,
)
from dagster._core.definitions.utils import DEFAULT_IO_MANAGER_KEY
from dagster_dbt import DbtCli
from dagster_dbt.asset_decorator import dbt_assets
from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator

manifest_path = Path(__file__).parent.joinpath("sample_manifest.json")
with open(manifest_path, "r") as f:
    manifest = json.load(f)

test_dagster_metadata_manifest_path = Path(__file__).parent.joinpath(
    "dbt_projects", "test_dagster_metadata", "manifest.json"
)
with open(test_dagster_metadata_manifest_path, "r") as f:
    test_dagster_metadata_manifest = json.load(f)


def test_materialize(test_project_dir):
    @dbt_assets(manifest=manifest)
    def all_dbt_assets(context, dbt: DbtCli):
        yield from dbt.cli(["build"], context=context).stream()

    assert materialize(
        [all_dbt_assets], resources={"dbt": DbtCli(project_dir=test_project_dir)}
    ).success


@pytest.mark.parametrize("manifest", [json.load(manifest_path.open()), manifest_path])
def test_manifest_argument(manifest):
    @dbt_assets(manifest=manifest)
    def my_dbt_assets():
        ...

    assert my_dbt_assets.keys == {
        AssetKey.from_user_string(key)
        for key in [
            "sort_by_calories",
            "cold_schema/sort_cold_cereals_by_calories",
            "subdir_schema/least_caloric",
            "sort_hot_cereals_by_calories",
            "orders_snapshot",
            "cereals",
        ]
    }


@pytest.mark.parametrize(
    "select,exclude,expected_asset_names",
    [
        (
            "*",
            None,
            {
                "sort_by_calories",
                "cold_schema/sort_cold_cereals_by_calories",
                "subdir_schema/least_caloric",
                "sort_hot_cereals_by_calories",
                "orders_snapshot",
                "cereals",
            },
        ),
        (
            "+least_caloric",
            None,
            {"sort_by_calories", "subdir_schema/least_caloric", "cereals"},
        ),
        (
            "sort_by_calories least_caloric",
            None,
            {"sort_by_calories", "subdir_schema/least_caloric"},
        ),
        (
            "tag:bar+",
            None,
            {
                "sort_by_calories",
                "cold_schema/sort_cold_cereals_by_calories",
                "subdir_schema/least_caloric",
                "sort_hot_cereals_by_calories",
                "orders_snapshot",
            },
        ),
        (
            "tag:foo",
            None,
            {"sort_by_calories", "cold_schema/sort_cold_cereals_by_calories"},
        ),
        (
            "tag:foo,tag:bar",
            None,
            {"sort_by_calories"},
        ),
        (
            None,
            "sort_hot_cereals_by_calories",
            {
                "sort_by_calories",
                "cold_schema/sort_cold_cereals_by_calories",
                "subdir_schema/least_caloric",
                "cereals",
                "orders_snapshot",
            },
        ),
        (
            None,
            "+least_caloric",
            {
                "cold_schema/sort_cold_cereals_by_calories",
                "sort_hot_cereals_by_calories",
                "orders_snapshot",
            },
        ),
        (
            None,
            "sort_by_calories least_caloric",
            {
                "cold_schema/sort_cold_cereals_by_calories",
                "sort_hot_cereals_by_calories",
                "orders_snapshot",
                "cereals",
            },
        ),
        (
            None,
            "tag:foo",
            {
                "subdir_schema/least_caloric",
                "sort_hot_cereals_by_calories",
                "orders_snapshot",
                "cereals",
            },
        ),
    ],
)
def test_selections(
    select: Optional[str], exclude: Optional[str], expected_asset_names: AbstractSet[str]
) -> None:
    @dbt_assets(
        manifest=manifest,
        select=select or "fqn:*",
        exclude=exclude,
    )
    def my_dbt_assets():
        ...

    expected_asset_keys = {AssetKey(key.split("/")) for key in expected_asset_names}
    assert my_dbt_assets.keys == expected_asset_keys

    expected_select_tag = "fqn:*" if select is None else select
    assert my_dbt_assets.op.tags.get("dagster-dbt/select") == expected_select_tag
    assert my_dbt_assets.op.tags.get("dagster-dbt/exclude") == exclude


@pytest.mark.parametrize(
    "partitions_def", [None, DailyPartitionsDefinition(start_date="2023-01-01")]
)
def test_partitions_def(partitions_def: Optional[PartitionsDefinition]) -> None:
    @dbt_assets(manifest=manifest, partitions_def=partitions_def)
    def my_dbt_assets():
        ...

    assert my_dbt_assets.partitions_def == partitions_def


@pytest.mark.parametrize("io_manager_key", [None, "my_io_manager_key"])
def test_io_manager_key(io_manager_key: Optional[str]) -> None:
    @dbt_assets(manifest=manifest, io_manager_key=io_manager_key)
    def my_dbt_assets():
        ...

    expected_io_manager_key = DEFAULT_IO_MANAGER_KEY if io_manager_key is None else io_manager_key

    for output_def in my_dbt_assets.node_def.output_defs:
        assert output_def.io_manager_key == expected_io_manager_key


def test_with_asset_key_replacements() -> None:
    class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
        @classmethod
        def node_info_to_asset_key(cls, node_info: Mapping[str, Any]) -> AssetKey:
            return AssetKey(["prefix", *super().node_info_to_asset_key(node_info).path])

    @dbt_assets(manifest=manifest, dagster_dbt_translator=CustomizedDagsterDbtTranslator())
    def my_dbt_assets():
        ...

    assert my_dbt_assets.keys_by_input_name == {}
    assert set(my_dbt_assets.keys_by_output_name.values()) == {
        AssetKey(["prefix", "cereals"]),
        AssetKey(["prefix", "cold_schema", "sort_cold_cereals_by_calories"]),
        AssetKey(["prefix", "subdir_schema", "least_caloric"]),
        AssetKey(["prefix", "orders_snapshot"]),
        AssetKey(["prefix", "sort_hot_cereals_by_calories"]),
        AssetKey(["prefix", "sort_by_calories"]),
    }


def test_with_description_replacements() -> None:
    expected_description = "customized description"

    class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
        @classmethod
        def node_info_to_description(cls, node_info: Mapping[str, Any]) -> str:
            return expected_description

    @dbt_assets(manifest=manifest, dagster_dbt_translator=CustomizedDagsterDbtTranslator())
    def my_dbt_assets():
        ...

    for description in my_dbt_assets.descriptions_by_key.values():
        assert description == expected_description


def test_with_metadata_replacements() -> None:
    expected_metadata = {"customized": "metadata"}

    class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
        @classmethod
        def node_info_to_metadata(cls, node_info: Mapping[str, Any]) -> Mapping[str, Any]:
            return expected_metadata

    @dbt_assets(manifest=manifest, dagster_dbt_translator=CustomizedDagsterDbtTranslator())
    def my_dbt_assets():
        ...

    for metadata in my_dbt_assets.metadata_by_key.values():
        assert metadata["customized"] == "metadata"


def test_dbt_meta_auto_materialize_policy() -> None:
    @dbt_assets(manifest=test_dagster_metadata_manifest)
    def my_dbt_assets():
        ...

    auto_materialize_policies = my_dbt_assets.auto_materialize_policies_by_key.values()
    assert auto_materialize_policies

    for auto_materialize_policy in auto_materialize_policies:
        assert auto_materialize_policy == AutoMaterializePolicy.eager()


def test_dbt_meta_freshness_policy() -> None:
    @dbt_assets(manifest=test_dagster_metadata_manifest)
    def my_dbt_assets():
        ...

    freshness_policies = my_dbt_assets.freshness_policies_by_key.values()
    assert freshness_policies

    for freshness_policy in freshness_policies:
        assert freshness_policy == FreshnessPolicy(
            maximum_lag_minutes=60.0, cron_schedule="* * * * *"
        )


def test_dbt_meta_asset_key() -> None:
    @dbt_assets(manifest=test_dagster_metadata_manifest)
    def my_dbt_assets():
        ...

    # Assert that source asset keys are set properly.
    assert set(my_dbt_assets.keys_by_input_name.values()) == {
        AssetKey(["customized", "source", "jaffle_shop", "main", "raw_customers"])
    }

    # Assert that models asset keys are set properly.
    assert {
        AssetKey(["customized", "staging", "customers"]),
        AssetKey(["customized", "staging", "orders"]),
        AssetKey(["customized", "staging", "payments"]),
    }.issubset(my_dbt_assets.keys)


def test_dbt_config_group() -> None:
    @dbt_assets(manifest=test_dagster_metadata_manifest)
    def my_dbt_assets():
        ...

    assert my_dbt_assets.group_names_by_key == {
        AssetKey(["customers"]): "default",
        AssetKey(["customized", "staging", "customers"]): "customized_dagster_group",
        AssetKey(["customized", "staging", "orders"]): "staging",
        AssetKey(["customized", "staging", "payments"]): "staging",
        AssetKey(["orders"]): "default",
        AssetKey(["raw_customers"]): "default",
        AssetKey(["raw_orders"]): "default",
        AssetKey(["raw_payments"]): "default",
    }
