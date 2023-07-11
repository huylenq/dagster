from typing import Sequence

from dagster import (
    AssetsDefinition,
    PartitionKeyRange,
)
from dagster._core.definitions.asset_selection import AssetSelection
from dagster._core.definitions.auto_materialize_condition import (
    MaxMaterializationsExceededAutoMaterializeCondition,
    MissingAutoMaterializeCondition,
    ParentMaterializedAutoMaterializeCondition,
    ParentOutdatedAutoMaterializeCondition,
)
from dagster._core.definitions.auto_materialize_policy import AutoMaterializePolicy
from dagster._core.definitions.events import AssetKey
from dagster._core.definitions.freshness_policy import FreshnessPolicy
from dagster._seven.compat.pendulum import create_pendulum_time

from .asset_reconciliation_scenario import (
    AssetReconciliationScenario,
    asset_def,
    run,
    run_request,
    single_asset_run,
)
from .basic_scenarios import diamond
from .freshness_policy_scenarios import (
    daily_to_unpartitioned,
    overlapping_freshness_inf,
)
from .partition_scenarios import (
    hourly_partitions_def,
    hourly_to_daily_partitions,
    two_assets_in_sequence_one_partition,
    two_partitions_partitions_def,
)

single_lazy_asset = [asset_def("asset1", auto_materialize_policy=AutoMaterializePolicy.lazy())]
single_lazy_asset_with_freshness_policy = [
    asset_def(
        "asset1",
        auto_materialize_policy=AutoMaterializePolicy.lazy(),
        freshness_policy=FreshnessPolicy(maximum_lag_minutes=60),
    )
]
lopsided_vee = [
    asset_def("root1"),
    asset_def("root2"),
    asset_def("A", ["root1"]),
    asset_def("B", ["A"]),
    asset_def("C", ["B"], auto_materialize_policy=AutoMaterializePolicy.eager()),
    asset_def("D", ["root2", "C"], auto_materialize_policy=AutoMaterializePolicy.eager()),
]

time_partitioned_eager_after_non_partitioned = [
    asset_def("unpartitioned_root_a"),
    asset_def("unpartitioned_root_b"),
    asset_def(
        "time_partitioned",
        ["unpartitioned_root_a"],
        partitions_def=hourly_partitions_def,
        auto_materialize_policy=AutoMaterializePolicy.eager(),
    ),
    asset_def(
        "unpartitioned_downstream",
        ["time_partitioned", "unpartitioned_root_b"],
        auto_materialize_policy=AutoMaterializePolicy.eager(),
    ),
]
static_partitioned_eager_after_non_partitioned = [
    asset_def("unpartitioned"),
    asset_def(
        "static_partitioned",
        ["unpartitioned"],
        partitions_def=two_partitions_partitions_def,
        auto_materialize_policy=AutoMaterializePolicy.eager(max_materializations_per_minute=2),
    ),
]


def with_auto_materialize_policy(
    assets_defs: Sequence[AssetsDefinition], auto_materialize_policy: AutoMaterializePolicy
) -> Sequence[AssetsDefinition]:
    """Note: this should be implemented in core dagster at some point, and this implementation is
    a lazy hack.
    """
    ret = []
    for assets_def in assets_defs:
        ret.append(assets_def.with_attributes(auto_materialize_policy=auto_materialize_policy))
    return ret


# auto materialization policies
auto_materialize_policy_scenarios = {
    "auto_materialize_policy_lazy_missing": AssetReconciliationScenario(
        assets=single_lazy_asset,
        unevaluated_runs=[],
        expected_run_requests=[],
    ),
    "auto_materialize_policy_lazy_freshness_missing": AssetReconciliationScenario(
        assets=single_lazy_asset_with_freshness_policy,
        unevaluated_runs=[],
        expected_run_requests=[run_request(asset_keys=["asset1"])],
    ),
    "auto_materialize_policy_eager_with_freshness_policies": AssetReconciliationScenario(
        assets=with_auto_materialize_policy(
            overlapping_freshness_inf, AutoMaterializePolicy.eager()
        ),
        cursor_from=AssetReconciliationScenario(
            assets=overlapping_freshness_inf,
            unevaluated_runs=[run(["asset1", "asset2", "asset3", "asset4", "asset5", "asset6"])],
        ),
        # change at the top, should be immediately propagated as all assets have eager reconciliation
        unevaluated_runs=[run(["asset1"])],
        expected_run_requests=[
            run_request(asset_keys=["asset2", "asset3", "asset4", "asset5", "asset6"])
        ],
    ),
    "auto_materialize_policy_lazy_with_freshness_policies": AssetReconciliationScenario(
        assets=with_auto_materialize_policy(
            overlapping_freshness_inf, AutoMaterializePolicy.lazy()
        ),
        cursor_from=AssetReconciliationScenario(
            assets=overlapping_freshness_inf,
            unevaluated_runs=[run(["asset1", "asset2", "asset3", "asset4", "asset5", "asset6"])],
        ),
        # change at the top, should be immediately propagated as all assets have eager reconciliation
        unevaluated_runs=[run(["asset1"])],
        expected_run_requests=[],
    ),
    "auto_materialize_policy_with_default_scope_hourly_to_daily_partitions_never_materialized": AssetReconciliationScenario(
        assets=with_auto_materialize_policy(
            hourly_to_daily_partitions,
            AutoMaterializePolicy.eager(),
        ),
        unevaluated_runs=[],
        current_time=create_pendulum_time(year=2013, month=1, day=7, hour=4),
        expected_run_requests=[
            # with default scope, only the last partition is materialized
            run_request(
                asset_keys=["hourly"],
                partition_key=hourly_partitions_def.get_last_partition_key(
                    current_time=create_pendulum_time(year=2013, month=1, day=7, hour=4)
                ),
            )
        ],
    ),
    "auto_materialize_policy_hourly_to_daily_partitions_never_materialized": AssetReconciliationScenario(
        assets=with_auto_materialize_policy(
            hourly_to_daily_partitions,
            AutoMaterializePolicy(
                on_missing=True,
                for_freshness=True,
                on_new_parent_data=True,
                max_materializations_per_minute=48,
            ),
        ),
        unevaluated_runs=[],
        current_time=create_pendulum_time(year=2013, month=1, day=7, hour=4),
        expected_run_requests=[
            run_request(asset_keys=["hourly"], partition_key=partition_key)
            for partition_key in hourly_partitions_def.get_partition_keys_in_range(
                PartitionKeyRange(start="2013-01-05-04:00", end="2013-01-07-03:00")
            )
        ],
        expected_conditions={
            **{
                ("hourly", p): {MissingAutoMaterializeCondition()}
                for p in hourly_partitions_def.get_partition_keys_in_range(
                    PartitionKeyRange(start="2013-01-05-4:00", end="2013-01-07-03:00")
                )
            },
            **{
                ("hourly", p): {
                    MaxMaterializationsExceededAutoMaterializeCondition(),
                    MissingAutoMaterializeCondition(),
                }
                for p in hourly_partitions_def.get_partition_keys_in_range(
                    PartitionKeyRange(start="2013-01-05-00:00", end="2013-01-05-03:00")
                )
            },
            ("daily", "2013-01-05"): {
                ParentOutdatedAutoMaterializeCondition(
                    waiting_on_asset_keys=frozenset({AssetKey("hourly")}),
                ),
                MissingAutoMaterializeCondition(),
            },
            ("daily", "2013-01-06"): {
                ParentOutdatedAutoMaterializeCondition(
                    waiting_on_asset_keys=frozenset({AssetKey("hourly")}),
                ),
                MissingAutoMaterializeCondition(),
            },
        },
    ),
    "auto_materialize_policy_hourly_to_daily_partitions_never_materialized2": AssetReconciliationScenario(
        assets=with_auto_materialize_policy(
            hourly_to_daily_partitions,
            AutoMaterializePolicy(
                on_missing=True,
                for_freshness=True,
                on_new_parent_data=False,
                max_materializations_per_minute=48,
            ),
        ),
        unevaluated_runs=[],
        current_time=create_pendulum_time(year=2013, month=1, day=7, hour=4),
        expected_run_requests=[
            run_request(asset_keys=["hourly"], partition_key=partition_key)
            for partition_key in hourly_partitions_def.get_partition_keys_in_range(
                PartitionKeyRange(start="2013-01-05-04:00", end="2013-01-07-03:00")
            )
        ],
    ),
    "auto_materialize_policy_lazy_parent_rematerialized_one_partition": AssetReconciliationScenario(
        assets=with_auto_materialize_policy(
            two_assets_in_sequence_one_partition,
            AutoMaterializePolicy.lazy(),
        ),
        unevaluated_runs=[
            run(["asset1", "asset2"], partition_key="a"),
            single_asset_run(asset_key="asset1", partition_key="a"),
        ],
        # no need to rematerialize as this is a lazy policy
        expected_run_requests=[],
    ),
    "auto_materialize_policy_max_materializations_exceeded": AssetReconciliationScenario(
        assets=with_auto_materialize_policy(
            hourly_to_daily_partitions,
            AutoMaterializePolicy(
                on_missing=True,
                on_new_parent_data=True,
                for_freshness=False,
                max_materializations_per_minute=1,
            ),
        ),
        unevaluated_runs=[],
        current_time=create_pendulum_time(year=2013, month=1, day=5, hour=5),
        expected_run_requests=[
            run_request(["hourly"], partition_key="2013-01-05-04:00"),
        ],
        expected_conditions={
            ("hourly", "2013-01-05-04:00"): {MissingAutoMaterializeCondition()},
            ("hourly", "2013-01-05-03:00"): {
                MissingAutoMaterializeCondition(),
                MaxMaterializationsExceededAutoMaterializeCondition(),
            },
            ("hourly", "2013-01-05-02:00"): {
                MissingAutoMaterializeCondition(),
                MaxMaterializationsExceededAutoMaterializeCondition(),
            },
            ("hourly", "2013-01-05-01:00"): {
                MissingAutoMaterializeCondition(),
                MaxMaterializationsExceededAutoMaterializeCondition(),
            },
            ("hourly", "2013-01-05-00:00"): {
                MissingAutoMaterializeCondition(),
                MaxMaterializationsExceededAutoMaterializeCondition(),
            },
        },
    ),
    "auto_materialize_policy_max_materializations_not_exceeded": AssetReconciliationScenario(
        assets=with_auto_materialize_policy(
            hourly_to_daily_partitions,
            AutoMaterializePolicy(
                on_missing=True,
                on_new_parent_data=True,
                for_freshness=False,
                max_materializations_per_minute=5,
            ),
        ),
        unevaluated_runs=[],
        current_time=create_pendulum_time(year=2013, month=1, day=5, hour=5),
        expected_run_requests=[
            run_request(["hourly"], partition_key="2013-01-05-04:00"),
            run_request(["hourly"], partition_key="2013-01-05-03:00"),
            run_request(["hourly"], partition_key="2013-01-05-02:00"),
            run_request(["hourly"], partition_key="2013-01-05-01:00"),
            run_request(["hourly"], partition_key="2013-01-05-00:00"),
        ],
        expected_conditions={
            ("hourly", "2013-01-05-04:00"): {MissingAutoMaterializeCondition()},
            ("hourly", "2013-01-05-03:00"): {MissingAutoMaterializeCondition()},
            ("hourly", "2013-01-05-02:00"): {MissingAutoMaterializeCondition()},
            ("hourly", "2013-01-05-01:00"): {MissingAutoMaterializeCondition()},
            ("hourly", "2013-01-05-00:00"): {MissingAutoMaterializeCondition()},
        },
    ),
    "auto_materialize_policy_daily_to_unpartitioned_freshness": AssetReconciliationScenario(
        assets=with_auto_materialize_policy(
            daily_to_unpartitioned,
            AutoMaterializePolicy.eager(),
        ),
        unevaluated_runs=[],
        current_time=create_pendulum_time(year=2020, month=2, day=7, hour=4),
        expected_run_requests=[run_request(asset_keys=["daily"], partition_key="2020-02-06")],
    ),
    "auto_materialize_policy_diamond_duplicate_conditions": AssetReconciliationScenario(
        assets=with_auto_materialize_policy(
            diamond,
            AutoMaterializePolicy.eager(),
        ),
        unevaluated_runs=[run(["asset1", "asset2", "asset3", "asset4"]), run(["asset1", "asset2"])],
        expected_run_requests=[run_request(asset_keys=["asset3", "asset4"])],
        expected_conditions={
            "asset3": {
                ParentMaterializedAutoMaterializeCondition(
                    materialized_asset_keys=frozenset({AssetKey("asset1")})
                )
            },
            "asset4": {
                ParentMaterializedAutoMaterializeCondition(
                    materialized_asset_keys=frozenset({AssetKey("asset1"), AssetKey("asset2")})
                )
            },
        },
    ),
    "auto_materialize_policy_diamond_one_side_updated": AssetReconciliationScenario(
        assets=[
            *diamond[0:3],
            *with_auto_materialize_policy(
                diamond[-1:],
                AutoMaterializePolicy.eager(),
            ),
        ],
        asset_selection=AssetSelection.keys("asset4"),
        unevaluated_runs=[run(["asset1", "asset2", "asset3", "asset4"]), run(["asset1", "asset2"])],
        expected_run_requests=[],
        expected_conditions={
            "asset4": {
                ParentMaterializedAutoMaterializeCondition(
                    materialized_asset_keys=frozenset({AssetKey("asset1"), AssetKey("asset2")})
                ),
                ParentOutdatedAutoMaterializeCondition(
                    waiting_on_asset_keys=frozenset({AssetKey("asset3")})
                ),
            },
        },
    ),
    "time_partitioned_after_partitioned_upstream_missing": AssetReconciliationScenario(
        assets=time_partitioned_eager_after_non_partitioned,
        asset_selection=AssetSelection.keys("time_partitioned", "unpartitioned_downstream"),
        unevaluated_runs=[],
        current_time=create_pendulum_time(year=2013, month=1, day=6, hour=1, minute=5),
        expected_run_requests=[],
    ),
    "time_partitioned_after_partitioned_upstream_materialized": AssetReconciliationScenario(
        assets=time_partitioned_eager_after_non_partitioned,
        asset_selection=AssetSelection.keys("time_partitioned", "unpartitioned_downstream"),
        unevaluated_runs=[run(["unpartitioned_root_a"])],
        current_time=create_pendulum_time(year=2013, month=1, day=5, hour=1, minute=5),
        expected_run_requests=[
            run_request(asset_keys=["time_partitioned"], partition_key="2013-01-05-00:00")
        ],
    ),
    "time_partitioned_after_partitioned_upstream_rematerialized": AssetReconciliationScenario(
        assets=time_partitioned_eager_after_non_partitioned,
        asset_selection=AssetSelection.keys("time_partitioned", "unpartitioned_downstream"),
        unevaluated_runs=[
            run(["unpartitioned_root_a"]),
            run(["time_partitioned"], partition_key="2013-01-05-00:00"),
            run(["unpartitioned_root_a"]),
        ],
        current_time=create_pendulum_time(year=2013, month=1, day=5, hour=1, minute=5),
        # do not execute, as we don't consider the already-materialized partitions to be invalidated
        # by the new materialization of the upstream
        expected_run_requests=[],
    ),
    "time_partitioned_after_partitioned_upstream_rematerialized2": AssetReconciliationScenario(
        assets=time_partitioned_eager_after_non_partitioned,
        asset_selection=AssetSelection.keys("time_partitioned", "unpartitioned_downstream"),
        unevaluated_runs=[
            run(["unpartitioned_root_a"]),
            run(["unpartitioned_root_b"]),
            # backfill
            run(["time_partitioned"], partition_key="2013-01-05-00:00"),
            run(["time_partitioned"], partition_key="2013-01-05-01:00"),
            run(["time_partitioned"], partition_key="2013-01-05-02:00"),
            run(["unpartitioned_downstream"]),
            # new root data
            run(["unpartitioned_root_a"]),
            run(["unpartitioned_root_b"]),
        ],
        current_time=create_pendulum_time(year=2013, month=1, day=5, hour=3, minute=5),
        # able to update the downstream, as time_partitioned is still considered up-to-date
        expected_run_requests=[run_request(["unpartitioned_downstream"])],
    ),
    "static_partitioned_after_partitioned_upstream_rematerialized": AssetReconciliationScenario(
        assets=static_partitioned_eager_after_non_partitioned,
        asset_selection=AssetSelection.keys("static_partitioned"),
        unevaluated_runs=[
            run(["unpartitioned"]),
            run(["static_partitioned"], partition_key="a"),
            run(["static_partitioned"], partition_key="b"),
            run(["unpartitioned"]),
        ],
        # do execute, as we do consider the already-materialized partitions to be invalidated
        # by the new materialization of the upstream
        expected_run_requests=[
            run_request(asset_keys=["static_partitioned"], partition_key="a"),
            run_request(asset_keys=["static_partitioned"], partition_key="b"),
        ],
    ),
    "waiting_on_parents_materialize_condition": AssetReconciliationScenario(
        assets=lopsided_vee,
        asset_selection=AssetSelection.keys("C", "D"),
        cursor_from=AssetReconciliationScenario(
            assets=lopsided_vee,
            asset_selection=AssetSelection.keys("C", "D"),
            cursor_from=AssetReconciliationScenario(
                assets=lopsided_vee,
                asset_selection=AssetSelection.keys("C", "D"),
                unevaluated_runs=[
                    run(["root1", "root2", "A", "B", "C", "D"]),
                    run(["root1", "root2"]),
                ],
                expected_run_requests=[],
                expected_conditions={
                    "D": {
                        ParentMaterializedAutoMaterializeCondition(
                            materialized_asset_keys=frozenset(
                                {AssetKey("root1"), AssetKey("root2")}
                            )
                        ),
                        ParentOutdatedAutoMaterializeCondition(
                            # waiting on A to be materialized (pulling in the new version of root1)
                            waiting_on_asset_keys=frozenset({AssetKey("A")})
                        ),
                    },
                },
            ),
            unevaluated_runs=[run(["A"]), run(["root2"])],
            expected_run_requests=[],
            expected_conditions={
                "D": {
                    ParentMaterializedAutoMaterializeCondition(
                        materialized_asset_keys=frozenset({AssetKey("root2"), AssetKey("A")})
                    ),
                    ParentOutdatedAutoMaterializeCondition(
                        # now waiting on B to be materialized (pulling in the new version of root1/A)
                        waiting_on_asset_keys=frozenset({AssetKey("B")})
                    ),
                },
            },
        ),
        unevaluated_runs=[run(["B"])],
        expected_run_requests=[
            run_request(["C", "D"]),
        ],
        expected_conditions={
            "C": {
                ParentMaterializedAutoMaterializeCondition(
                    materialized_asset_keys=frozenset({AssetKey("B")})
                )
            },
            "D": {
                ParentMaterializedAutoMaterializeCondition(
                    materialized_asset_keys=frozenset({AssetKey("B"), AssetKey("root2")})
                )
            },
        },
    ),
}
