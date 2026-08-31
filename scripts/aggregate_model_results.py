"""Aggregate model results over time and space."""

from pathlib import Path

import geopandas as gpd
from ribasim_nl.zonebudget import (
    NodeGroups,
    aggregate_seasonal_results,
    aggregate_spatial_results,
    aggregate_time_results,
)

from ribasim_nl import Model

SOURCE_MODEL = Path("data/Rijkswaterstaat/modellen/lhm_val_3yr/lhm_coupled.toml")
ECHO_POLYGONS = Path("path/to/ECHO_toestroom_landelijk_v11.shp")
WS_TARGET_MODEL = Path("data/Rijkswaterstaat/modellen/lhm_ws_3yr/lhm_ws.toml")
ECHO_TARGET_MODEL = Path("data/Rijkswaterstaat/modellen/lhm_echo_3yr/lhm_echo.toml")


def write_outputs(results, target_model: Path) -> None:
    written_toml = results.write_visualization_model(target_model.parent)
    written_toml.replace(target_model)


def main() -> None:
    model = Model.read(SOURCE_MODEL)
    source = model.to_xugrid(add_flow=True)

    authority_groups = NodeGroups.from_node_column(
        model,
        "meta_waterbeheerder",
        exclude=["Rijkswaterstaat"],
    )
    authority_results = aggregate_spatial_results(
        model,
        authority_groups,
        dataset=aggregate_time_results(source, "YS"),
        aggregate_flow_boundaries=True,
        flow_boundary_filter={"meta_categorie": "RWZI"},
    )
    write_outputs(authority_results, WS_TARGET_MODEL)

    echo_polygons = gpd.read_file(ECHO_POLYGONS, columns=["ECHO_ID", "geometry"])
    node_df = model.node.df
    assert node_df is not None
    assert node_df.crs is not None
    echo_polygons = echo_polygons.set_crs(node_df.crs)
    echo_groups = NodeGroups.from_polygons(model, echo_polygons, "ECHO_ID")
    echo_results = aggregate_spatial_results(
        model,
        echo_groups,
        dataset=aggregate_seasonal_results(source),
        aggregate_flow_boundaries=True,
        flow_boundary_filter={"meta_categorie": "RWZI"},
    )
    write_outputs(echo_results, ECHO_TARGET_MODEL)


if __name__ == "__main__":
    main()
