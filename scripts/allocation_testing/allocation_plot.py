from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import xarray as xr
from matplotlib import colors
from matplotlib.cm import ScalarMappable
from ribasim import Model

ROOT = Path("./scripts/allocation_testing")
PLOTS_DIR = ROOT / "plots"
SCENARIOS = ["constant", "dynamic"]
TARGET_NODE_ID = 6335
TARGET_LINK_IDS = [485, 10364]


def read_snapshot(model: Model, filename: str, variable: str, time_idx: int) -> pd.DataFrame:
    with xr.open_dataset(model.results_path / filename) as ds:
        return ds[[variable]].isel(time=time_idx).to_dataframe().reset_index()


def read_results_link(model: Model, link_id: int, variable: str = "flow_rate") -> pd.Series:
    with xr.open_dataset(model.results_path / "flow.nc") as ds:
        series = ds[variable].sel(link_id=link_id).to_pandas()
    series.name = variable
    return series


def read_results_node(model: Model, node_id: int, variable: str = "level") -> pd.Series:
    with xr.open_dataset(model.results_path / "basin.nc") as ds:
        series = ds[variable].sel(node_id=node_id).to_pandas()
    series.name = variable
    return series


def make_spatial_diff(
    geometry_df: gpd.GeoDataFrame,
    id_col: str,
    disabled_df: pd.DataFrame,
    enabled_df: pd.DataFrame,
    value_col: str,
) -> gpd.GeoDataFrame:
    diff = geometry_df.merge(
        disabled_df[[id_col, value_col]].rename(columns={value_col: "disabled"}),
        on=id_col,
        how="inner",
    ).merge(
        enabled_df[[id_col, value_col]].rename(columns={value_col: "enabled"}),
        on=id_col,
        how="inner",
    )
    diff["diff"] = diff["enabled"] - diff["disabled"]
    return gpd.GeoDataFrame(diff, geometry="geometry", crs=geometry_df.crs)


def save_diff_map(gdf: gpd.GeoDataFrame, title: str, colorbar_label: str, out_path: Path, **plot_kwargs):
    fig, ax = plt.subplots(figsize=(12, 12))
    vmax = gdf["diff"].abs().max()
    if pd.isna(vmax) or vmax == 0:
        vmax = 1.0
    norm = colors.TwoSlopeNorm(vmin=-vmax, vcenter=0.0, vmax=vmax)
    gdf.plot(ax=ax, column="diff", cmap="coolwarm", norm=norm, **plot_kwargs)
    cbar = plt.colorbar(ScalarMappable(norm=norm, cmap="coolwarm"), ax=ax, shrink=0.7)
    cbar.set_label(colorbar_label)
    ax.set_title(title)
    ax.set_axis_off()
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def save_series_with_diff(disabled: pd.Series, enabled: pd.Series, title: str, y_label: str, out_path: Path):
    merged = pd.concat(
        [disabled.rename("disabled"), enabled.rename("enabled")],
        axis=1,
        join="inner",
    )
    merged["diff"] = merged["enabled"] - merged["disabled"]

    fig, (ax_top, ax_bottom) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    ax_top.plot(merged.index, merged["disabled"], label="disabled")
    ax_top.plot(merged.index, merged["enabled"], label="enabled")
    ax_top.set_title(title)
    ax_top.set_ylabel(y_label)
    ax_top.grid(True)
    ax_top.legend()

    ax_bottom.plot(merged.index, merged["diff"], color="black")
    ax_bottom.axhline(0.0, color="gray", linewidth=1.0)
    unit = y_label.split("[")[-1].rstrip("]") if "[" in y_label else ""
    ax_bottom.set_ylabel(f"difference [{unit}]")
    ax_bottom.set_xlabel("time")
    ax_bottom.grid(True)

    fig.autofmt_xdate()
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def main():
    PLOTS_DIR.mkdir(exist_ok=True)

    for scenario in SCENARIOS:
        disabled = Model.read(ROOT / f"{scenario}_alloc_disabled" / "hws.toml")
        enabled = Model.read(ROOT / f"{scenario}_alloc_enabled" / "hws.toml")

        flow_links = enabled.link.df.reset_index()
        if "link_id" not in flow_links.columns:
            flow_links = flow_links.rename(columns={flow_links.columns[0]: "link_id"})
        flow_links = flow_links[flow_links["link_type"] == "flow"].copy()

        basin_nodes = enabled.basin.node.df.reset_index()
        if "node_id" not in basin_nodes.columns:
            basin_nodes = basin_nodes.rename(columns={basin_nodes.columns[0]: "node_id"})

        flow_first = make_spatial_diff(
            flow_links,
            id_col="link_id",
            disabled_df=read_snapshot(disabled, "flow.nc", "flow_rate", time_idx=1),
            enabled_df=read_snapshot(enabled, "flow.nc", "flow_rate", time_idx=1),
            value_col="flow_rate",
        )
        flow_final = make_spatial_diff(
            flow_links,
            id_col="link_id",
            disabled_df=read_snapshot(disabled, "flow.nc", "flow_rate", time_idx=-1),
            enabled_df=read_snapshot(enabled, "flow.nc", "flow_rate", time_idx=-1),
            value_col="flow_rate",
        )
        basin_first = make_spatial_diff(
            basin_nodes,
            id_col="node_id",
            disabled_df=read_snapshot(disabled, "basin.nc", "level", time_idx=1),
            enabled_df=read_snapshot(enabled, "basin.nc", "level", time_idx=1),
            value_col="level",
        )
        basin_final = make_spatial_diff(
            basin_nodes,
            id_col="node_id",
            disabled_df=read_snapshot(disabled, "basin.nc", "level", time_idx=-1),
            enabled_df=read_snapshot(enabled, "basin.nc", "level", time_idx=-1),
            value_col="level",
        )

        save_diff_map(
            flow_first,
            title=f"{scenario.capitalize()} inflow: flow-link difference after first timestep",
            colorbar_label="Flow-rate difference after first timestep [m3/s]\n(enabled - disabled)",
            out_path=PLOTS_DIR / f"{scenario}_flow_difference_first_timestep.png",
            linewidth=2.5,
        )
        save_diff_map(
            flow_final,
            title=f"{scenario.capitalize()} inflow: final timestep flow-link difference",
            colorbar_label="Final timestep flow-rate difference [m3/s]\n(enabled - disabled)",
            out_path=PLOTS_DIR / f"{scenario}_flow_difference_final_timestep.png",
            linewidth=2.5,
        )
        save_diff_map(
            basin_first,
            title=f"{scenario.capitalize()} inflow: basin-level difference after first timestep",
            colorbar_label="Basin-level difference after first timestep [m]\n(enabled - disabled)",
            out_path=PLOTS_DIR / f"{scenario}_basin_difference_first_timestep.png",
            markersize=20,
        )
        save_diff_map(
            basin_final,
            title=f"{scenario.capitalize()} inflow: final timestep basin-level difference",
            colorbar_label="Final timestep basin-level difference [m]\n(enabled - disabled)",
            out_path=PLOTS_DIR / f"{scenario}_basin_difference_final_timestep.png",
            markersize=20,
        )

        for link_id in TARGET_LINK_IDS:
            save_series_with_diff(
                read_results_link(disabled, link_id=link_id),
                read_results_link(enabled, link_id=link_id),
                title=f"{scenario.capitalize()} inflow: link {link_id} flow rate",
                y_label="flow rate [m3/s]",
                out_path=PLOTS_DIR / f"{scenario}_link_{link_id}_timeseries.png",
            )

        save_series_with_diff(
            read_results_node(disabled, node_id=TARGET_NODE_ID),
            read_results_node(enabled, node_id=TARGET_NODE_ID),
            title=f"{scenario.capitalize()} inflow: basin node {TARGET_NODE_ID} level",
            y_label="level [m]",
            out_path=PLOTS_DIR / f"{scenario}_node_{TARGET_NODE_ID}_timeseries.png",
        )


if __name__ == "__main__":
    main()
