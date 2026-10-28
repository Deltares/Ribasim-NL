"""Mass-load verification helpers for the DELWAQ notebook.

Directions for improvement:

right now, for ANIMO and ER, this module calculates the time-weighted average, and

For the boundaries, it simply forward and backward fills the known values, as delwaq would, to obtain daily mass loads.
This is a much more simple approach and could also be applied to the loads on basins from ANIMO and ER
(this would cause the input data, or at least memory usage if only used for verification, to be much larger)

- links could be added to the boundary plots, right now it is just scattered dots,
which does not quite make you imagine the actual Dutch water system
- this func could simply take model as input,
    and then within the function select the instances necessary for plotting either basins or boundaries

"""

from typing import Literal

import matplotlib.pyplot as plt
import pandas as pd


def verify_mass_loads(
    loads_df,
    model=None,
    spatial_data=None,
    spatial_unit: Literal["basin", "boundary"] | None = None,
    start_date=None,
    end_date=None,
    make_plot=True,
):
    """
    Verify mass-load input for the water-quality model.

    Parameters
    ----------
    loads_df : pandas.DataFrame
        Must contain:
        - node_id
        - time
        - substance
        - load [g/s]

    model : ribasim.Model, optional
        Model instance used to resolve spatial inputs automatically when
        ``spatial_data`` is not provided.

    spatial_data : geopandas.GeoDataFrame, optional
        Spatial units containing node_id and geometry. Use basin polygons when
        ``spatial_unit="basin"`` and boundary points when
        ``spatial_unit="boundary"``. When omitted, this is resolved from the
        model when available.

    spatial_unit : {"basin", "boundary"} or None, default None
        Type of spatial unit used for mapping and load normalization. When
        ``None``, no spatial plot is created.

    start_date, end_date : str or datetime, optional
        Analysis period. Provide both or neither.

    make_plot : bool, default True
        Create verification plots when a spatial unit is selected.

    Returns
    -------
    dict
    """
    if spatial_unit is None:
        make_plot = False
    elif spatial_unit not in {"basin", "boundary"}:
        raise ValueError("spatial_unit must be 'basin', 'boundary', or None.")

    links = None
    if spatial_data is None and model is not None and spatial_unit is not None:
        if spatial_unit == "basin":
            spatial_data = model.basin.area.df.copy()
        elif spatial_unit == "boundary":
            spatial_data = model.flow_boundary.node.df.copy()
            links = model.link.df.copy()

    if spatial_data is None and spatial_unit is not None:
        raise ValueError("Provide spatial_data or a model when spatial_unit is set.")

    # ==============================================================
    # Structure and completeness
    # ==============================================================

    required_columns = {"node_id", "time", "substance", "load"}
    missing_columns = required_columns - set(loads_df.columns)

    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

    if (start_date is None) != (end_date is None):
        raise ValueError("Provide both start_date and end_date, or neither.")

    df = loads_df.copy()
    df["time"] = pd.to_datetime(df["time"])
    if df["time"].isna().any():
        raise ValueError("The time column must not contain missing values.")

    if start_date is None:
        start_date = df["time"].min()
        end_date = df["time"].max()
    else:
        start_date = pd.Timestamp(start_date)
        end_date = pd.Timestamp(end_date)

    if start_date >= end_date:
        raise ValueError("end_date must be after start_date.")

    df = df.loc[df["time"] <= end_date].copy()

    if df.empty:
        raise ValueError(f"No mass-load records found on or before {end_date.date()}.")

    # DELWAQ interprets a load as a state: it remains active until the next
    # value for the same node and substance. Include the last state before the
    # requested period so that forward-filling also works at start_date.
    state_values = (
        df.sort_values("time")
        .pivot_table(
            index="time",
            columns=["node_id", "substance"],
            values="load",
            aggfunc="last",
        )
        .sort_index()
    )
    time_index = state_values.index.union(pd.DatetimeIndex([start_date, end_date])).sort_values()
    state_values = state_values.reindex(time_index).ffill()

    missing_initial_values = state_values.loc[start_date].isna()
    if missing_initial_values.any():
        missing = [
            f"{node_id}/{substance}" for node_id, substance in missing_initial_values[missing_initial_values].index
        ]
        raise ValueError(
            "No load is available at start_date for node/substance pairs: "
            + ", ".join(missing[:10])
            + (" ..." if len(missing) > 10 else "")
        )

    period_values = state_values.loc[start_date:end_date]
    summed_loads = period_values.T.groupby(level="substance").sum().T

    def weighted_means(period_start, period_end):
        """Return node/substance means weighted over [period_start, period_end]."""
        boundaries = period_values.index.union(pd.DatetimeIndex([period_start, period_end])).sort_values()
        values = state_values.reindex(boundaries).ffill()
        if values.loc[period_start].isna().any():
            raise ValueError(f"No load is available at {period_start}.")

        weights = (values.index.to_series().shift(-1) - values.index).dt.total_seconds()
        weights = weights.clip(lower=0).fillna(0)
        valid_intervals = (values.index >= period_start) & (values.index < period_end)
        weights = weights.where(valid_intervals, 0)
        return values.mul(weights, axis=0).sum(axis=0) / weights.sum()

    # Records are retained for diagnostics; all statistics below use the
    # forward-filled state values.
    df = df.loc[df["time"].between(start_date, end_date)].copy()
    if df.empty:
        raise ValueError(f"No mass-load records found between {start_date.date()} and {end_date.date()}.")
    df["year"] = df["time"].dt.year

    print(f"Analysed period: {start_date.date()} to {end_date.date()}")

    print("Number of nodes with a mass load:", df["node_id"].nunique())

    print("Substances:", sorted(df["substance"].unique()))

    print("\nMissing values:")
    print(df.isna().sum())

    # ==============================================================
    # Annual and period statistics
    # ==============================================================

    seconds_per_average_year = 365.25 * 24 * 60 * 60
    annual_frames = []
    for year in range(start_date.year, end_date.year + 1):
        year_start = max(start_date, pd.Timestamp(year=year, month=1, day=1))
        year_end = min(end_date, pd.Timestamp(year=year + 1, month=1, day=1))
        if year_start >= year_end:
            continue
        means = weighted_means(year_start, year_end).rename("load").reset_index()
        means["year"] = year
        annual_frames.append(means)

    annual_node_loads = pd.concat(annual_frames, ignore_index=True)
    annual_distribution = annual_node_loads.groupby(
        ["year", "substance"],
        as_index=False,
    ).agg(
        total_load_g_s=("load", "sum"),
        mean_node_load_g_s=("load", "mean"),
        median_node_load_g_s=("load", "median"),
    )
    annual_distribution["total_load_kg_year"] = annual_distribution["total_load_g_s"] * seconds_per_average_year / 1000

    max_load = df.loc[df["load"].idxmax()]

    print("\nHighest individual load entry in the selected period:")
    print(max_load)

    # ==============================================================
    # Period statistics
    # ==============================================================

    nutrient_substances = {
        "N": ["NO3", "NH4", "OON"],
        "P": ["PO4", "AAP", "OOP"],
    }

    period_node_loads = weighted_means(start_date, end_date).rename("load").reset_index()
    period_substance_totals = period_node_loads.groupby("substance")["load"].sum()

    top_mean_node_loads = {}
    period_results = {}

    for nutrient, substances in nutrient_substances.items():
        nutrient_node_loads = (
            period_node_loads.loc[period_node_loads["substance"].isin(substances)]
            .groupby("node_id", as_index=False)["load"]
            .sum()
            .sort_values("load", ascending=False)
            .head(5)
            .reset_index(drop=True)
        )
        top_mean_node_loads[nutrient] = nutrient_node_loads

        mean_g_s = period_substance_totals.reindex(
            substances,
            fill_value=0,
        ).sum()

        period_results[f"total_{nutrient}_mean_g_s"] = mean_g_s

        period_results[f"total_{nutrient}_mean_t_year"] = mean_g_s * seconds_per_average_year / 1e6

    for nutrient, ranked_loads in top_mean_node_loads.items():
        print(f"Top 5 mean {nutrient} loads by node:")
        print(ranked_loads.to_string(index=False))
        print()

    max_top_load = max(
        (ranked_loads["load"].max() for ranked_loads in top_mean_node_loads.values() if not ranked_loads.empty),
        default=0,
    )
    if max_top_load > 0:
        print(
            "Consider using a log scale for the spatial plots because the top 5 node loads are highly skewed "
            "and a single extreme value can compress the color range for all other nodes."
        )
        print()

    period_statistics = pd.Series(
        {
            "start_date": start_date,
            "end_date": end_date,
            **period_results,
        },
        name="period_statistics",
    )

    print()

    for nutrient in nutrient_substances:
        mean_g_s = period_results[f"total_{nutrient}_mean_g_s"]

        mean_t_year = period_results[f"total_{nutrient}_mean_t_year"]

        print(f"Mean total {nutrient} load: {mean_g_s:.6f} g/s ({mean_t_year:,.0f} tonnes/year)")

    # ==============================================================
    # Prepare map plots
    # ==============================================================

    node_substance_loads = period_node_loads.copy()

    node_substance_loads["load_kg_year"] = node_substance_loads["load"] * seconds_per_average_year / 1000

    spatial_data = spatial_data.copy()
    if "node_id" not in spatial_data.columns:
        if spatial_data.index.name != "node_id":
            raise ValueError("spatial_data must contain a 'node_id' column or have an index named 'node_id'.")
        spatial_data = spatial_data.reset_index()
    spatial_data["node_id"] = pd.to_numeric(spatial_data["node_id"], errors="raise").astype("int64")
    node_substance_loads["node_id"] = pd.to_numeric(
        node_substance_loads["node_id"],
        errors="raise",
    ).astype("int64")

    if spatial_unit == "basin":
        # EPSG:28992 -> area returned in m²
        spatial_data["area_ha"] = spatial_data.geometry.area / 10_000
        if spatial_data["area_ha"].le(0).any() or spatial_data["area_ha"].isna().any():
            raise ValueError("Basin spatial data contains missing or non-positive polygon areas.")

    spatial_node_ids = set(spatial_data["node_id"])
    load_node_ids = set(node_substance_loads["node_id"])
    overlap = spatial_node_ids & load_node_ids
    if not overlap:
        raise ValueError(
            "No node_id values in the load data match the spatial data. "
            f"Load node_id dtype: {node_substance_loads['node_id'].dtype}; "
            f"spatial data node_id dtype: {spatial_data['node_id'].dtype}."
        )
    mapped_label = "basin polygons" if spatial_unit == "basin" else "boundary points"
    print(f"Nodes mapped to {mapped_label}: {len(overlap)} of {len(load_node_ids)}")

    nutrient_maps = {}
    map_column = "load_kg_ha_year" if spatial_unit == "basin" else "load_kg_year"
    map_unit = "kg/ha/year" if spatial_unit == "basin" else "kg/year"

    if spatial_unit is not None:
        for nutrient, substances in nutrient_substances.items():
            # TO PLOT A SINGLE SUBSTANCE INSTEAD:
            # substances = ["NO3"]

            nutrient_loads = (
                node_substance_loads.loc[node_substance_loads["substance"].isin(substances)]
                .groupby(
                    "node_id",
                    as_index=False,
                )["load_kg_year"]
                .sum()
            )

            nutrient_map = spatial_data.merge(
                nutrient_loads,
                on="node_id",
                how="left",
            )

            nutrient_map["load_kg_year"] = nutrient_map["load_kg_year"].fillna(0)

            if spatial_unit == "basin":
                nutrient_map["load_kg_ha_year"] = nutrient_map["load_kg_year"] / nutrient_map["area_ha"]

            nutrient_maps[nutrient] = nutrient_map

    # ==============================================================
    # Plotting
    # ==============================================================

    fig = None

    if make_plot and spatial_unit is not None:
        fig, axes = plt.subplots(
            3,
            2,
            figsize=(16, 20),
        )

        # ----------------------------------------------------------
        # Forward-filled total load per selected substance groups
        # ----------------------------------------------------------

        substance_groups = (
            ("OON, NH4, NO3", ["OON", "NH4", "NO3"]),
            ("OOP, AAP, PO4", ["OOP", "AAP", "PO4"]),
        )
        for ax, (title, substances) in zip(axes[0], substance_groups, strict=True):
            for substance in substances:
                if substance in summed_loads:
                    ax.plot(
                        summed_loads.index,
                        summed_loads[substance],
                        drawstyle="steps-post",
                        marker="o",
                        label=substance,
                    )

            ax.set(
                xlabel="Time",
                ylabel="Total load [g/s]",
                title=f"Forward-filled total load: {title}",
            )
            ax.grid(True, alpha=0.3)
            ax.legend(title="Substance")

        # ----------------------------------------------------------
        # Linear-scale distributions, excluding the highest 1 percent
        # separately for N and P so one nutrient cannot set the other's bins.
        # ----------------------------------------------------------

        for nutrient, ax in zip(("N", "P"), axes[1], strict=True):
            values = nutrient_maps[nutrient][map_column]
            values = values.replace([float("inf"), -float("inf")], pd.NA).dropna()
            values = values[values.gt(0)]
            if values.empty:
                ax.text(
                    0.5,
                    0.5,
                    "No positive finite values",
                    ha="center",
                    va="center",
                    transform=ax.transAxes,
                )
            else:
                upper_limit = values.quantile(0.99)
                values = values[values <= upper_limit]
                if len(values) > 1 and not values.eq(values.iloc[0]).all():
                    ax.hist(
                        values, bins=30, density=True, alpha=0.75
                    )  # density should be lower for N because the bins are wider
                else:
                    ax.axvline(values.iloc[0], color="tab:blue")
            ax.set(
                xlabel=f"Load [{map_unit}]",
                ylabel="Density",
                title=f"Total {nutrient} {spatial_unit} loads (below 99th percentile)",
            )
            ax.grid(alpha=0.3)

        # ----------------------------------------------------------
        # Total N map
        # ----------------------------------------------------------

        vmax_n = nutrient_maps["N"][map_column].replace([float("inf"), -float("inf")], pd.NA).quantile(0.99)

        if spatial_unit == "boundary" and links is not None:
            links.plot(ax=axes[2, 0], color="lightgray", linewidth=0.4, zorder=1)

        nutrient_maps["N"].plot(
            column=map_column,
            cmap="viridis",
            legend=True,
            vmax=vmax_n,
            ax=axes[2, 0],
            zorder=2,
        )

        axes[2, 0].set_title(f"Total N load [{map_unit}]")

        axes[2, 0].axis("off")

        # ----------------------------------------------------------
        # Total P map
        # ----------------------------------------------------------

        vmax_p = nutrient_maps["P"][map_column].replace([float("inf"), -float("inf")], pd.NA).quantile(0.99)

        if spatial_unit == "boundary" and links is not None:
            links.plot(ax=axes[2, 1], color="lightgray", linewidth=0.4, zorder=1)

        nutrient_maps["P"].plot(
            column=map_column,
            cmap="viridis",
            legend=True,
            vmax=vmax_p,
            ax=axes[2, 1],
            zorder=2,
        )

        axes[2, 1].set_title(f"Total P load [{map_unit}]")

        axes[2, 1].axis("off")

        fig.tight_layout()

        plt.show()

    return {
        "filtered_loads": df,
        "annual_node_loads": annual_node_loads,
        "annual_distribution": annual_distribution,
        "forward_filled_loads": period_values,
        "summed_loads": summed_loads,
        "period_node_loads": period_node_loads,
        "period_substance_totals": period_substance_totals,
        "period_statistics": period_statistics,
        "top_5_mean_node_loads": top_mean_node_loads,
        "nutrient_maps": nutrient_maps,
        "max_load": max_load,
        "figure": fig,
    }
