# %%
import time

import geopandas as gpd
import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.check_basin_level import add_check_basin_level
from ribasim_nl.parametrization.level_boundary_table import update_level_boundary_static

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "Vechtstromen"
short_name = "vechtstromen"
run_model = True
run_period = None
static_data_xlsx = cloud.joinpath(authority, "verwerkt/parameters/static_data.xlsx")
aanvoergebieden_gpkg = cloud.joinpath(authority, "verwerkt", "sturing", "aanvoergebieden.gpkg")

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")

cloud.synchronize(filepaths=[static_data_xlsx, aanvoergebieden_gpkg, qlr_path])

# %%
# read
model = Model.read(ribasim_toml)
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

start_time = time.time()
# %%
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=1)
print("Elapsed Time:", time.time() - start_time, "seconds")
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.03


# %%
def sync_basin_profiles_with_target_levels(model: Model, basin_updates: list[tuple[list[int], float]]) -> pd.DataFrame:
    assert model.basin.area.df is not None
    assert model.basin.profile.df is not None

    basin_area_df = model.basin.area.df
    basin_profile_df = model.basin.profile.df
    profile_change_records = []

    for node_ids, new_level in basin_updates:
        for node_id in dict.fromkeys(node_ids):
            area_mask = basin_area_df["node_id"] == node_id
            if not area_mask.any():
                continue

            old_level = basin_area_df.loc[area_mask, "meta_streefpeil"].iloc[0]
            if pd.isna(old_level) or pd.isna(new_level) or old_level == new_level:
                continue

            profile_mask = basin_profile_df["node_id"] == node_id
            if not profile_mask.any():
                continue

            level_shift = new_level - old_level
            basin_profile_df.loc[profile_mask, "level"] += level_shift
            profile_change_records.append(
                {
                    "node_id": node_id,
                    "old_level": old_level,
                    "new_level": new_level,
                    "level_shift": level_shift,
                }
            )

    profile_change_df = pd.DataFrame(profile_change_records)
    if not profile_change_df.empty:
        print(f"Basinprofielen gesynchroniseerd voor {len(profile_change_df)} basins")
        for row in profile_change_df.itertuples():
            print(
                f"node_id={row.node_id}: profielniveau verschoven met {row.level_shift} m "
                f"({row.old_level} -> {row.new_level})"
            )

    return profile_change_df


def sync_basin_profiles_with_level_change_df(model: Model, basin_level_change_df: pd.DataFrame) -> pd.DataFrame:
    assert model.basin.profile.df is not None

    if basin_level_change_df.empty:
        return basin_level_change_df

    basin_profile_df = model.basin.profile.df
    profile_change_records = []

    for row in basin_level_change_df.itertuples():
        if pd.isna(row.old_level) or pd.isna(row.new_level) or row.old_level == row.new_level:
            continue

        profile_mask = basin_profile_df["node_id"] == row.target_basin_id
        if not profile_mask.any():
            continue

        level_shift = row.new_level - row.old_level
        basin_profile_df.loc[profile_mask, "level"] += level_shift
        profile_change_records.append(
            {
                "target_basin_id": row.target_basin_id,
                "old_level": row.old_level,
                "new_level": row.new_level,
                "level_shift": level_shift,
            }
        )

    profile_change_df = pd.DataFrame(profile_change_records)
    if not profile_change_df.empty:
        print(f"ManningResistance-netwerk: {len(profile_change_df)} basinprofielen aangepast")
        for row in profile_change_df.itertuples():
            print(
                f"target_basin_id={row.target_basin_id}: profielniveau verschoven met {row.level_shift} m "
                f"({row.old_level} -> {row.new_level})"
            )

    return profile_change_df


# %%
def validate_basin_levels_against_profiles(model: Model, label: str) -> pd.DataFrame:
    assert model.basin.state.df is not None
    assert model.basin.profile.df is not None

    min_profile_level = model.basin.profile.df.groupby("node_id")["level"].min().rename("min_profile_level")
    basin_level_df = model.basin.state.df.merge(min_profile_level, on="node_id", how="left")
    basin_level_df["level_margin"] = basin_level_df["level"] - basin_level_df["min_profile_level"]

    invalid_df = basin_level_df[
        basin_level_df["min_profile_level"].notna() & basin_level_df["level"].lt(basin_level_df["min_profile_level"])
    ].sort_values("level_margin")

    if invalid_df.empty:
        print(f"{label}: geen basins met level onder profielbodem")
    else:
        print(f"{label}: {len(invalid_df)} basins met level onder profielbodem")
        print(invalid_df[["node_id", "level", "min_profile_level", "level_margin"]].head(20).to_string(index=False))

    return invalid_df


# %%
def _as_node_id_list(node_ids) -> list[int]:
    if node_ids is None:
        return []
    if isinstance(node_ids, pd.Series):
        return [int(node_id) for node_id in node_ids.to_list()]
    if isinstance(node_ids, (list, tuple, set)):
        return [int(node_id) for node_id in node_ids]
    return [int(node_ids)]


def _connected_node_ids(model: Model, node_id: int, direction: str) -> list[int]:
    return _as_node_id_list(getattr(model, f"{direction}_node_id")(node_id))


def _first_node_of_type_along_network(
    model: Model,
    start_node_id: int,
    direction: str,
    target_node_types: tuple[str, ...],
    pass_through_node_types: tuple[str, ...],
    max_iter: int = 500,
) -> int | None:
    frontier = [start_node_id]
    visited = {start_node_id}

    for _ in range(max_iter):
        if not frontier:
            return None

        next_frontier = []
        for current_node_id in frontier:
            for next_node_id in _connected_node_ids(model, current_node_id, direction):
                if next_node_id in visited:
                    continue

                visited.add(next_node_id)
                next_node_type = model.get_node_type(next_node_id)

                if next_node_type in target_node_types:
                    return next_node_id
                if next_node_type in pass_through_node_types:
                    next_frontier.append(next_node_id)

        frontier = next_frontier

    return None


def _first_basin_along_network(model: Model, start_node_id: int, direction: str, max_iter: int = 500) -> int | None:
    return _first_node_of_type_along_network(
        model=model,
        start_node_id=start_node_id,
        direction=direction,
        target_node_types=("Basin",),
        pass_through_node_types=("Junction", "ManningResistance", "Outlet", "Pump", "TabulatedRatingCurve"),
        max_iter=max_iter,
    )


def _find_first_control_node_along_network(
    model: Model, start_node_id: int, direction: str, max_iter: int = 500
) -> int | None:
    return _first_node_of_type_along_network(
        model=model,
        start_node_id=start_node_id,
        direction=direction,
        target_node_types=("Outlet", "Pump"),
        pass_through_node_types=("Basin", "Junction", "ManningResistance", "TabulatedRatingCurve"),
        max_iter=max_iter,
    )


def propagate_target_levels_to_manning_upstream_basins(
    model: Model, aanvoergebieden_df: gpd.GeoDataFrame
) -> pd.DataFrame:
    assert model.node.df is not None
    assert model.basin.area.df is not None
    assert model.basin.state.df is not None

    basin_area_df = model.basin.area.df
    supply_area_polygon = aanvoergebieden_df.geometry.union_all()
    manning_gdf = model.node.df.loc[model.node.df["node_type"] == "ManningResistance"].copy()
    manning_gdf = gpd.GeoDataFrame(manning_gdf, geometry="geometry", crs=model.crs)
    manning_gdf = manning_gdf[manning_gdf.within(supply_area_polygon)]
    manning_node_ids = manning_gdf.index.to_list()
    manning_node_ids = [node_id for node_id in manning_node_ids if node_id != 1151]
    basin_level_change_records = []

    for manning_node_id in manning_node_ids:
        upstream_control_node_id = _find_first_control_node_along_network(model, manning_node_id, "upstream")
        if upstream_control_node_id is None:
            continue

        upstream_basin_id = _first_basin_along_network(model, manning_node_id, "upstream")
        if upstream_basin_id is None:
            continue

        control_node_id = _find_first_control_node_along_network(model, manning_node_id, "downstream")
        if control_node_id is None:
            continue

        control_upstream_basin_id = _first_basin_along_network(model, control_node_id, "upstream")
        if control_upstream_basin_id is None:
            continue

        source_mask = basin_area_df["node_id"] == control_upstream_basin_id
        target_mask = basin_area_df["node_id"] == upstream_basin_id
        if not source_mask.any() or not target_mask.any():
            continue

        source_level = basin_area_df.loc[source_mask, "meta_streefpeil"].iloc[0]
        old_level = basin_area_df.loc[target_mask, "meta_streefpeil"].iloc[0]

        if pd.isna(source_level) or old_level == source_level:
            continue

        basin_area_df.loc[target_mask, "meta_streefpeil"] = source_level
        basin_level_change_records.append(
            {
                "manning_node_id": manning_node_id,
                "target_basin_id": upstream_basin_id,
                "old_level": old_level,
                "new_level": source_level,
                "control_node_id": control_node_id,
                "control_node_type": model.get_node_type(control_node_id),
                "source_basin_id": control_upstream_basin_id,
                "upstream_control_node_id": upstream_control_node_id,
            }
        )

    model.basin.state.df = basin_area_df[["node_id", "meta_streefpeil"]].rename(columns={"meta_streefpeil": "level"})

    basin_level_change_df = pd.DataFrame(basin_level_change_records)
    if basin_level_change_df.empty:
        print("Geen basinpeilen aangepast via ManningResistance-netwerk.")
        return basin_level_change_df

    basin_level_change_df = basin_level_change_df.sort_values(["target_basin_id", "manning_node_id"]).drop_duplicates(
        subset=["target_basin_id"], keep="last"
    )

    print(f"ManningResistance-netwerk: {len(basin_level_change_df)} basinpeilen aangepast")
    for row in basin_level_change_df.itertuples():
        print(
            f"target_basin_id={row.target_basin_id}: {row.old_level} -> {row.new_level} "
            f"(manning_node_id={row.manning_node_id}, "
            f"source_basin_id={row.source_basin_id}, "
            f"control_node_id={row.control_node_id}, control_node_type={row.control_node_type}, "
            f"upstream_control_node_id={row.upstream_control_node_id})"
        )

    sync_basin_profiles_with_level_change_df(model=model, basin_level_change_df=basin_level_change_df)

    return basin_level_change_df


def sync_downstream_control_levels_with_target_levels(
    model: Model, basin_updates: list[tuple[list[int], float]]
) -> pd.DataFrame:
    assert model.basin.area.df is not None
    assert model.outlet.static.df is not None
    assert model.pump.static.df is not None

    basin_area_df = model.basin.area.df
    update_records = []

    for basin_node_ids, _ in basin_updates:
        for basin_node_id in dict.fromkeys(basin_node_ids):
            basin_mask = basin_area_df["node_id"] == basin_node_id
            if not basin_mask.any():
                continue

            basin_level = basin_area_df.loc[basin_mask, "meta_streefpeil"].iloc[0]
            if pd.isna(basin_level):
                continue

            frontier = [basin_node_id]
            visited = {basin_node_id}
            control_node_ids = set()

            while frontier:
                next_frontier = []
                for current_node_id in frontier:
                    for next_node_id in _as_node_id_list(model.downstream_node_id(current_node_id)):
                        if next_node_id in visited:
                            continue

                        visited.add(next_node_id)
                        next_node_type = model.get_node_type(next_node_id)

                        if next_node_type in {"Outlet", "Pump"}:
                            control_node_ids.add(next_node_id)
                        elif next_node_type in {"Basin", "Junction", "ManningResistance", "TabulatedRatingCurve"}:
                            next_frontier.append(next_node_id)

                frontier = next_frontier

            for node_type, static_df in [("Outlet", model.outlet.static.df), ("Pump", model.pump.static.df)]:
                node_ids = [node_id for node_id in control_node_ids if model.get_node_type(node_id) == node_type]
                for node_id in node_ids:
                    static_mask = static_df["node_id"] == node_id
                    if not static_mask.any():
                        continue

                    old_level = static_df.loc[static_mask, "min_upstream_level"].min()
                    if pd.isna(old_level) or old_level >= basin_level:
                        continue

                    static_df.loc[static_mask, "min_upstream_level"] = basin_level
                    update_records.append(
                        {
                            "basin_node_id": basin_node_id,
                            "control_node_id": node_id,
                            "control_node_type": node_type,
                            "old_min_upstream_level": old_level,
                            "new_min_upstream_level": basin_level,
                        }
                    )

    update_df = pd.DataFrame(update_records).drop_duplicates(subset=["control_node_id"], keep="last")
    if not update_df.empty:
        print(f"Benedenstroomse Outlet/Pump min_upstream_levels gesynchroniseerd voor {len(update_df)} nodes")
        for row in update_df.itertuples():
            print(
                f"basin_node_id={row.basin_node_id} -> {row.control_node_type} {row.control_node_id}: "
                f"{row.old_min_upstream_level} -> {row.new_min_upstream_level}"
            )

    return update_df


# %%
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2019, ["meta_categorie"]] = "Inlaat"


propagate_target_levels_to_manning_upstream_basins(
    model=model,
    aanvoergebieden_df=aanvoergebieden_df,
)

basin_level_overrides = [
    ([2157, 1561, 1660], 10),
    ([1493], 10.1),
    ([1643], 6.8),
    ([2192], 5.7),
    ([1844], 10.22),
    ([2030, 1433, 2085], 5.75),
    ([1768, 1681], 10),
    ([1605], 17.7),
    ([1864, 1659], 10.5),
    ([1160, 1723, 1554, 1623], 7.35),
    ([2238, 1388, 1428, 1421, 1479, 1670, 1834, 1839, 2156], 4.39),
    ([1635, 1544, 1634, 1823, 1830, 2147], 8.35),
    ([2150, 1461, 1534, 1540, 1574], 7.35),
    ([1393, 1513], 2.65),
    ([1744, 2153], 3.73),
    ([1405, 1730, 2178, 2003, 2163, 2222, 1700, 1633, 1881, 1843], 9.1),  # Kanaal Almelo-De Haandrik zomerpeil
    ([1852, 1448, 1847, 2308], 7.1),  # De Haandrik - Hardenberg
    ([1637, 1495], 5.60),  # Hardenberg - Marinberg
    ([1593], 4.50),  # Mariënberg - Junne
    ([2158, 2180], 2.65),  # Junne - Vilsteren
    ([1862], 11.2),  # Vossebeltsluis
    (
        [1879, 1878, 1873, 2061, 1621, 1644],
        12.95,
    ),  # Noordscheschutsluis; Stieltjeskanaalsluis; Nieuw #Zwindersesluis
    ([1856, 1518, 1442, 1528], 17.7),  # Oranjesluis; Bargersluis in Oranjekanaal vormt de waterscheiding
    ([2340], 9.7),  # Westerhaar aanvoerbasin, node_id vastgezet in 01_fix_model.py
]


sync_basin_profiles_with_target_levels(model=model, basin_updates=basin_level_overrides)

for node_ids, meta_streefpeil in basin_level_overrides:
    mask = model.basin.area.df.node_id.isin(node_ids)
    model.basin.area.df.loc[mask, "meta_streefpeil"] = meta_streefpeil

# Herbereken afgeleide tabellen na handmatige streefpeil-overrides.
model.basin.state.df = model.basin.area.df[["node_id", "meta_streefpeil"]].rename(columns={"meta_streefpeil": "level"})
validate_basin_levels_against_profiles(model=model, label="Na basin_level_overrides")
sync_downstream_control_levels_with_target_levels(model=model, basin_updates=basin_level_overrides)

# Outlet 704 ligt benedenstrooms van basin 1545; gebruik het streefpeil van dat basin als drempelpeil.
model.outlet.static.df.loc[model.outlet.static.df.node_id == 704, "min_upstream_level"] = 10.5

# Herbereken level boundaries op basis van de actuele basinpeilen.
update_level_boundary_static(
    model=model,
    static_data_xlsx=static_data_xlsx,
    code_column="meta_code_waterbeheerder",
)

# %%
node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA

# %%
# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
add_check_basin_level(model=model)
model.write(ribasim_toml)

# %%

# run model
if run_model:
    if run_period is not None:
        model.endtime = model.starttime + run_period
        model.write(ribasim_toml)
    result = model.run()
    assert result.exit_code == 0

    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_afvoer()
# %%
