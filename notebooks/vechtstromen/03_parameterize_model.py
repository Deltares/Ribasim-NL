# %%
import time

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

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")

cloud.synchronize(filepaths=[static_data_xlsx, qlr_path])

# %%
# read
model = Model.read(ribasim_toml)

start_time = time.time()
# %%
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=1)
print("Elapsed Time:", time.time() - start_time, "seconds")
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.04


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
model.outlet.static.df.loc[model.outlet.static.df.node_id == 704, "min_upstream_level"] = 10.5
model.pump.static.df.loc[model.pump.static.df.node_id == 672, "max_flow_rate"] = 1.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 375, "max_flow_rate"] = 1.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 947, "max_flow_rate"] = 2.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 260, "max_flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 532, "max_flow_rate"] = 2.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1060, "max_flow_rate"] = 2.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 44, "max_flow_rate"] = 1.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 260, "max_flow_rate"] = 0.5


basin_level_overrides = [
    ([2157, 1561, 1660], 10),
    ([1493], 10.1),
    ([1643], 6.8),
    ([1844], 10.22),
    ([2030, 1433, 2085], 5.75),
    ([1768, 1681], 10),
    ([1605], 17.7),
    ([1864, 1659], 10.5),
    ([1160, 1723, 1554], 7.35),
    ([2238, 1388, 1428, 1421, 1479, 1670, 1834, 1839, 2156], 4.39),
    ([1544, 1623], 6),
    ([1635, 1544, 1634, 1823, 1830, 2147], 8.35),
    ([2150, 1461, 1534, 1540, 1574], 7.35),
    ([1393, 1513], 1.25),
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
]


sync_basin_profiles_with_target_levels(model=model, basin_updates=basin_level_overrides)

for node_ids, meta_streefpeil in basin_level_overrides:
    mask = model.basin.area.df.node_id.isin(node_ids)
    model.basin.area.df.loc[mask, "meta_streefpeil"] = meta_streefpeil

# Herbereken afgeleide tabellen na handmatige streefpeil-overrides.
model.basin.state.df = model.basin.area.df[["node_id", "meta_streefpeil"]].rename(columns={"meta_streefpeil": "level"})
validate_basin_levels_against_profiles(model=model, label="Na basin_level_overrides")
sync_downstream_control_levels_with_target_levels(model=model, basin_updates=basin_level_overrides)

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
