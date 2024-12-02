# %%

import pandas as pd

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()


# %%
data = []
for authority in cloud.water_authorities:
    ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model_network")
    if ribasim_dir.exists():
        ribasim_toml = next(ribasim_dir.glob("*.toml"), None)
        if ribasim_toml is not None:
            print(authority)
            model = Model.read(ribasim_toml)
            try:
                valid = model.invalid_topology_at_node().empty
            except KeyError:
                valid = False

            unassigned_basin_area = model.unassigned_basin_area
            if not unassigned_basin_area.empty:
                unassigned_basin_area.to_file(
                    ribasim_dir.joinpath("basin_node_area_fouten.gpkg"), layer="area_niet_toegekend"
                )
            area_mismatch = len(model.basin.node.df) - len(model.basin.area.df)

            unassigned_basin_node = model.basin.node.df[~model.basin.node.df.index.isin(model.basin.area.df.node_id)]
            if not unassigned_basin_node.empty:
                unassigned_basin_node.to_file(
                    ribasim_dir.joinpath("basin_node_area_fouten.gpkg"), layer="node_niet_toegekend"
                )

            data += [
                {
                    "waterschap": authority,
                    "model_valide": valid,
                    "basin_niet_toegekend": len(unassigned_basin_area),
                    "basin_knopen": len(model.basin.node.df),
                    "basin_vlakken": len(model.basin.area.df),
                    "basin_verschil": area_mismatch,
                    "basin_area_lt_5000m2": len(model.basin.area.df[model.basin.area.df.area < 5000]),
                }
            ]

df = pd.DataFrame(data)


df.to_excel(cloud.joinpath("stand_modellen.xlsx"), index=False)

# %%
