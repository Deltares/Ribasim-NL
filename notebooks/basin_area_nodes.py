# %%

import pandas as pd

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()


# %%
data = []
for authority in cloud.water_authorities:
    ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3", "model.toml")
    if ribasim_toml.exists():
        model = Model.read(ribasim_toml)
        data += [
            {
                "waterschap": authority,
                "basin_nodes": len(model.basin.node.df),
                "basin_areas": len(model.basin.area.df),
                "basin_verschil": abs(len(model.basin.node.df) - len(model.basin.area.df)),
                "basin_area_lt_5000m2": len(model.basin.area.df[model.basin.area.df.area < 5000]),
            }
        ]

df = pd.DataFrame(data)


df.to_excel(cloud.joinpath("verschil_basins.xlsx"), index=False)
