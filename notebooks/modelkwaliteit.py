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

        # duplicated edges
        duplicated_edges = len(model.edge.df[model.edge.df.duplicated()])
        model.edge.df.drop_duplicates(inplace=True)

        # non existing nodes
        mask = model.edge.df.to_node_id.isin(model.node_table().df.index) & model.edge.df.from_node_id.isin(
            model.node_table().df.index
        )
        nodes_not_existing = len(model.edge.df[~mask])
        model.edge.df = model.edge.df[mask]

        data += [
            {
                "waterschap": authority,
                "basin_nodes": len(model.basin.node.df),
                "basin_areas": len(model.basin.area.df),
                "basin_verschil": abs(len(model.basin.node.df) - len(model.basin.area.df)),
                "basin_area_lt_5000m2": len(model.basin.area.df[model.basin.area.df.area < 5000]),
                "verkeerde_in_uitstroom": len(model.invalid_topology_at_node()),
                "dubbele_edges": duplicated_edges,
                "niet-bestaande_knopen_bij_edge": nodes_not_existing,
            }
        ]


df = pd.DataFrame(data)


df.to_excel(cloud.joinpath("modelkwaliteit.xlsx"), index=False)
