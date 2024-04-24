# %%
from ribasim import Model, Node
from ribasim.nodes import tabulated_rating_curve
from ribasim_nl import CloudStorage
from ribasim_nl.case_conversions import pascal_to_snake_case

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_202470", "hws.toml")
model = Model.read(ribasim_toml)


def change_node_type(model, node_id, node_type, data):
    # get existing network node_type
    existing_node_type = (
        model.node_table().df.set_index("node_id").at[node_id, "node_type"]
    )

    # remove from existing table
    table = getattr(model, pascal_to_snake_case(existing_node_type))
    node = Node(**table.node.df[table.node.df["node_id"] == node_id].iloc[0].to_dict())
    for attr in table.model_fields.keys():
        df = getattr(table, attr).df
        if df is not None:
            getattr(table, attr).df = df[df.node_id != node_id]

    # add to table
    table = getattr(model, pascal_to_snake_case(node_type))
    table.add(node, data)

    # change in edge table
    model.edge.df.loc[
        model.edge.df["from_node_id"] == node_id, ["from_node_type"]
    ] = node_type
    model.edge.df.loc[
        model.edge.df["to_node_id"] == node_id, ["to_node_type"]
    ] = node_type

    return model


# drempelhoogtes
# [crest_level, target_level, verhoogde afvoer, extreme afvoer, ontwerp afvoer]
# Linne: 17 mNAP (legger RWS)
# Roermond: 11.8 m NAP (legger RWS)
# Belfeld: 8.4 m NAP (legger RWS)
# Sambeek: 5.8 m NAP (legger RWS)
# Grave: 2.7 m NAP (legger RWS)
# Lith: 2.5 m NAP (legger RWS)

# Linne
model = change_node_type(
    model,
    node_id=29,
    node_type="TabulatedRatingCurve",
    data=[
        tabulated_rating_curve.Static(
            level=[17, 20.85, 21.15, 22.55, 23.50],
            flow_rate=[0.0, 10, 1250, 2600, 3862],
        )
    ],
)

# Roermond
model = change_node_type(
    model,
    node_id=30,
    node_type="TabulatedRatingCurve",
    data=[
        tabulated_rating_curve.Static(
            level=[11.8, 16.85, 17.3, 20.2, 21.2], flow_rate=[0.0, 10, 1250, 2600, 3862]
        )
    ],
)

# %%
model.write(ribasim_toml)
