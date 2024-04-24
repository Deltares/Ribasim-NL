# %%
from ribasim import Model, Node
from ribasim.nodes import linear_resistance, outlet, tabulated_rating_curve
from ribasim_nl import CloudStorage
from ribasim_nl.case_conversions import pascal_to_snake_case

# %%
cloud = CloudStorage()
ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_prefix", "hws.toml")
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


# %% change to outlets
model = change_node_type(
    model,
    node_id=51,
    node_type="Outlet",
    data=[outlet.Static(flow_rate=[18], min_crest_level=[2])],
)  # Empel

model = change_node_type(
    model,
    node_id=52,
    node_type="Outlet",
    data=[outlet.Static(flow_rate=[18], min_crest_level=[28.65])],
)  # Sluis VI
model = change_node_type(
    model,
    node_id=50,
    node_type="Outlet",
    data=[outlet.Static(flow_rate=[40], min_crest_level=[28.65])],
)  # Sluis I
model = change_node_type(
    model,
    node_id=49,
    node_type="Outlet",
    data=[outlet.Static(flow_rate=[30], min_crest_level=[10])],
)  # Eefde

model = change_node_type(
    model,
    node_id=6,
    node_type="Outlet",
    data=[outlet.Static(flow_rate=[355], min_crest_level=[-4.5])],
)  # Krabbergatsluizen


model = change_node_type(
    model,
    node_id=7,
    node_type="Outlet",
    data=[outlet.Static(flow_rate=[1000], min_crest_level=[-4.5])],
)  # Houtribsluizen

model = change_node_type(
    model,
    node_id=10,
    node_type="Outlet",
    data=[outlet.Static(flow_rate=[700], min_crest_level=[-0.4])],
)  # Ijmuiden


# %% change profiles ML/NB kanalen to resistance
model = change_node_type(
    model,
    node_id=139,
    node_type="LinearResistance",
    data=[linear_resistance.Static(resistance=[0.05], max_flow_rate=[5])],
)

model = change_node_type(
    model,
    node_id=182,
    node_type="LinearResistance",
    data=[linear_resistance.Static(resistance=[0.05], max_flow_rate=[20])],
)
model = change_node_type(
    model,
    node_id=216,
    node_type="LinearResistance",
    data=[linear_resistance.Static(resistance=[0.05], max_flow_rate=[10])],
)

model = change_node_type(
    model,
    node_id=175,
    node_type="LinearResistance",
    data=[linear_resistance.Static(resistance=[0.05], max_flow_rate=[2.5])],
)

# Reevediep
# model = change_node_type(
#     model,
#     node_id=149,
#     node_type="LinearResistance",
#     data=[linear_resistance.Static(resistance=[0.05], max_flow_rate=[1000])],
# )


# %% Meuse resistance to 0.05
# 1 = Borgharen
# 29 = Linne
# 30 = Roermond
# 31 = Belfeld
# 28 = Sambeek
# 26 = Grave
# 25 = Linne (Maxima sluizen)

mask = model.linear_resistance.static.df.resistance == 0.0005
model.linear_resistance.static.df.loc[mask, ["resistance"]] = 0.005

mask = model.linear_resistance.static.df.node_id.isin([29, 30, 31, 28, 26])
model.linear_resistance.static.df.loc[mask, ["resistance"]] = 0.015

# ManningResistance
# model.manning_resistance.static.df.loc[:, ["manning_n"]] = 0.06

# Reevediep
mask = model.manning_resistance.static.df.node_id == 149
model.manning_resistance.static.df.loc[mask, ["manning_n"]] = 0.6

model = change_node_type(
    model,
    node_id=1,
    node_type="TabulatedRatingCurve",
    data=[
        tabulated_rating_curve.Static(
            level=[39.7, 44.1, 44.3, 45.55, 47], flow_rate=[0.0, 10, 1250, 2600, 3862]
        )
    ],
)

# %% fix profile at Eem
# levels uit LSM
bottom_level = -3.63
invert_level = 0.97
geom = model.basin.area.df.set_index("node_id").at[95, "geometry"]
invert_area = geom.area
bottom_area = geom.buffer(-((invert_level - bottom_level) * 2)).area
mask = model.basin.profile.df.node_id == 95
model.basin.profile.df.loc[mask, ["level"]] = [bottom_level, invert_level]
model.basin.profile.df.loc[mask, ["area"]] = [bottom_area, invert_area]

# %% write
ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "hws", "hws.toml")
model.write(ribasim_toml)
