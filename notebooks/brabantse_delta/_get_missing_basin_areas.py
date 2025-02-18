# %%
import geopandas as gpd

from ribasim_nl import Model

authority = "BrabantseDelta"

node_ids = gpd.read_file(
    f"d:/projecten/D2306.LHM_RIBASIM/02.brongegevens/{authority}/modellen/{authority}_fix_model/basin_node_area_errors.gpkg",
    layer="unassigned_basin_node",
).node_id.to_list()

node_ids += [1394]

lhm_model = Model.read(r"d:/projecten/D2306.LHM_RIBASIM/02.brongegevens/Rijkswaterstaat/modellen/lhm/lhm.toml")


mask = (
    lhm_model.basin.node.df.meta_waterbeheerder == authority
) & lhm_model.basin.node.df.meta_node_id_waterbeheerder.isin(node_ids)

basin_area_df = lhm_model.basin.area.df.set_index("node_id")[mask].reset_index()
basin_area_df.loc[:, "node_id"] = basin_area_df.node_id.apply(
    lambda x: lhm_model.basin.node.df.at[x, "meta_node_id_waterbeheerder"]
)

basin_area_df.to_file(f"d:/projecten/D2306.LHM_RIBASIM/02.brongegevens/{authority}/verwerkt/add_basin_area.gpkg")

# %%
