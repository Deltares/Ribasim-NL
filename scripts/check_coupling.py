# %%
import geopandas as gpd
import pandas as pd
from ribasim_nl.coupling import add_from_to_target_levels, get_node_function

from ribasim_nl import CloudStorage, Model

# %%
cloud = CloudStorage()
toml_path = cloud.joinpath(r"rijkswaterstaat/modellen/lhm_coupled/lhm_coupled.toml")
model = Model.read(filepath=toml_path)

# %%
df = gpd.read_file(toml_path.with_name("link.gpkg"))
df[["from_node_functie", "to_node_functie"]] = df.apply(
    lambda row: pd.Series(
        [
            get_node_function(model, row.from_node_id),
            get_node_function(model, row.to_node_id),
        ]
    ),
    axis=1,
)
df = add_from_to_target_levels(df=df, model=model)
