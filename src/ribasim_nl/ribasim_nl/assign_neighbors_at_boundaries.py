# %%
import geopandas as gpd

from ribasim_nl import CloudStorage
from ribasim_nl.model import Model

cloud = CloudStorage()

noordzee_gpkg = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/noordzee.gpkg")


def assign_noordzee(model: Model):
    """Assign 'Noordzee' as authority to nodes within the Noordzee polygon."""
    cloud.synchronize([noordzee_gpkg])
    noordzee_poly = gpd.read_file(noordzee_gpkg).union_all()

    mask = model.level_boundary.node.df.within(noordzee_poly)
    model.level_boundary.node.df.loc[mask, "meta_couple_authority"] = "Noordzee"
