# %%
import geopandas as gpd
from ribasim_nl import CloudStorage
from ribasim_nl.geodataframe import direct_basins, join_by_poly_overlay, split_basins

cloud = CloudStorage()

# %% Prepare RWS krw_basin_polygons

krw_poly_gdf = gpd.read_file(
    cloud.joinpath(
        "Basisgegevens", "KRW", "krw_oppervlaktewaterlichamen_nederland_vlakken.gpkg"
    )
)

krw_split_lines_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "krw_split_lijnen.gpkg")
)

rws_opp_poly_gdf = gpd.read_file(
    cloud.joinpath(
        "Rijkswaterstaat", "aangeleverd", "oppervlaktewaterlichamen_rijk.gpkg"
    )
)


rws_krw_poly_gdf = split_basins(krw_poly_gdf, krw_split_lines_gdf)

rws_krw_poly_gdf = join_by_poly_overlay(
    rws_krw_poly_gdf,
    rws_opp_poly_gdf[["waterlichaam", "geometry"]],
    select_by="poly_area",
)


rws_krw_poly = cloud.joinpath("Rijkswaterstaat", "verwerkt", "krw_basins_vlakken.gpkg")

rws_krw_poly_gdf.to_file(rws_krw_poly)


# %% create overlay with krw_lines and polygons
krw_line_gdf = gpd.read_file(
    cloud.joinpath(
        "Basisgegevens", "KRW", "krw_oppervlaktewaterlichamen_nederland_lijnen.gpkg"
    )
)

rws_krw_lines = cloud.joinpath("Rijkswaterstaat", "verwerkt", "krw_basins_lijnen.gpkg")

rws_krw_line_gdf = join_by_poly_overlay(
    gpd.GeoDataFrame(krw_line_gdf.explode()["geometry"]), rws_krw_poly_gdf
)

rws_krw_line_gdf.to_file(rws_krw_lines)

# %% direct basins

basin_ident = "owmident"
link_ident = "Name"

basins_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "krw_basins_vlakken.gpkg")
)

network_gdf = gpd.read_file(
    cloud.joinpath("Basisgegevens", "lsm3-j18_5v6", "shapes", "network_Branches.shp")
)
network_gdf.set_crs(28992, inplace=True)
drop_duplicates = True

poly_directions_gdf = direct_basins(basins_gdf, network_gdf, basin_ident, link_ident)


poly_directions_gdf.to_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "krw_basins_verbindingen.gpkg")
)

# %% snap nodes

# %% build graph

# %% build_network

# %% A(h)
