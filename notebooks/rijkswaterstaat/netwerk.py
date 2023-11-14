# %%
import geopandas as gpd
import pandas as pd
from ribasim_nl import CloudStorage
from ribasim_nl.geometry import cut_basin

cloud = CloudStorage()

# %% inlezen dataframes


rws_opp_poly_gdf = gpd.read_file(
    cloud.joinpath(
        "Rijkswaterstaat", "aangeleverd", "oppervlaktewaterlichamen_rijk.gpkg"
    )
)


# %% write it generic so it can b converted to 1 function
krw_poly_gdf = gpd.read_file(
    cloud.joinpath(
        "Basisgegevens", "KRW", "krw_oppervlaktewaterlichamen_nederland_vlakken.gpkg"
    )
)

krw_split_lines_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "krw_split_lijnen.gpkg")
)

rws_krw_basins = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "krw_basins_vlakken.gpkg"
)

lines_gdf = krw_split_lines_gdf
poly_gdf = krw_poly_gdf

for line in lines_gdf.explode(index_parts=False).itertuples():
    # filter by spatial index
    idx = poly_gdf.sindex.intersection(line.geometry.bounds)
    poly_select_gdf = poly_gdf.iloc[idx][poly_gdf.iloc[idx].intersects(line.geometry)]

    ## filter by intersecting geometry
    poly_select_gdf = poly_select_gdf[poly_select_gdf.intersects(line.geometry)]

    ## filter polygons with two intersection-points only
    poly_select_gdf = poly_select_gdf[
        poly_select_gdf.geometry.boundary.intersection(line.geometry).apply(
            lambda x: False if x.geom_type == "Point" else len(x.geoms) == 2
        )
    ]

    ## if there are no polygon-candidates, something is wrong
    if poly_select_gdf.empty:
        print(
            f"no intersect for {line}. Please make sure it is extended outside the basin on two sides"
        )
    else:
        ## we create 2 new fatures in data
        data = []
        for basin in poly_select_gdf.itertuples():
            kwargs = basin._asdict()
            for geom in cut_basin(basin.geometry, line.geometry).geoms:
                kwargs["geometry"] = geom
                data += [{**kwargs}]

    ## we update poly_gdf with new polygons
    poly_gdf = poly_gdf[~poly_gdf.index.isin(poly_select_gdf.index)]
    poly_gdf = pd.concat(
        [poly_gdf, gpd.GeoDataFrame(data, crs=poly_gdf.crs).set_index("Index")],
        ignore_index=True,
    )

poly_gdf.to_file(rws_krw_basins)

# %% add name
naming_poly_gdf = rws_opp_poly_gdf
naming_column = "waterlichaam"

# start function
columns = list(poly_gdf.columns) + [naming_column]
poly_gdf["left_index"] = poly_gdf.index

overlay_gdf = gpd.overlay(poly_gdf, naming_poly_gdf, how="intersection")
overlay_gdf["geom_area"] = overlay_gdf.geometry.area
overlay_gdf.sort_values(by="geom_area", inplace=True)

overlay_gdf.drop_duplicates(subset="left_index", keep="last", inplace=True)
overlay_gdf.sort_values(by="left_index", inplace=True)
overlay_gdf.index = poly_gdf.index
overlay_gdf = overlay_gdf[columns]

overlay_gdf.loc[:, ["geometry"]] = poly_gdf.geometry
overlay_gdf.to_file(rws_krw_basins)

# %%
