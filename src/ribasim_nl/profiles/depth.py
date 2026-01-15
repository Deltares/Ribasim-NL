import geopandas as gpd

from profiles import hydrotopes as ht


def depth_from_hydrotopes(
    hydro_objects: gpd.GeoDataFrame, hydrotope_map: gpd.GeoDataFrame, hydrotopes: ht.HydrotopeTable, **kwargs
) -> gpd.GeoDataFrame:
    if "width" not in hydro_objects.columns:
        msg = "Hydro-objects are missing width estimates"
        raise ValueError(msg)

    # optional arguments
    col_fid: str = kwargs.get("col_fid", "HYDROTYPE2")
    dropna: bool = kwargs.get("dropna", True)
    min_map: bool = kwargs.get("min_map", True)

    # verify definition of all occurring hydrotopes
    unique_fids = hydrotope_map[col_fid].unique()
    if not all(i in unique_fids for i in hydrotopes):
        missing = [i for i in unique_fids if i not in hydrotopes]
        msg = f"Not all occurring hydrotopes initiated: {missing=}"
        raise ValueError(msg)

    # minimise map-columns
    if min_map:
        hydrotope_map = hydrotope_map[[col_fid, "geometry"]]

    def depth_calculator(fid: int, width: float) -> float | None:
        ht = hydrotopes[fid]
        if ht is None:
            return None
        return ht.depth(width)

    # align CRSs
    if hydro_objects.crs != hydrotope_map.crs:
        hydrotope_map.to_crs(hydro_objects.crs)

    temp = gpd.sjoin(hydro_objects, hydrotope_map, how="left", predicate="intersects", rsuffix="map").reset_index(
        drop=False
    )
    temp["overlap"] = temp.apply(
        lambda row: row["geometry"].intersection(hydrotope_map.loc[row["index_map"], "geometry"]).length, axis=1
    )
    idx = temp.groupby("index")["overlap"].idxmax()
    hydro_objects["ht_code"] = temp.loc[idx, col_fid]

    print(f"{hydro_objects.columns=}")
    hydro_objects["depth"] = hydro_objects.apply(lambda row: depth_calculator(row["ht_code"], row["width"]), axis=1)
    if dropna:
        hydro_objects.dropna(subset="depth", inplace=True, ignore_index=True)

    return hydro_objects
