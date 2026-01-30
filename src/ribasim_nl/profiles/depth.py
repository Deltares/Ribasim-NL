import geopandas as gpd
import shapely

from profiles import hydrotopes as ht


def make_depth_profiles(
    profile_points: gpd.GeoDataFrame, col_profile_id: str = "profiellijnid", col_z: str = "hoogte"
) -> gpd.GeoDataFrame:
    """Translate profile point data to profile line data.

    :param profile_points: geospatial dataset with profile points
    :param col_profile_id: column profile ID, defaults 'profiellijnid'
    :param col_z: column z-coordinate, defaults to 'hoogte'

    :type profile_points: geopandas.GeoDataFrame
    :type col_profile_id: str, optional
    :type col_z: str, optional

    :return: geospatial dataset with profile lines
    :rtype: geopandas.GeoDataFrame
    """
    if not all(profile_points.has_z):
        profile_points["geometry"] = profile_points.apply(
            lambda row: shapely.Point(row.geometry.x, row.geometry.y, row[col_z]), axis=1
        )
    profile_lines = profile_points.groupby(col_profile_id)["geometry"].apply(
        lambda g: shapely.LineString(g) if len(g) > 1 else None
    )
    profile_lines.dropna(inplace=True)
    return gpd.GeoDataFrame(
        {"profiellijnid": profile_lines.index}, geometry=profile_lines.values, crs=profile_points.crs
    )


def depth_from_hydrotopes(
    hydro_objects: gpd.GeoDataFrame, hydrotope_map: gpd.GeoDataFrame, hydrotopes: ht.HydrotopeTable, **kwargs
) -> gpd.GeoDataFrame:
    """Determine representative depths of hydro-objects based on their representative width and hydrotope.

    :param hydro_objects: geospatial data of hydro-objects
    :param hydrotope_map: geospatial data of hydrotopes
    :param hydrotopes: table with hydrotope-data
    :param kwargs: optional arguments

    :key col_fid: column-name with hydrotope ID data (in `hydrotope_map`), defaults to 'HYDROTOPE2'
    :key drop_na: drop hydro-objects for which depth cannot be determined, defaults to True
    :key min_map: minimal joining of `hydrotope_map` to the `hydro_objects`, defaults to True

    :type hydro_objects: geopandas.GeoDataFrame
    :type hydrotope_map: geopandas.GeoDataFrame
    :type hydrotopes: profiles.hydrotopes.HydrotopeTable

    :return: hydro-objects with depth-estimates
    :rtype: geopandas.GeoDataFrame

    :raises ValueError: if hydro-objects are missing width-estimates
    """

    def depth_calculator(fid: int, width: float) -> float | None:
        """Calculation of depth based on overlapping hydrotope."""
        ht = hydrotopes.get_by_fid(fid)
        if ht is None:
            return None
        return ht.depth(width)

    # verify required width estimations of hydro-objects
    if "width" not in hydro_objects.columns:
        msg = "Hydro-objects are missing width estimates"
        raise ValueError(msg)

    # optional arguments
    col_fid: str = kwargs.get("col_fid", "HYDROTYPE2")
    drop_na: bool = kwargs.get("drop_na", True)
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

    # align CRSs
    if hydro_objects.crs != hydrotope_map.crs:
        hydrotope_map.to_crs(hydro_objects.crs)

    # find overlapping hydrotopes per hydro-object
    temp = gpd.sjoin(hydro_objects, hydrotope_map, how="left", predicate="intersects", rsuffix="map").reset_index(
        drop=False
    )
    temp["overlap"] = temp.apply(
        lambda row: row["geometry"].intersection(hydrotope_map.loc[row["index_map"], "geometry"]).length, axis=1
    )

    # assign the hydrotope with the largest overlap per hydro-object
    idx = temp.groupby("index")["overlap"].idxmax()
    hydro_objects["ht_code"] = temp.loc[idx, col_fid]

    # calculate depth
    hydro_objects["depth"] = hydro_objects.apply(lambda row: depth_calculator(row["ht_code"], row["width"]), axis=1)
    if drop_na:
        hydro_objects.dropna(subset="depth", inplace=True, ignore_index=True)

    # return updated hydro-objects
    return hydro_objects
