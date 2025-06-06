import itertools
import logging
import math
import os
import pathlib

import fiona
import geopandas as gpd
import numpy as np
import numpy.typing as npt
import pandas as pd
import pydantic
import shapely.ops
import tqdm.auto as tqdm
from shapely.geometry import LineString, MultiLineString, MultiPoint, Point, Polygon

from ribasim_nl import CloudStorage, settings


class ParseCrossings:
    def __init__(
        self,
        gpkg_path: pathlib.Path | str,
        output_path: pathlib.Path | str | None = None,
        allowed_distance: float = 0.5,
        search_radius_structure: float = 60.0,
        search_radius_HWS_BZM: float = 30.0,
        agg_peilgebieden_layer: str | None = None,
        agg_peilgebieden_column: str | None = None,
        agg_areas_threshold: float = 0.8,
        krw_path: pathlib.Path | str | None = None,
        krw_column_id: str | None = None,
        krw_column_name: str | None = None,
        krw_min_overlap: float = 0.1,
        move_distance: float = 1e-3,
        almost_equal: float = 1e-6,
        line_tol: float = 1e-3,
        almost_zero: float = 1e-8,
        list_sep: str = ",",
        disable_progress: bool = False,
        show_log: bool = False,
        logfile: pathlib.Path | str | None = None,
    ) -> None:
        """_summary_

        Parameters
        ----------
        gpkg_path : pathlib.Path | str
            _description_
        output_path : pathlib.Path | str | None, optional
            _description_, by default None
        allowed_distance : float, optional
            _description_, by default 0.5
        search_radius_structure : float, optional
            _description_, by default 60.0
        search_radius_HWS_BZM : float, optional
            _description_, by default 30.0
        agg_peilgebieden_layer : str | None, optional
            _description_, by default None
        agg_peilgebieden_column : str | None, optional
            _description_, by default None
        agg_areas_threshold : float, optional
            _description_, by default 0.8
        krw_path : pathlib.Path | str | None, optional
            _description_, by default None
        krw_column_id : str | None, optional
            _description_, by default None
        krw_column_name : str | None, optional
            _description_, by default None
        krw_min_overlap : float, optional
            _description_, by default 0.1
        move_distance : float, optional
            _description_, by default 1e-3
        almost_equal : float, optional
            _description_, by default 1e-6
        line_tol : float, optional
            _description_, by default 1e-3
        almost_zero : float, optional
            _description_, by default 1e-8
        list_sep : str, optional
            _description_, by default ","
        disable_progress : bool, optional
            _description_, by default False
        show_log : bool, optional
            _description_, by default False
        logfile : pathlib.Path | str | None, optional
            _description_, by default None

        Raises
        ------
        ValueError
            _description_
        ValueError
            _description_
        ValueError
            _description_
        """
        # Maximum allowed distance difference between 2 points
        # to be considered the same crossing.
        self.allowed_distance = allowed_distance

        # Search radius for structures near a crossing.
        self.search_radius_structure = search_radius_structure

        # Search radius for open water near a crossing.
        self.search_radius_HWS_BZM = search_radius_HWS_BZM

        # Distance to move a point away from a boundary
        self.move_distance = move_distance

        # Radius to exclude a point itself from a buffer search.
        self.almost_equal = almost_equal

        # Distance to consider two lines as connected
        self.line_tol = line_tol

        # Distance to consider a line on a boundary
        self.almost_zero = almost_zero

        # List separator used in serialization/deserialization.
        self.list_sep = list_sep

        # Boolean to silence the progress bars.
        self.disable_progress = disable_progress

        # read all layers of geopackage
        base_path = settings.ribasim_nl_data_dir
        gpkg_path = os.path.join(base_path, gpkg_path)  # add the base path
        self.df_gpkg = {L: gpd.read_file(gpkg_path, layer=L) for L in fiona.listlayers(gpkg_path)}

        # Validate globalids
        for layername, df_layer in self.df_gpkg.items():
            if "globalid" not in df_layer.columns:
                continue
            if df_layer.globalid.str.contains(self.list_sep).any():
                raise ValueError(f"{layername}: contains the reserved character '{self.list_sep}'")

        # Enforce category for peilgebied
        if "peilgebied_cat" not in self.df_gpkg["peilgebied"].columns:
            self.df_gpkg["peilgebied"]["peilgebied_cat"] = 0
        self.peilgebied_cat_lookup = self.df_gpkg["peilgebied"].set_index("globalid").peilgebied_cat.copy()

        # Aggregate areas
        self.agg_areas_threshold = agg_areas_threshold
        self.agg_peilgebieden_layer = agg_peilgebieden_layer
        self.agg_peilgebieden_column = agg_peilgebieden_column
        if self.agg_peilgebieden_layer is not None:
            if self.agg_peilgebieden_column is None:
                raise ValueError("Aggregation layer is defined, but aggregation column is not defined")
            if not self.df_gpkg[self.agg_peilgebieden_layer][self.agg_peilgebieden_column].is_unique:
                raise ValueError(f"Aggregation column '{agg_peilgebieden_column}' has duplicate values")

        # KRW
        krw_path = os.path.join(base_path, krw_path)  # add the base path
        self.krw_path = krw_path
        self.krw_column_id = krw_column_id
        self.krw_column_name = krw_column_name
        self.krw_min_overlap = krw_min_overlap

        # Output path
        output_path = os.path.join(base_path, output_path)  # add the base path
        self.output_path = output_path

        # logger settings
        logger_name = f"{__name__.split('.')[0]}_{pathlib.Path(gpkg_path).stem}"
        self.log = logging.getLogger(logger_name)
        handlers = [logging.NullHandler()]
        if show_log:
            handlers.append(logging.StreamHandler())
        if logfile is not None:
            # Prepend base_path to logfile if it's not an absolute path
            logfile = os.path.join(base_path, logfile) if not os.path.isabs(logfile) else logfile
            handlers.append(logging.FileHandler(pathlib.Path(logfile), "w"))
        for handler in handlers:
            formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
        self.log.setLevel(logging.DEBUG)

    @staticmethod
    @pydantic.validate_call(config={"strict": True})
    def _polar(x1: float, x2: float, y1: float, y2: float) -> tuple[float, float]:
        """_summary_

        Parameters
        ----------
        x1 : float
            _description_
        x2 : float
            _description_
        y1 : float
            _description_
        y2 : float
            _description_

        Returns
        -------
        tuple[float, float]
            _description_
        """
        az = math.atan2(x1 - x2, y1 - y2)
        sina = math.sin(az)
        cosa = math.cos(az)
        return sina, cosa

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _extend_linestrings(
        self, df_linesingle: gpd.GeoDataFrame, df_peil_boundary: gpd.GeoDataFrame
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """_summary_

        Parameters
        ----------
        df_linesingle : gpd.GeoDataFrame
            _description_
        df_peil_boundary : gpd.GeoDataFrame
            _description_

        Returns
        -------
        tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]
            _description_

        Raises
        ------
        ValueError
            _description_
        """
        # Find endpoints
        df_endpoints = df_linesingle.copy()
        df_endpoints["geometry"] = df_endpoints.boundary
        df_endpoints = df_endpoints.explode(index_parts=True)

        # Find lines which end exactly on a border.
        extend_idx = {}
        for row_endpoint in tqdm.tqdm(
            df_endpoints.itertuples(),
            total=len(df_endpoints),
            desc="Find which endpoints need to be extended",
            disable=self.disable_progress,
        ):
            buffpoint = row_endpoint.geometry.buffer(self.almost_zero)
            idx_peilgebieden = df_peil_boundary.sindex.query(buffpoint, predicate="intersects")
            df_subset = df_peil_boundary.iloc[idx_peilgebieden, :].copy()
            if len(df_subset) > 0:
                k = (row_endpoint.geometry.x, row_endpoint.geometry.y)
                if k not in extend_idx:
                    extend_idx[k] = ([], df_subset)
                extend_idx[k][0].append(row_endpoint.Index)

        if len(extend_idx) > 0:
            # Move and modify those lines which end exactly on a border.
            for idxs, df_subset in tqdm.tqdm(
                extend_idx.values(),
                total=len(extend_idx),
                desc="Move groups of endpoints away from boundary",
                disable=self.disable_progress,
            ):
                # Move only the first item. Move the other points the exact same way.
                geom = np.array(df_linesingle.geometry.at[idxs[0][0:-1]].coords)
                if idxs[0][-1] == 0:
                    i1, i2 = 1, 0
                else:
                    i1, i2 = -2, -1
                x1, x2 = geom[i1, 0], geom[i2, 0]
                y1, y2 = geom[i1, 1], geom[i2, 1]
                sina, cosa = self._polar(x1, x2, y1, y2)
                xlen, ylen = x2 - x1, y2 - y1
                alen = math.sqrt(xlen**2 + ylen**2) + self.move_distance
                xnew = x1 - alen * sina
                ynew = y1 - alen * cosa

                # It is possible that the extended point is on the border again
                # (in case the angle of the line is the same as the angle of
                # the border). Change the angle slightly in that case.
                if df_subset.intersects(Point(xnew, ynew)).any():
                    alt_sina = sina + self.move_distance
                    if alt_sina <= (2 * np.pi):
                        alt_sina = sina - self.move_distance
                    xnew = x1 - alen * alt_sina
                    ynew = y1 - alen * cosa

                # Sanity check that the moved point is not on border again
                if df_subset.intersects(Point(xnew, ynew)).any():
                    raise ValueError("Move was unsucessful")

                for idx in idxs:
                    geom = np.array(df_linesingle.geometry.at[idx[0:-1]].coords)
                    if idx[-1] == 0:
                        geom[0, 0:2] = (xnew, ynew)
                    else:
                        geom[-1, 0:2] = (xnew, ynew)
                    df_linesingle.at[idx[0:-1], "geometry"] = LineString(geom)

        # Determine endpoints again, but with multipoints
        df_endpoints = df_linesingle.copy()
        df_endpoints["geometry"] = df_linesingle.boundary

        return df_linesingle, df_endpoints

    @staticmethod
    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _make_valid_2dgeom(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        gdf["geometry"] = gdf.make_valid()
        gdf["geometry"] = gdf.geometry.apply(shapely.force_2d)
        gdf = gdf[~gdf.is_empty].copy()

        return gdf

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _parse_peilgebieden(self) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """_summary_

        Returns
        -------
        tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]
            _description_

        Raises
        ------
        ValueError
            _description_
        """
        # Force valid peilgebieden with buffer=0 and remove empty geometries.
        df_peilgebieden = self.df_gpkg["peilgebied"].copy()
        df_peilgebieden = self._make_valid_2dgeom(df_peilgebieden)
        if pd.isna(df_peilgebieden.globalid).any():
            raise ValueError("One or more globalids of 'peilgebied' are null")

        # Add the KRW bodies
        df_peilgebieden = self.add_krw_to_peilgebieden(
            df_peilgebieden,
            self.krw_path,
            self.krw_column_id,
            self.krw_column_name,
            self.krw_min_overlap,
            self.list_sep,
        )

        # Determine the boundaries of the peilgebieden.
        df_peil_boundary = df_peilgebieden.copy()
        df_peil_boundary["geometry"] = df_peil_boundary.geometry.boundary

        return df_peilgebieden, df_peil_boundary

    @staticmethod
    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def add_krw_to_peilgebieden(
        df_peilgebieden: gpd.GeoDataFrame,
        krw_path: pathlib.Path | str | None,
        krw_column_id: str | None,
        krw_column_name: str | None,
        krw_min_overlap: float,
        krw_sep: str,
    ) -> gpd.GeoDataFrame:
        """_summary_

        Parameters
        ----------
        df_peilgebieden : gpd.GeoDataFrame
            _description_
        krw_path : pathlib.Path | str | None
            _description_
        krw_column_id : str | None
            _description_
        krw_column_name : str | None
            _description_
        krw_min_overlap : float
            _description_

        Returns
        -------
        gpd.GeoDataFrame
            _description_
        """
        # Add columns to peilgebieden dataframe
        pgb_krw_id = "owmident"
        pgb_krw_name = "owmnaam"
        dfp = df_peilgebieden.copy()
        if pgb_krw_id not in dfp.columns:
            dfp.insert(len(dfp.columns) - 1, pgb_krw_id, None)
        if pgb_krw_name not in dfp.columns:
            dfp.insert(len(dfp.columns) - 1, pgb_krw_name, None)

        # Reset values for krw columns in peilgebieden dataframe
        dfp[pgb_krw_id] = None
        dfp[pgb_krw_name] = None

        if not (krw_path is None or krw_column_id is None or krw_column_name is None):
            # Determine all krw polygons
            bbox = shapely.geometry.box(*dfp.geometry.total_bounds)
            df_krw = []
            for layername in fiona.listlayers(krw_path):
                df = gpd.read_file(krw_path, layer=layername)
                df = ParseCrossings._make_valid_2dgeom(df)
                df = df.explode(ignore_index=True)
                df.insert(0, "krwlayer", layername)

                # Limit to those geometries that intersect with the bounding
                # box of tthe peilgebieden dataframe.
                idxs = df.sindex.query(bbox, predicate="intersects")
                df = df.iloc[idxs, :].copy()

                # Buffer linestrings
                ls_geom = df.geom_type == "LineString"
                df.loc[ls_geom, "geometry"] = df.loc[ls_geom, "geometry"].buffer(0.5)

                # Only keep polygons
                df = df[df.geom_type == "Polygon"].copy()

                df_krw.append(df)
            df_krw = pd.concat(df_krw, ignore_index=True)

            # Assign krw ids
            for idx, row in dfp.iterrows():
                # Determine overlapping aggregate areas
                idxs = df_krw.sindex.query(row.geometry, predicate="intersects")
                matches = df_krw.geometry.iloc[idxs].intersection(row.geometry)
                matches = matches.area / df_krw.geometry.iloc[idxs].area
                matches = matches[matches >= krw_min_overlap]
                if len(matches) > 0:
                    df = df_krw.loc[matches.index, [krw_column_id, krw_column_name]].copy()
                    df = df.drop_duplicates(subset=[krw_column_id, krw_column_name])
                    dfp.at[idx, pgb_krw_id] = krw_sep.join(df[krw_column_id].tolist())
                    dfp.at[idx, pgb_krw_name] = krw_sep.join(df[krw_column_name].tolist())

        return dfp

    @pydantic.validate_call(config={"strict": True})
    def find_crossings_with_peilgebieden(
        self,
        layer: str,
        group_stacked: bool = True,
        agg_links: bool = True,
        filterlayer: str | None = None,
        reduce: bool = True,
        return_lines: bool = False,
        write_debug=None,
    ) -> gpd.GeoDataFrame:
        """_summary_

        Parameters
        ----------
        layer : str
            _description_
        group_stacked : bool, optional
            _description_, by default True
        agg_links : bool, optional
            _description_, by default True
        filterlayer : str | None, optional
            _description_, by default None
        reduce : bool, optional
            _description_, by default True
        return_lines : bool, optional
            _description_, by default False
        write_debug : _type_, optional
            _description_, by default None

        Returns
        -------
        gpd.GeoDataFrame
            _description_
        """
        # Check uniqueness of globalid
        for lyr in ["peilgebied", layer, filterlayer]:
            if lyr is None:
                continue
            if not self.df_gpkg[lyr].globalid.is_unique:
                raise ValueError(f"The globalid of '{lyr}' contains duplicates")

        dfc = {
            layer: [],
            "crossing_type": [],
            "peilgebieden": [],
            "peilgebied_from": [],
            "peilgebied_to": [],
            "geometry": [],
        }

        # Explode lines to single parts, ensure the lines are valid and remove
        # empty geometries.
        df_linesingle = self._get_layer_as_singleparts(layer, id_as_index=False)

        # Parse peilgebeiden
        df_peilgebieden, df_peil_boundary = self._parse_peilgebieden()

        # Determine endpoints
        df_linesingle, df_endpoints = self._extend_linestrings(df_linesingle, df_peil_boundary)

        # Find crossings of the lines with the peilgebieden.
        crossings = {}
        for row_line in tqdm.tqdm(
            df_linesingle.itertuples(),
            total=len(df_linesingle),
            desc=f"Find crossings for '{layer}'",
            disable=self.disable_progress,
        ):
            # Find the intersection of the current line with the peilgebieden.
            idx_peilgebieden = df_peil_boundary.sindex.query(row_line.geometry, predicate="intersects")

            # If no areas intersect, continue with the following line.
            if len(idx_peilgebieden) == 0:
                continue

            # Add peilgebieden which completely contain the current line.
            idx_peilgebieden2 = df_peilgebieden.sindex.query(row_line.geometry, predicate="within")
            idx_peilgebieden2 = list(set(idx_peilgebieden2).difference(set(idx_peilgebieden)))
            idx_within = df_peilgebieden.index[idx_peilgebieden2].to_numpy()

            # Find crossings with the current line.
            df_points = df_peil_boundary.iloc[idx_peilgebieden, :].copy()
            df_points["geometry"] = df_points.intersection(row_line.geometry)

            # Subset of polygons of the intersecting areas.
            df_subsetpeil_poly = df_peilgebieden.loc[np.hstack([df_points.index.to_numpy(), idx_within]), :].copy()

            # Explode to single parts and remove empty or non-point geometries.
            df_points = df_points.explode(ignore_index=False, index_parts=True)
            df_points = df_points[(~df_points.is_empty) & (df_points.geometry.geom_type == "Point")].copy()

            if len(df_subsetpeil_poly) == 0:
                # No areas intersect, continue with the following line.
                continue
            else:
                # At least 1 peilgebied match: Determine potential crossing
                # with these peilgebieden.
                crossings = self._add_potential_crossing(
                    crossings,
                    df_endpoints,
                    df_linesingle,
                    df_points,
                    df_subsetpeil_poly,
                )

        # Add the found crossings.
        for (pfrom, pto, _, _), (crossing, line_ids) in crossings.items():
            n_areas, str_areas = self._classify_from_to_peilgebieden(pfrom, pto)
            dfc[layer].append(line_ids)
            dfc["crossing_type"].append(n_areas)
            dfc["peilgebieden"].append(str_areas)
            dfc["peilgebied_from"].append(pfrom)
            dfc["peilgebied_to"].append(pto)
            dfc["geometry"].append(crossing)

        # Create dataframe of (potential) crossings
        dfc = gpd.GeoDataFrame(dfc, geometry="geometry", crs="epsg:28992")

        with_ends = False
        if layer == "duikersifonhevel":
            with_ends = True

        # Add waterlevels, structures and correct water flow based on these.
        dfc = self._add_waterlevels_to_crossings(dfc)
        dfc = self._find_structures_at_crossings(dfc, df_linesingle, df_endpoints, "stuw", with_ends)
        dfc = self._find_structures_at_crossings(dfc, df_linesingle, df_endpoints, "gemaal", with_ends)
        dfc = self._correct_water_flow(dfc)

        # If needed, check for multiple stacked/grouped crossings which can be
        # reduced to a single crossing or no crossing at all.
        line_groups = None
        if group_stacked:
            dfc, line_groups = self._find_stacked_crossings(
                layer, dfc, df_linesingle, df_endpoints, df_peilgebieden, reduce
            )
            dfc = self._correct_water_flow(dfc)

        if filterlayer is None:
            # return the found crossings.
            dfc = self._correct_structures(dfc, df_linesingle, df_endpoints, "stuw", with_ends, "in_use")
            dfc = self._correct_structures(dfc, df_linesingle, df_endpoints, "gemaal", with_ends, "in_use")
            dfc = self._correct_water_flow(dfc)
            dfc = self._add_double_links(dfc, "in_use")
            dfc = self._aggregate_identical_links(dfc, agg_links)
            dfc = self._aggregate_areas(dfc, agg_links)
            dfc = self._correct_structures(dfc, df_linesingle, df_endpoints, "stuw", with_ends, "agg_areas_in_use")
            dfc = self._correct_structures(dfc, df_linesingle, df_endpoints, "gemaal", with_ends, "agg_areas_in_use")
            dfc = self._correct_water_flow(dfc)
            dfc = self._add_double_links(dfc, "agg_areas_in_use")
            if return_lines:
                return dfc, line_groups
            else:
                return dfc
        else:
            # Filter the crossings with another layer with overlapping lines
            df_filter, dfs = self._filter_crossings_with_layer(dfc, df_peilgebieden, filterlayer, write_debug)
            dfs = self._correct_structures(dfs, df_linesingle, df_endpoints, "stuw", with_ends, "in_use")
            dfs = self._correct_structures(dfs, df_linesingle, df_endpoints, "gemaal", with_ends, "in_use")
            dfs = self._correct_water_flow(dfs)
            dfs = self._add_double_links(dfs, "in_use")
            dfs = self._aggregate_identical_links(dfs, agg_links)
            dfs = self._aggregate_areas(dfs, agg_links)
            dfs = self._correct_structures(dfs, df_linesingle, df_endpoints, "stuw", with_ends, "agg_areas_in_use")
            dfs = self._correct_structures(dfs, df_linesingle, df_endpoints, "gemaal", with_ends, "agg_areas_in_use")
            dfs = self._correct_water_flow(dfs)
            dfs = self._add_double_links(dfs, "agg_areas_in_use")
            return dfc, df_filter, dfs

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def write_crossings(
        self,
        df_hydro: gpd.GeoDataFrame,
        filterlayer: str | None,
        df_filter: gpd.GeoDataFrame | None,
        df_hydro_filter: gpd.GeoDataFrame | None,
    ) -> None:
        output_path = pathlib.Path(self.output_path)
        if not output_path.parent.exists():
            output_path.parent.mkdir(parents=True)

        # Write the input files (some with minor modifications)
        for layer, df in self.df_gpkg.items():
            df.to_file(output_path, layer=layer)

        # Write supplied output files
        df_hydro.to_file(output_path, layer="crossings_hydroobject")
        if df_filter is not None:
            df_filter.to_file(output_path, layer=f"crossings_{filterlayer}")
        if df_hydro_filter is not None:
            df_hydro_filter.to_file(output_path, layer="crossings_hydroobject_filtered")

        # Write the crossings to the GoodCloud
        cloud = CloudStorage()
        crossings_path_cloud_parent = output_path.parent  # Use the parent directory of the output path
        cloud.upload_content(dir_path=crossings_path_cloud_parent, overwrite=True)

    @pydantic.validate_call(config={"strict": True})
    def _classify_from_to_peilgebieden(self, pfrom: str | None, pto: str | None) -> tuple[str, str]:
        """_summary_

        Parameters
        ----------
        pfrom : str | None
            _description_
        pto : str | None
            _description_

        Returns
        -------
        tuple[int, str]
            _description_
        """
        crossing_areas = []
        area_types = []
        for p in [pfrom, pto]:
            if pd.isna(p):
                area_types.append(-1)
            else:
                area_types.append(self.peilgebied_cat_lookup.at[p])
                crossing_areas.append(p)

        str_areas = self.list_sep.join(sorted(crossing_areas))
        type_areas = "".join(map(str, sorted(area_types)))

        return type_areas, str_areas

    @pydantic.validate_call(config={"strict": True})
    def _get_layer_as_singleparts(self, layername: str, id_as_index: bool = False):
        """_summary_

        Parameters
        ----------
        layername : str
            _description_
        id_as_index : bool, optional
            _description_, by default False

        Returns
        -------
        _type_
            _description_

        Raises
        ------
        ValueError
            _description_
        ValueError
            _description_
        """
        df_single = self.df_gpkg[layername].copy()

        if pd.isna(df_single.globalid).any():
            raise ValueError(f"One or more globalids of '{layername}' are null")

        if df_single.globalid.duplicated().any():
            raise ValueError(f"One or more globalids of '{layername}' are not unique")

        if id_as_index:
            df_single = df_single.set_index("globalid", inplace=False)

        df_single = df_single.explode(ignore_index=False, index_parts=True)
        df_single = self._make_valid_2dgeom(df_single)

        change_idx, change_geom = [], []
        for row in tqdm.tqdm(
            df_single.itertuples(),
            desc=f"Snap geometries in '{layername}'",
            total=len(df_single),
            disable=self.disable_progress,
        ):
            ps = row.geometry.boundary.geoms
            if len(ps) != 2:
                continue
            p0, p1 = ps

            p0_changed, p1_changed = False, False
            idx0 = df_single.sindex.query(p0.buffer(self.line_tol), predicate="intersects")
            if len(idx0) > 0:
                dist0 = df_single.iloc[idx0].distance(p0)
                if (dist0 > self.almost_zero).any():
                    snap_lbl0 = dist0[dist0 > self.almost_zero].idxmin()
                    geom = df_single.geometry.at[snap_lbl0]
                    p0 = geom.interpolate(geom.project(p0))
                    p0_changed = True

            idx1 = df_single.sindex.query(p1.buffer(self.line_tol), predicate="intersects")
            if len(idx1) > 0:
                dist1 = df_single.iloc[idx1].distance(p1)
                if (dist1 > self.almost_zero).any():
                    snap_lbl1 = dist1[dist1 > self.almost_zero].idxmin()
                    geom = df_single.geometry.at[snap_lbl1]
                    p1 = geom.interpolate(geom.project(p1))
                    p1_changed = True

            if p0_changed or p1_changed:
                coords = list(row.geometry.coords)
                if p0_changed:
                    coords = list(p0.coords) + coords
                if p1_changed:
                    coords = coords + list(p1.coords)
                change_idx.append(row.Index)
                change_geom.append(LineString(coords))

        if len(change_idx) > 0:
            df_single.loc[change_idx, "geometry"] = change_geom

        return df_single

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _enforce_linestring(
        self,
        line: LineString | MultiLineString,
        point: Point,
        point_buffer: Polygon,
        df_peilgebied: gpd.GeoDataFrame,
    ):
        """_summary_

        Parameters
        ----------
        line : _type_
            _description_
        point_buffer : _type_
            _description_
        df_peilgebied : _type_
            _description_

        Returns
        -------
        _type_
            _description_
        """
        # Enforce linestring
        if line.geom_type == "MultiLineString":
            line = shapely.ops.linemerge(line)

        # If we still have a MultiLineString, use the first LineString that
        # overlaps with the point.
        if line.geom_type == "MultiLineString":
            temp = gpd.GeoSeries([line]).explode(index_parts=False, ignore_index=True)
            line = None
            for geom in temp.to_numpy():
                if geom.intersects(point_buffer) and df_peilgebied.intersects(geom).all():
                    line = geom
                    break
            if line is None:
                self.log.warning(
                    f"{point}: could not find a valid merged LineString, using the nearest part of the MultiLineString"
                )
                line = temp.geometry.at[temp.distance(point).idxmin()]

        return line

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _make_merged_line(
        self,
        crossing: Point,
        df_endpoints: gpd.GeoDataFrame,
        df_linesingle: gpd.GeoDataFrame,
        df_peilgebied: gpd.GeoDataFrame,
    ) -> tuple[LineString, str]:
        """_summary_

        Parameters
        ----------
        crossing : Point
            _description_
        df_endpoints : gpd.GeoDataFrame
            _description_
        df_linesingle : gpd.GeoDataFrame
            _description_
        df_peilgebied : gpd.GeoDataFrame
            _description_

        Returns
        -------
        tuple[LineString, str]
            _description_
        """
        ring_buffer = crossing.buffer(self.allowed_distance)
        point_buffer = crossing.buffer(self.almost_equal)

        _, idx_conn = self._find_closest_lines(crossing, self.almost_equal, df_linesingle, df_endpoints, n_recurse=1)
        if len(idx_conn) == 0:
            self.log.error(f"Crossing {crossing} is not on or near a line, ignoring...")
            return None, None

        df_conn = df_linesingle.iloc[idx_conn].copy()
        df_conn = df_conn[df_conn.intersects(ring_buffer)].copy()
        df_conn["geometry"] = df_conn.intersection(ring_buffer)
        df_conn = df_conn.explode(index_parts=False)
        df_conn = df_conn[df_conn.geom_type == "LineString"].copy()
        line = MultiLineString(df_conn.geometry.tolist())
        line_ids = self.list_sep.join(sorted(df_conn.globalid.unique()))

        # Try and merge the MultiLineString to a single LineString
        line = self._enforce_linestring(line, crossing, point_buffer, df_peilgebied)
        line = line.intersection(ring_buffer)

        return line, line_ids

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _make_merged_line_stacked(
        self,
        point_buffer1: Polygon,
        geom: Point,
        df_linesingle: gpd.GeoDataFrame,
        df_endpoints: gpd.GeoDataFrame,
        reduce: bool = True,
    ) -> LineString:
        """_summary_

        Parameters
        ----------
        point_buffer1 : Polygon
            _description_
        geom : Point
            _description_
        df_linesingle : gpd.GeoDataFrame
            _description_
        df_endpoints : gpd.GeoDataFrame
            _description_
        reduce : bool, optional
            _description_, by default True

        Returns
        -------
        LineString
            _description_
        """
        # Find the line(s) on which the current crossing lies and the directly
        # connected lines.
        nrec = 10
        if reduce:
            nrec = 1
        idx, idx_conn = self._find_closest_lines(geom, self.almost_equal, df_linesingle, df_endpoints, n_recurse=nrec)
        if len(idx_conn) == 0:
            self.log.error(f"Stacked crossing {geom} is not on or near a line, ignoring...")
            return None

        df_line = df_linesingle.iloc[idx, :].copy()
        df_line_conn = df_linesingle.iloc[idx_conn, :].copy()

        if reduce:
            # Reduce line to within buffered point
            df_line_conn = df_line_conn[df_line_conn.intersects(point_buffer1)].copy()
            df_line_conn = df_line_conn.intersection(point_buffer1)

        line_conn = df_line_conn[~df_line_conn.is_empty].unary_union
        if line_conn.geom_type == "MultiLineString":
            line_conn = shapely.ops.linemerge(line_conn)

        if line_conn.is_closed:
            check = df_line_conn.boundary.intersection(df_line.unary_union)
            check = check[~check.index.isin(df_line.index) & (check.geom_type == "MultiPoint") & (~check.is_empty)]
            if len(check) > 0:
                # Remove line(s) which might lead to a closed line.
                df_line_conn = df_line_conn[~df_line_conn.index.isin(check.index)]
                line_conn = df_line_conn[~df_line_conn.is_empty].unary_union
                if line_conn.geom_type == "MultiLineString":
                    line_conn = shapely.ops.linemerge(line_conn)

        return line_conn

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _add_potential_crossing(
        self,
        crossings: dict,
        df_endpoints: gpd.GeoDataFrame,
        df_linesingle: gpd.GeoDataFrame,
        crossing_points: gpd.GeoDataFrame,
        df_peilgebied: gpd.GeoDataFrame,
    ) -> dict[tuple[str | None, str | None, float, float], Point]:
        """_summary_

        Parameters
        ----------
        crossings : dict
            _description_
        df_endpoints : gpd.GeoDataFrame
            _description_
        df_linesingle : gpd.GeoDataFrame
            _description_
        crossing_points : gpd.GeoDataFrame
            _description_
        df_peilgebied : gpd.GeoDataFrame
            _description_

        Returns
        -------
        dict[tuple[str | None, str | None, float, float], Point]
            _description_
        """
        for crossing in crossing_points.geometry:
            # Find crossing line with potentially added connections
            merged_crossing_line, merged_ids = self._make_merged_line(
                crossing,
                df_endpoints,
                df_linesingle,
                df_peilgebied,
            )

            if merged_crossing_line is None:
                continue

            # Find intersection with buffered point, excluding the point itself.
            ring_buffer = crossing.buffer(self.allowed_distance)
            point_buffer = crossing.buffer(self.almost_equal)

            # Make two lines of the single LineString, separated by the point.
            # This can still result in a single LineString if the point happens
            # to coincide with an endpoint of the line. Enforce MultiLineString
            # in that case.
            line = merged_crossing_line.intersection(ring_buffer.difference(point_buffer))
            if line.geom_type == "LineString":
                line = MultiLineString([line])

            # Reference distance of crossing along line.
            crosspoint_dist = merged_crossing_line.project(crossing)

            # Find area names of the intersecting peilgebieden.
            buffer_from = []
            buffer_to = []
            if line.geom_type == "MultiLineString":
                for subline in line.geoms:
                    if subline.geom_type == "LineString":
                        idxmatches = df_peilgebied[df_peilgebied.intersects(subline)].index.tolist()
                        test_bound = df_peilgebied.geometry.loc[idxmatches].boundary.intersection(subline)
                        test_bound = test_bound[(~test_bound.is_empty) & (test_bound.geom_type == "LineString")]
                        test_bound = test_bound[test_bound.geom_equals(subline).to_numpy()].index.tolist()
                        if len(test_bound) > 0:
                            idxmatches = sorted(set(idxmatches).difference(test_bound))
                        matches = df_peilgebied.globalid.loc[idxmatches].tolist()
                        dist = merged_crossing_line.project(Point(subline.coords[-1]))
                        if dist < crosspoint_dist:
                            buffer_from += matches
                        else:
                            buffer_to += matches

            # Reduce to sorted lists which only contain unique names.
            buffer_from = sorted(set(buffer_from))
            buffer_to = sorted(set(buffer_to))
            if len(buffer_from) == 0:
                buffer_from.append(None)
            if len(buffer_to) == 0:
                buffer_to.append(None)

            # Add all potential combinations found from the buffer
            for bfrom in buffer_from:
                for bto in buffer_to:
                    # Skip if the proposed crossing has two identical names
                    if bfrom == bto:
                        continue
                    crossings[(bfrom, bto, crossing.x, crossing.y)] = (
                        crossing,
                        merged_ids,
                    )

        return crossings

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _find_stacked_crossings(
        self,
        crossing_layer: str,
        dfc: gpd.GeoDataFrame,
        df_linesingle: gpd.GeoDataFrame,
        df_endpoints: gpd.GeoDataFrame,
        df_peilgebieden: gpd.GeoDataFrame,
        reduce: bool = True,
    ) -> tuple[gpd.GeoDataFrame, dict]:
        """_summary_

        Parameters
        ----------
        crossing_layer : str
            _description_
        dfc : gpd.GeoDataFrame
            _description_
        df_linesingle : gpd.GeoDataFrame
            _description_
        df_endpoints : gpd.GeoDataFrame
            _description_
        df_peilgebieden : gpd.GeoDataFrame
            _description_
        reduce : bool, optional
            _description_, by default True

        Returns
        -------
        tuple[gpd.GeoDataFrame, dict]
            _description_

        Raises
        ------
        ValueError
            _description_
        TypeError
            _description_
        """
        dfs = dfc.copy()
        dfs.insert(len(dfs.columns) - 1, "match_group", 0)
        dfs.insert(len(dfs.columns) - 1, "match_stacked", 0)
        dfs.insert(len(dfs.columns) - 1, "match_composite", False)
        dfs.insert(len(dfs.columns) - 1, "match_group_unique", False)
        dfs.insert(len(dfs.columns) - 1, "in_use", False)

        dfo_all = df_peilgebieden.copy()
        dfo_all["geometry"] = dfo_all.buffer(-self.almost_equal).boundary
        dfo_all = dfo_all[~dfo_all.is_empty].copy()

        groupid = 1
        new_rows = []
        line_groups = {}
        for row in tqdm.tqdm(
            dfs.itertuples(),
            desc="  - Group geometrically stacked crossings",
            total=len(dfs),
            disable=self.disable_progress,
        ):
            line_id = row[dfs.columns.get_loc(crossing_layer) + 1]

            point_buffer1 = row.geometry.buffer(self.allowed_distance)
            group = dfs.iloc[dfs.sindex.query(point_buffer1, predicate="intersects"), :].copy()
            group = group[group.match_group == 0].copy()

            # Continue with the next iteration if we have no matches
            if len(group) == 0:
                continue

            # Try and make a merged line for the stacked points
            line_geom = self._make_merged_line_stacked(
                point_buffer1,
                row.geometry,
                df_linesingle,
                df_endpoints,
                reduce=reduce,
            )

            # @TODO line_geom can be None. This only happens if the crossing
            # could not be located on a line. Unexpected?
            if line_geom is not None:
                if not reduce:
                    # Weird bug that buffer almost_equal does not equal distance almost_equal
                    line_buf = line_geom.buffer(10 * self.almost_equal)
                    group = dfs.iloc[dfs.sindex.query(line_buf, predicate="intersects"), :].copy()
                    group = group[group.match_group == 0].copy()

                # Reduce points to those which are on the matched line(s)
                group = group[group.distance(line_geom) <= self.almost_equal].copy()

            if len(group) == 0:
                continue
            elif len(group) == 1:
                if line_geom is None:
                    # @TODO line_geom can be None. This only happens if the
                    # crossing could not be located on a line. Unexpected?
                    dfs.loc[group.index, "in_use"] = False
                else:
                    dfs.loc[group.index, "match_group"] = groupid
                    dfs.loc[group.index, "match_stacked"] = len(group)
                    dfs.loc[group.index, "match_group_unique"] = True
                    dfs.loc[group.index, "in_use"] = True
                    line_groups[groupid] = line_geom
                    groupid += 1
            else:
                if line_geom is None:
                    # @TODO line_geom can be None. This only happens if the
                    # crossing could not be located on a line. Unexpected?
                    dfs.loc[group.index, "in_use"] = False
                elif line_geom.geom_type == "MultiLineString":
                    for (pfrom, pto), subgroup in group.groupby(
                        ["peilgebied_from", "peilgebied_to"], dropna=False, sort=False
                    ):
                        if pd.isna(pfrom) or pd.isna(pto):
                            in_use = subgroup.index
                        else:
                            in_use = subgroup.index[[0]]
                        match_group_unique = True
                        if len(in_use) > 1:
                            match_group_unique = False
                        dfs.loc[subgroup.index, "match_group"] = groupid
                        dfs.loc[subgroup.index, "match_stacked"] = len(group)
                        dfs.loc[subgroup.index, "match_group_unique"] = match_group_unique
                        dfs.loc[in_use, "in_use"] = True
                        line_groups[groupid] = line_geom
                    groupid += 1
                elif line_geom.geom_type == "LineString":
                    dfo_idx = dfo_all.sindex.query(line_geom, predicate="intersects")
                    dfo = dfo_all.iloc[dfo_idx, :].copy()
                    # dfo = dfo[dfo.intersects(line_geom)].copy()
                    dfo["geometry"] = dfo.intersection(line_geom)
                    dfo = dfo.explode(ignore_index=True)
                    direction = []
                    keep_in_group = []
                    for r in group.itertuples():
                        if pd.isna(r.peilgebied_to) or pd.isna(r.peilgebied_from):
                            direction.append(np.nan)
                            keep_in_group.append(True)
                        else:
                            rdist = line_geom.project(r.geometry)
                            dfo["dist"] = [line_geom.project(d) - rdist for d in dfo.geometry]
                            if (
                                len(dfo[dfo.globalid == r.peilgebied_to]) == 0
                                or len(dfo[dfo.globalid == r.peilgebied_from]) == 0
                            ):
                                # Link case where a peilgebied just barely
                                # touches a line. Because of the negative
                                # buffer it will not touch anymore. Remove
                                # this crossing from the group.
                                direction.append(np.nan)
                                keep_in_group.append(False)
                            else:
                                lto = dfo[dfo.globalid == r.peilgebied_to].dist.abs().idxmin()
                                lfrom = dfo[dfo.globalid == r.peilgebied_from].dist.abs().idxmin()
                                direction.append(dfo.dist.at[lto] - dfo.dist.at[lfrom])
                                keep_in_group.append(True)

                    if not np.any(keep_in_group):
                        # If the group is empty, return early
                        dfs.loc[group.index, "match_group"] = groupid
                        dfs.loc[group.index, "match_stacked"] = len(group)
                        dfs.loc[group.index, "match_group_unique"] = False
                        dfs.loc[group.index, "in_use"] = False
                        line_groups[groupid] = line_geom
                        groupid += 1
                        continue

                    group["direction"] = np.sign(direction)
                    group = group[keep_in_group].copy()

                    uniq_dir = group.direction.unique()
                    uniq_dir_nonnan = uniq_dir[~np.isnan(uniq_dir)]
                    if len(uniq_dir_nonnan) > 1:
                        if (
                            len(group) == 2
                            and (uniq_dir == -1).any()
                            and (uniq_dir == 1).any()
                            and not np.isnan(uniq_dir).any()
                            and (len(group.peilgebied_from.unique()) == 1 or len(group.peilgebied_to.unique()) == 1)
                        ):
                            # Set status of group
                            dfs.loc[group.index, "match_group"] = groupid
                            dfs.loc[group.index, "match_stacked"] = len(group)
                            dfs.loc[group.index, "match_group_unique"] = True
                            if len(group.peilgebied_from.unique()) == 1 and len(group.peilgebied_to.unique()) == 1:
                                pass
                            else:
                                if len(group.peilgebied_from.unique()) == 1:
                                    pfrom, pto = group.peilgebied_to
                                    peilfrom, peilto = group.streefpeil_to
                                else:
                                    pfrom, pto = group.peilgebied_from
                                    peilfrom, peilto = group.streefpeil_from
                                # Special case, add a composite point ignoring the middle area
                                (
                                    type_areas,
                                    str_areas,
                                ) = self._classify_from_to_peilgebieden(pfrom, pto)
                                new_row = dfs.loc[group.index[0], :].copy()
                                new_row["peilgebieden"] = str_areas
                                new_row["crossing_type"] = type_areas
                                new_row["peilgebied_from"] = pfrom
                                new_row["peilgebied_to"] = pto
                                new_row["streefpeil_from"] = peilfrom
                                new_row["streefpeil_to"] = peilto
                                new_row["match_group"] = groupid
                                new_row["match_stacked"] = len(group)
                                new_row["match_composite"] = True
                                new_row["match_group_unique"] = True
                                new_row["flip"] = None
                                new_row["in_use"] = True
                                new_rows.append(new_row)
                        else:
                            # We cannot reduce this group, keep all crossings
                            dfs.loc[group.index, "match_group"] = groupid
                            dfs.loc[group.index, "match_stacked"] = len(group)
                            dfs.loc[group.index, "match_group_unique"] = True
                            dfs.loc[group.index, "in_use"] = True
                        line_groups[groupid] = line_geom
                        groupid += 1
                    else:
                        if len(uniq_dir_nonnan) == 1 and uniq_dir_nonnan[0] == -1:
                            # Reverse linestring
                            line_geom = line_geom.reverse()
                        group["dist_along"] = np.nan
                        for subrow in group.itertuples():
                            group.at[subrow.Index, "dist_along"] = line_geom.project(subrow.geometry)
                            if subrow.crossing_type.startswith("-1"):
                                # Special case: correct the flow order of crossing_type=-1.
                                # Assume that drawing order is determined by the line_geom.
                                # This is why the line_geo was reversed earlier.
                                rdist = line_geom.project(subrow.geometry)
                                dfo["dist"] = [line_geom.project(d) - rdist for d in dfo.geometry]
                                if pd.isna(subrow.peilgebied_from):
                                    lto = dfo[dfo.globalid == subrow.peilgebied_to].dist.abs().idxmin()
                                    if dfo.dist.at[lto] < 0:
                                        group.at[subrow.Index, "peilgebied_from"] = subrow.peilgebied_to
                                        group.at[subrow.Index, "peilgebied_to"] = None
                                        group.at[subrow.Index, "streefpeil_from"] = subrow.streefpeil_to
                                        group.at[subrow.Index, "streefpeil_to"] = None
                                elif pd.isna(subrow.peilgebied_to):
                                    lfrom = dfo[dfo.globalid == subrow.peilgebied_from].dist.abs().idxmin()
                                    if dfo.dist.at[lfrom] > 0:
                                        group.at[subrow.Index, "peilgebied_from"] = None
                                        group.at[subrow.Index, "peilgebied_to"] = subrow.peilgebied_from
                                        group.at[subrow.Index, "streefpeil_from"] = None
                                        group.at[subrow.Index, "streefpeil_to"] = subrow.streefpeil_from
                                else:
                                    raise ValueError(f"{subrow.peilgebied_from=}, {subrow.peilgebied_to}")

                        group = group.sort_values("dist_along")
                        pfrom = group.peilgebied_from[group.dist_along == group.dist_along.min()].unique()
                        pto = group.peilgebied_to[group.dist_along == group.dist_along.max()].unique()
                        match_group_unique = True
                        if len(pfrom) != 1 or len(pto) != 1:
                            self.log.warning(f"No unique point for point group '{groupid}' on line '{line_id}'")
                            match_group_unique = False
                        pfrom = pfrom[0]
                        pto = pto[0]

                        if pfrom != pto:
                            if pd.isna(pfrom):
                                check_from = pd.isna(group.peilgebied_from)
                            else:
                                check_from = group.peilgebied_from == pfrom
                            if pd.isna(pto):
                                check_to = pd.isna(group.peilgebied_to)
                            else:
                                check_to = group.peilgebied_to == pto
                            entry_exists = check_from & check_to
                            if entry_exists.any():
                                # The entry exists, toggle it to 'in_use'.
                                dfs.at[group[entry_exists].index[0], "in_use"] = True
                            else:
                                # The entry does not exist yet, create a new composite entry
                                (
                                    type_areas,
                                    str_areas,
                                ) = self._classify_from_to_peilgebieden(pfrom, pto)
                                new_row = dfs.loc[group.index[0], :].copy()
                                new_row["peilgebieden"] = str_areas
                                new_row["crossing_type"] = type_areas
                                new_row["peilgebied_from"] = pfrom
                                new_row["peilgebied_to"] = pto
                                new_row["streefpeil_from"] = group.streefpeil_from[check_from].iat[0]
                                new_row["streefpeil_to"] = group.streefpeil_to[check_to].iat[0]
                                new_row["match_group"] = groupid
                                new_row["match_stacked"] = len(group)
                                new_row["match_composite"] = True
                                new_row["match_group_unique"] = match_group_unique
                                new_row["flip"] = None
                                new_row["in_use"] = True
                                new_rows.append(new_row)
                        dfs.loc[group.index, "match_group"] = groupid
                        dfs.loc[group.index, "match_stacked"] = len(group)
                        dfs.loc[group.index, "match_group_unique"] = match_group_unique
                        line_groups[groupid] = line_geom
                        groupid += 1
                else:
                    raise TypeError(f"{line_geom.geom_type=}")

        if len(new_rows) > 0:
            new_rows = gpd.GeoDataFrame(new_rows, geometry="geometry", crs=dfs.crs)
            dfs = pd.concat([dfs, new_rows], ignore_index=False)
            dfs = dfs.sort_index().reset_index(drop=True)

        return dfs, line_groups

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _find_closest_HWS_BZM(self, crossing: Point, df_HWS_BZM: gpd.GeoDataFrame) -> str | None:
        """_summary_

        Parameters
        ----------
        crossing : Point
            _description_
        df_HWS_BZM : gpd.GeoDataFrame
            _description_

        Returns
        -------
        str | None
            _description_
        """
        lbl_HWS_BZM = None

        buffered_crossing = crossing.buffer(self.search_radius_HWS_BZM)
        idx = df_HWS_BZM.sindex.query(buffered_crossing, predicate="intersects")
        df_subset = df_HWS_BZM.iloc[idx, :].copy()
        if len(df_subset) > 0:
            lbl_HWS_BZM = df_subset.distance(crossing).idxmin()

        return lbl_HWS_BZM

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _add_waterlevels_to_crossings(self, dfc: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """_summary_

        Parameters
        ----------
        dfc : gpd.GeoDataFrame
            _description_

        Returns
        -------
        gpd.GeoDataFrame
            _description_
        """
        # Reference to water levels of streefpeil table.
        strfpl = self.df_gpkg["streefpeil"].copy().set_index("globalid", inplace=False).waterhoogte

        # Reference to HWS_BZM polygons
        df_HWS_BZM = self.df_gpkg["peilgebied"].copy()
        df_HWS_BZM = df_HWS_BZM[df_HWS_BZM.peilgebied_cat != 0].copy()
        df_HWS_BZM = df_HWS_BZM.set_index("globalid")

        # Copy crossings and valid crossings.
        dfs = dfc.copy()

        # Add streefpeil information to valid crossings
        if "streefpeil_from" not in dfs.columns:
            dfs.insert(len(dfs.columns) - 1, "streefpeil_from", np.nan)
        if "streefpeil_to" not in dfs.columns:
            dfs.insert(len(dfs.columns) - 1, "streefpeil_to", np.nan)
        for row in tqdm.tqdm(
            dfs.itertuples(),
            total=len(dfs),
            desc="  - Add water levels to crossings",
            disable=self.disable_progress,
        ):
            if pd.isna(row.peilgebied_from):
                lbl_HWS_BZM = self._find_closest_HWS_BZM(row.geometry, df_HWS_BZM)
                if lbl_HWS_BZM is not None:
                    type_areas, str_areas = self._classify_from_to_peilgebieden(lbl_HWS_BZM, row.peilgebied_to)
                    if lbl_HWS_BZM in strfpl:
                        dfs.at[row.Index, "streefpeil_from"] = strfpl.at[lbl_HWS_BZM]
                    dfs.at[row.Index, "crossing_type"] = type_areas
                    dfs.at[row.Index, "peilgebieden"] = str_areas
            elif row.peilgebied_from in strfpl:
                dfs.at[row.Index, "streefpeil_from"] = strfpl.at[row.peilgebied_from]

            if pd.isna(row.peilgebied_to):
                lbl_HWS_BZM = self._find_closest_HWS_BZM(row.geometry, df_HWS_BZM)
                if lbl_HWS_BZM is not None:
                    type_areas, str_areas = self._classify_from_to_peilgebieden(row.peilgebied_from, lbl_HWS_BZM)
                    if lbl_HWS_BZM in strfpl:
                        dfs.at[row.Index, "streefpeil_from"] = strfpl.at[lbl_HWS_BZM]
                    dfs.at[row.Index, "crossing_type"] = type_areas
                    dfs.at[row.Index, "peilgebieden"] = str_areas
            elif row.peilgebied_to in strfpl:
                dfs.at[row.Index, "streefpeil_to"] = strfpl.at[row.peilgebied_to]

        return dfs

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _make_structure_string(self, structure_list: pd.Series) -> str:
        """_summary_

        Parameters
        ----------
        structure_list : pd.Series
            _description_

        Returns
        -------
        str
            _description_
        """
        # Join into a single string first. Cells may or may not contain
        # multiple ids in string format. This pandas function also drops NA
        # values.
        struct_str = structure_list.str.cat(sep=self.list_sep, na_rep=None)

        # Split again by separator and find unique values by using set(). This
        # also sorts the list.
        struct_set = set(struct_str.split(self.list_sep))

        # Join into a single string again.
        struct_str = self.list_sep.join(struct_set)

        return struct_str

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _filter_crossings_with_layer(
        self,
        dfc: gpd.GeoDataFrame,
        df_peilgebieden: gpd.GeoDataFrame,
        filterlayer: str,
        write_debug=None,
    ):
        """_summary_

        Parameters
        ----------
        dfc : gpd.GeoDataFrame
            _description_
        df_peilgebieden : gpd.GeoDataFrame
            _description_
        filterlayer : str
            _description_
        write_debug : _type_, optional
            _description_, by default None

        Returns
        -------
        _type_
            _description_

        Raises
        ------
        ValueError
            _description_
        TypeError
            _description_
        """
        if write_debug is not None:
            dfc.to_file(write_debug, layer="crossings")
            df_peilgebieden.to_file(write_debug, layer="peilgebieden")

        filter_col = "in_use"
        dfs = dfc.copy()
        dfs_valid = dfs[dfs[filter_col]].copy()

        # Determine crossings for the filter layer
        df_filter, line_groups = self.find_crossings_with_peilgebieden(
            filterlayer,
            filterlayer=None,
            group_stacked=True,
            reduce=False,
            return_lines=True,
        )

        # Find out which crossings can be filtered out.
        add_crossings = []
        for groupid, group in tqdm.tqdm(
            df_filter.groupby("match_group", sort=False),
            desc=f"Filter crossings along '{filterlayer}'",
            disable=self.disable_progress,
        ):
            if groupid == 0:
                raise ValueError(f"Found crossings which have not been grouped {group=}")

            # Merged line geometry for this group
            line_geom = line_groups[groupid]

            # Relevant crossings for this group
            replace_crossing_candidates = group[group[filter_col]].copy()

            replace_crossing_vec = []
            if len(replace_crossing_candidates) == 0:
                pass
            elif len(replace_crossing_candidates) == 1:
                replace_crossing_vec.append(replace_crossing_candidates.iloc[0,].copy())
            else:
                if line_geom.geom_type == "LineString" or line_geom.geom_type == "MultiLineString":
                    replace_stuw = None
                    if "stuw" in dfs.columns:
                        replace_stuw = self._make_structure_string(replace_crossing_candidates.stuw)
                    replace_gemaal = None
                    if "gemaal" in dfs.columns:
                        replace_gemaal = self._make_structure_string(replace_crossing_candidates.gemaal)

                    for x0, x1 in itertools.combinations(self._find_line_ends(line_geom), 2):
                        subbound = MultiPoint([x0, x1])
                        subpg = df_peilgebieden.sindex.query(subbound, predicate="intersects")
                        subpg = df_peilgebieden.iloc[subpg, :].copy()

                        p0 = subpg.index[subpg.intersects(x0)].tolist()
                        if len(p0) == 0:
                            p0 = None
                        else:
                            if len(p0) > 1:
                                self.log.warning(
                                    f"{filterlayer} line endpoint lies in multiple overlapping peilgebieden ({subpg.globalid.loc[p0].tolist()}), using only the first"
                                )
                            p0 = subpg.globalid.at[p0[0]]

                        p1 = subpg.index[subpg.intersects(x1)].tolist()
                        if len(p1) == 0:
                            p1 = None
                        else:
                            if len(p1) > 1:
                                self.log.warning(
                                    f"{filterlayer} line endpoint lies in multiple overlapping peilgebieden ({subpg.globalid.loc[p1].tolist()}), using only the first"
                                )
                            p1 = subpg.globalid.at[p1[0]]

                        if p0 == p1:
                            pass
                        else:
                            type_areas, str_areas = self._classify_from_to_peilgebieden(p0, p1)
                            matches = replace_crossing_candidates[replace_crossing_candidates.peilgebieden == str_areas]
                            if len(matches) > 0:
                                replace_crossing = replace_crossing_candidates.loc[matches.index[0], :].copy()
                            else:
                                replace_crossing = replace_crossing_candidates.iloc[0, :].copy()
                                replace_crossing["peilgebieden"] = str_areas
                                replace_crossing["crossing_type"] = type_areas
                                replace_crossing["peilgebied_from"] = p0
                                replace_crossing["peilgebied_to"] = p1
                                replace_crossing["streefpeil_from"] = None
                                replace_crossing["streefpeil_to"] = None
                                replace_crossing["match_composite"] = True
                                replace_crossing["match_group_unique"] = True
                                replace_crossing["in_use"] = True

                            replace_crossing["flip"] = None
                            if "gemaal" in dfs.columns:
                                replace_crossing["gemaal"] = replace_gemaal
                                # replace_gemaal = None
                            if "stuw" in dfs.columns:
                                replace_crossing["stuw"] = replace_stuw
                                # replace_stuw = None
                            replace_crossing_vec.append(replace_crossing)
                else:
                    raise TypeError(f"{line_geom=}")

            # Find the matching crossings in the to-be-filtered layer.
            matching_crossings = []
            for row in group.itertuples():
                dist0 = dfs_valid.distance(row.geometry)
                dist1 = dfs_valid.distance(line_geom)
                matching_crossings.append(
                    dfs_valid[
                        (dist0 < self.allowed_distance)
                        & (dist1 < self.almost_equal)
                        & (dfs_valid.peilgebieden == row.peilgebieden)
                    ].copy()
                )
            matching_crossings = pd.concat(matching_crossings, ignore_index=False)

            # Disable the matching crossings
            dfs.loc[matching_crossings.index, "in_use"] = False

            # Check and see if the replacement crossing already exists in the
            # matching crossings
            for replace_crossing in replace_crossing_vec:
                pfrom = replace_crossing.peilgebied_from
                pto = replace_crossing.peilgebied_to
                check_from = matching_crossings.peilgebied_from == pfrom
                check_to = matching_crossings.peilgebied_to == pto
                check_exists = check_from & check_to
                if "stuw" in dfs.columns:
                    check_stuw = matching_crossings.stuw == replace_crossing.stuw
                    check_exists = check_exists & check_stuw
                if "gemaal" in dfs.columns:
                    check_gemaal = matching_crossings.gemaal == replace_crossing.gemaal
                    check_exists = check_exists & check_gemaal
                c_exists = matching_crossings.index[check_exists]
                if len(c_exists) > 0:
                    dfs.at[c_exists[0], "in_use"] = True
                elif len(matching_crossings) > 0:
                    new_row = matching_crossings.iloc[0, :].copy()
                    type_areas, str_areas = self._classify_from_to_peilgebieden(pfrom, pto)
                    new_row["crossing_type"] = type_areas
                    new_row["peilgebieden"] = str_areas
                    new_row["peilgebied_from"] = pfrom
                    new_row["peilgebied_to"] = pto
                    new_row["streefpeil_from"] = replace_crossing.streefpeil_from
                    new_row["streefpeil_to"] = replace_crossing.streefpeil_to
                    new_row["match_composite"] = True
                    new_row["match_group_unique"] = True
                    new_row["in_use"] = True
                    new_row["flip"] = None
                    if "gemaal" in dfs.columns:
                        new_row["gemaal"] = replace_crossing.gemaal
                    if "stuw" in dfs.columns:
                        new_row["stuw"] = replace_crossing.stuw
                    new_row["geometry"] = replace_crossing.geometry
                    add_crossings.append(new_row)

        if len(add_crossings) > 0:
            add_crossings = gpd.GeoDataFrame(add_crossings, crs=dfs.crs)
            dfs = pd.concat([dfs, add_crossings], ignore_index=False, sort=False)

        dfs = dfs.drop_duplicates(keep="first")
        dfs = dfs.sort_index().reset_index(drop=True)

        return df_filter, dfs

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _find_structures_at_crossings(
        self,
        dfc: gpd.GeoDataFrame,
        df_linesingle: gpd.GeoDataFrame,
        df_endpoints: gpd.GeoDataFrame,
        structurelayer: str,
        with_ends: bool,
    ) -> gpd.GeoDataFrame:
        """_summary_

        Parameters
        ----------
        dfc : gpd.GeoDataFrame
            _description_
        df_linesingle : gpd.GeoDataFrame
            _description_
        df_endpoints : gpd.GeoDataFrame
            _description_
        structurelayer : str
            _description_
        with_ends : bool
            _description_

        Returns
        -------
        gpd.GeoDataFrame
            _description_
        """
        # Reference to the structure GeoDataFrame.
        df_structures = self.df_gpkg[structurelayer].copy()

        # Copy crossings and valid crossings.
        dfs = dfc.copy()
        if structurelayer not in dfs.columns:
            dfs.insert(len(dfs.columns) - 1, structurelayer, None)
        dfs[structurelayer] = None

        # Only look at crossing that are going to be used
        filter_col = "in_use"
        if filter_col in dfs.columns:
            df_filter = dfs[dfs[filter_col]].copy()
        else:
            df_filter = dfs.copy()

        # Add structures to crossings.
        df_sub_structures = df_structures.copy()
        df_struct = df_structures.set_index("globalid")
        while len(df_sub_structures) > 0:
            orphaned_structures = []
            for structure in tqdm.tqdm(
                df_sub_structures.itertuples(),
                total=len(df_sub_structures),
                desc=f"  - Add structures of '{structurelayer}' to crossings",
                disable=self.disable_progress,
            ):
                dfs, orphaned_structures = self._assign_structure(
                    dfs,
                    orphaned_structures,
                    df_filter,
                    df_linesingle,
                    df_endpoints,
                    df_struct,
                    structure.geometry,
                    structure.globalid,
                    structurelayer,
                    with_ends,
                )
            df_sub_structures = pd.DataFrame(orphaned_structures, columns=["globalid", "geometry"])

        return dfs

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _find_closest_lines(
        self,
        poi: Point,
        max_dist: float,
        df_linesingle: gpd.GeoDataFrame,
        df_endpoints: gpd.GeoDataFrame,
        n_recurse=1,
        filter_type: str | None = None,
    ) -> tuple[npt.NDArray, npt.NDArray]:
        """_summary_

        Parameters
        ----------
        poi : Point
            _description_
        max_dist : float
            _description_
        df_linesingle : gpd.GeoDataFrame
            _description_
        df_endpoints : gpd.GeoDataFrame
            _description_
        n_recurse : int, optional
            _description_, by default 1
        filter_type : str | None, optional
            _description_, by default None

        Returns
        -------
        tuple[npt.NDArray, npt.NDArray]
            _description_

        Raises
        ------
        ValueError
            _description_
        """
        if filter_type is None:
            # Find all line objects near to the PoI
            idx = df_linesingle.sindex.query(poi.buffer(max_dist), predicate="intersects")
        elif filter_type == "nearest":
            # Find the line object nearest to the PoI
            idx = df_linesingle.sindex.nearest(poi, max_distance=max_dist)[1, :]
        else:
            raise ValueError(f"Unknown filter_type argument ({filter_type=})")

        # Return early if we did not find any lines
        if len(idx) == 0:
            return idx, idx

        # Recursively search for connected lines
        idx_conn = idx.copy()
        for _ in range(n_recurse):
            # Check for line ends ending on the current line
            line_buffer = df_linesingle.geometry.iloc[idx_conn].buffer(self.almost_zero)
            idx_conn_new1 = df_endpoints.sindex.query(line_buffer, predicate="intersects")[1, :]

            # Check for the current line ends ending on other lines
            point_buffer = df_endpoints.geometry.iloc[idx_conn].buffer(self.almost_zero)
            idx_conn_new2 = df_linesingle.sindex.query(point_buffer, predicate="intersects")[1, :]

            # Combine potential new additions
            idx_conn_new = np.hstack([idx_conn_new1, idx_conn_new2])

            # break early if no new connections were found
            if set(idx_conn) == set(idx_conn_new):
                break

            # save (unique) new connections
            idx_conn = np.unique(idx_conn_new)

        return idx, idx_conn

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _find_line_ends(self, geom: MultiLineString | LineString) -> list:
        """_summary_

        Parameters
        ----------
        geom : MultiLineString | LineString
            _description_

        Returns
        -------
        list
            _description_

        Raises
        ------
        TypeError
            _description_
        """
        if geom.geom_type == "LineString":
            line_ends = list(geom.boundary.geoms)
        elif geom.geom_type == "MultiLineString":
            df_line_geom = gpd.GeoSeries([geom]).explode(index_parts=False, ignore_index=True)
            line_ends = df_line_geom.copy()
            line_ends.update(line_ends.boundary)
            line_ends = line_ends.explode(index_parts=True)
            keep_ends = []
            for i, (idx, geom) in enumerate(line_ends.items()):
                edx = line_ends.sindex.query(geom.buffer(self.almost_zero), predicate="intersects")
                edx = edx[edx != i]
                if len(edx) == 0:
                    min_dist = df_line_geom.iloc[df_line_geom.index != idx[0]].distance(geom).min()
                    if min_dist > self.almost_zero:
                        keep_ends.append(i)
            line_ends = line_ends.iloc[keep_ends].tolist()
        else:
            raise TypeError(f"{geom.geom_type=}")

        return line_ends

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _assign_structure(
        self,
        dfs: gpd.GeoDataFrame,
        orphaned_structures: list,
        df_filter: gpd.GeoDataFrame,
        df_linesingle: gpd.GeoDataFrame,
        df_endpoints: gpd.GeoDataFrame,
        df_structures: gpd.GeoDataFrame,
        structure_geom: Point,
        structure_id: str,
        structurelayer: str,
        with_ends: bool,
    ) -> tuple[gpd.GeoDataFrame, list]:
        """_summary_

        Parameters
        ----------
        dfs : gpd.GeoDataFrame
            _description_
        orphaned_structures : list
            _description_
        df_filter : gpd.GeoDataFrame
            _description_
        df_linesingle : gpd.GeoDataFrame
            _description_
        df_endpoints : gpd.GeoDataFrame
            _description_
        df_structures : gpd.GeoDataFrame
            _description_
        structure_geom : Point
            _description_
        structure_id : str
            _description_
        structurelayer : str
            _description_
        with_ends : bool
            _description_

        Returns
        -------
        tuple[gpd.GeoDataFrame, list]
            _description_
        """
        # Find the line objects nearest to the structure
        _, idxs = self._find_closest_lines(
            structure_geom,
            self.search_radius_structure,
            df_linesingle,
            df_endpoints,
            n_recurse=10,
            filter_type="nearest",
        )
        if len(idxs) == 0:
            self.log.warning(f"{structurelayer} '{structure_id}' has no line object nearby")
            return dfs, orphaned_structures

        # Find closest points on nearest line(s)
        df_line_geom = df_linesingle.iloc[idxs, :].reset_index(drop=True)
        buff_line = df_line_geom.buffer(self.almost_equal)
        idx = df_filter.sindex.query(buff_line, predicate="intersects")
        df_close = df_filter.iloc[np.unique(idx[1, :]), :].copy()

        # Optionally include endpoints from the line object
        if with_ends and len(df_close) > 0:
            line_ends = self._find_line_ends(MultiLineString(df_line_geom.geometry.tolist()))
            df_close["geometry"] = df_close.apply(lambda r: MultiPoint([r.geometry] + line_ends), axis=1)

        # Find the closest point
        idx = df_close.sindex.nearest(structure_geom, max_distance=self.search_radius_structure)[1, :]
        if len(idx) == 0:
            self.log.warning(f"{structurelayer} '{structure_id}' has no crossings nearby")
            return dfs, orphaned_structures
        lbl = df_close.index[idx[0]]

        # Find potential stacked points on closest point
        buff_point = df_filter.geometry.at[lbl].buffer(self.almost_equal)
        idx_close = df_filter.sindex.query(buff_point, predicate="intersects")
        df_stacked = df_filter.iloc[idx_close, :].copy()

        # Add the structure to the crossing(s) which are closest.
        cur_structids = dfs.at[lbl, structurelayer]
        if cur_structids == "":
            cur_structids = None
        if pd.isna(cur_structids):
            dfs.loc[df_stacked.index, structurelayer] = structure_id
        else:
            # Only add the new structure to the crossing(s) if the new
            # structure is closer by than the old structure.
            crossing = dfs.geometry.at[lbl]
            old_struct_ids = dfs.at[lbl, structurelayer].split(self.list_sep)
            old_struct_ids = [sid for sid in old_struct_ids if sid != ""]
            old_structs = df_structures.geometry.loc[old_struct_ids]
            if old_structs.distance(crossing).min() > structure_geom.distance(crossing):
                self.log.info(f"Replacing {structurelayer} at {crossing} with '{structure_id}'")
                for old_struct_id, old_struct in zip(old_struct_ids, old_structs):
                    orphaned_structures.append((old_struct_id, old_struct))
                dfs.loc[df_stacked.index, structurelayer] = structure_id

        return dfs, orphaned_structures

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _correct_structures(
        self,
        dfc: gpd.GeoDataFrame,
        df_linesingle: gpd.GeoDataFrame,
        df_endpoints: gpd.GeoDataFrame,
        structurelayer: str,
        with_ends: bool,
        filter_col: str,
    ) -> gpd.GeoDataFrame:
        """_summary_

        Parameters
        ----------
        dfc : gpd.GeoDataFrame
            _description_
        df_linesingle : gpd.GeoDataFrame
            _description_
        df_endpoints : gpd.GeoDataFrame
            _description_
        structurelayer : str
            _description_
        with_ends : bool
            _description_
        filter_col : str
            _description_

        Returns
        -------
        gpd.GeoDataFrame
            _description_
        """
        # Reference to the structure GeoDataFrame.
        df_structures = self.df_gpkg[structurelayer].copy()
        df_structures = df_structures.set_index("globalid")

        # Copy crossings and valid crossings.
        dfs = dfc.copy()
        df_filter = dfs[dfs[filter_col]].copy()

        # Find previously assigned structures that are now unassigned.
        orphaned_structures = []
        for structure_id, group in dfs.groupby(structurelayer, sort=False):
            if not group[filter_col].any():
                for sid in structure_id.split(self.list_sep):
                    orphaned_structures.append((sid, df_structures.geometry.at[sid]))

        df_sub_structures = pd.DataFrame(orphaned_structures, columns=["globalid", "geometry"])
        df_sub_structures = df_sub_structures.drop_duplicates(subset="globalid")
        while len(df_sub_structures) > 0:
            orphaned_structures = []
            for structure in tqdm.tqdm(
                df_sub_structures.itertuples(),
                total=len(df_sub_structures),
                desc=f"  - Add structures of '{structurelayer}' to crossings",
                disable=self.disable_progress,
            ):
                dfs, orphaned_structures = self._assign_structure(
                    dfs,
                    orphaned_structures,
                    df_filter,
                    df_linesingle,
                    df_endpoints,
                    df_structures,
                    structure.geometry,
                    structure.globalid,
                    structurelayer,
                    with_ends,
                )
            df_sub_structures = pd.DataFrame(orphaned_structures, columns=["globalid", "geometry"])

        # Fix empty strings
        dfs.loc[dfs[structurelayer] == "", structurelayer] = None

        return dfs

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _correct_water_flow(self, dfc: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """_summary_

        Parameters
        ----------
        dfc : gpd.GeoDataFrame
            _description_

        Returns
        -------
        gpd.GeoDataFrame
            _description_
        """
        dfs = dfc.copy()

        # Free flowing water flows from high to low
        if "flip" not in dfs.columns:
            dfs.insert(len(dfs.columns) - 1, "flip", None)

        for row in tqdm.tqdm(
            dfs.itertuples(),
            total=len(dfs),
            desc="  - Correct free water flow",
            disable=self.disable_progress,
        ):
            if pd.isna([row.streefpeil_from, row.streefpeil_to]).any() > 0:
                continue

            if row.streefpeil_from < row.streefpeil_to:
                # Invert water levels and area id's
                dfs.at[row.Index, "peilgebied_from"] = row.peilgebied_to
                dfs.at[row.Index, "peilgebied_to"] = row.peilgebied_from
                dfs.at[row.Index, "streefpeil_from"] = row.streefpeil_to
                dfs.at[row.Index, "streefpeil_to"] = row.streefpeil_from
                dfs.at[row.Index, "flip"] = "streefpeil"

        # Gemaal: water is pumped from low to high
        for row in tqdm.tqdm(
            dfs.itertuples(),
            total=len(dfs),
            desc="  - Correct pumped water flow",
            disable=self.disable_progress,
        ):
            if pd.isna([row.streefpeil_from, row.streefpeil_to]).any() > 0:
                continue

            if row.streefpeil_from > row.streefpeil_to and not pd.isna(row.gemaal):
                # Invert water levels and area id's
                dfs.at[row.Index, "peilgebied_from"] = row.peilgebied_to
                dfs.at[row.Index, "peilgebied_to"] = row.peilgebied_from
                dfs.at[row.Index, "streefpeil_from"] = row.streefpeil_to
                dfs.at[row.Index, "streefpeil_to"] = row.streefpeil_from
                if pd.isna(row.flip):
                    dfs.at[row.Index, "flip"] = "gemaal"
                else:
                    dfs.at[row.Index, "flip"] = "streefpeil,gemaal"

        return dfs

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _add_double_links(self, dfc: gpd.GeoDataFrame, filter_col: str) -> gpd.GeoDataFrame:
        """_summary_

        Parameters
        ----------
        dfc : gpd.GeoDataFrame
            _description_
        filter_col : str
            _description_

        Returns
        -------
        gpd.GeoDataFrame
            _description_
        """
        dfs = dfc.copy()
        dfs_filter = dfs[dfs[filter_col]].copy()

        for _, row in tqdm.tqdm(
            dfs_filter.iterrows(),
            total=len(dfs_filter),
            desc="Add double links for crossings with 'stuw' and 'gemaal'",
            disable=self.disable_progress,
        ):
            if not pd.isna(row.stuw) and not pd.isna(row.gemaal):
                pfrom = row.peilgebied_to
                pto = row.peilgebied_from
                sfrom = row.streefpeil_to
                sto = row.streefpeil_from
                # Check if the double link already exists
                if not ((dfs.peilgebied_from == pfrom) & (dfs.peilgebied_to == pto)).any():
                    add_row = row.copy()
                    # Reverse peilgebieden and streefpeilen, remove gemaal.
                    add_row.at["peilgebied_from"] = pfrom
                    add_row.at["peilgebied_to"] = pto
                    add_row.at["streefpeil_from"] = sfrom
                    add_row.at["streefpeil_to"] = sto
                    add_row.at["gemaal"] = None
                    add_row.at["flip"] = "dubbel_link"
                    dfs = pd.concat([dfs, gpd.GeoDataFrame([add_row], crs=dfs.crs)], ignore_index=False)
                    dfs = dfs.sort_index().reset_index(drop=True)

        return dfs

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _extend_groupvars(self, dfc: gpd.GeoDataFrame, base_groupvars: list[str]) -> list[str]:
        groupvars = base_groupvars.copy()

        if "gemaal" in dfc.columns:
            groupvars.append("gemaal")

        # Objects 'stuw' are (for now) not needed as separate crossings
        # if "stuw" in dfc.columns:
        #     # Aggregate multiple objects of type stuw to a single stuw.
        #     dfc.loc[~pd.isna(dfc.stuw), "stuw"] = "stuw"
        #     groupvars.append("stuw")

        return groupvars

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _aggregate_identical_links(self, dfc: gpd.GeoDataFrame, agg_links: bool) -> gpd.GeoDataFrame:
        """_summary_

        Parameters
        ----------
        dfc : gpd.GeoDataFrame
            _description_
        agg_links : bool
            _description_

        Returns
        -------
        gpd.GeoDataFrame
            _description_
        """
        group_col = "agg_links_group"
        old_use_col = "in_use"
        new_use_col = "agg_links_in_use"

        dfs = dfc.copy()
        if group_col not in dfs.columns:
            dfs.insert(len(dfs.columns) - 1, group_col, None)
        if new_use_col not in dfs.columns:
            dfs.insert(len(dfs.columns) - 1, new_use_col, True)
        dfs[group_col] = None
        dfs[new_use_col] = dfs[old_use_col].copy()

        if agg_links:
            peilgebieden = self.df_gpkg["peilgebied"].copy()
            # peilgebieden = peilgebieden[peilgebieden.peilgebied_cat == 0].copy()
            peilgebieden["geometry"] = peilgebieden.centroid
            peilgebieden = peilgebieden.set_index("globalid")

            dfs_filter = dfs[dfs[old_use_col] & dfs.crossing_type.isin(["00", "01", "02"])].copy()
            basegroup = ["peilgebied_from", "peilgebied_to"]
            groupvars = self._extend_groupvars(dfs_filter, basegroup)

            for gvars, group in tqdm.tqdm(
                dfs_filter.groupby(groupvars, dropna=False, sort=False),
                desc="Aggregate links between peilgebieden",
                disable=self.disable_progress,
            ):
                pfrom, pto = gvars[0], gvars[1]
                if pd.isna(pfrom) or pd.isna(pto):
                    self.log.error(
                        f"One or both values in {dict(zip(basegroup, [pfrom, pto]))} are NaN, skipping link aggregation"
                    )
                    continue

                agg_links_group = self.list_sep.join(map(str, gvars))
                dfs.loc[group.index, group_col] = agg_links_group
                dfs.loc[group.index, new_use_col] = False
                lbl_choice = group.distance(peilgebieden.geometry.at[pto]).idxmin()
                if "stuw" in dfs_filter.columns:
                    struct_str = self._make_structure_string(dfs.stuw.loc[group.index])
                    if struct_str == "":
                        struct_str = None
                    dfs.at[lbl_choice, "stuw"] = struct_str
                dfs.at[lbl_choice, new_use_col] = True

        return dfs

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def _aggregate_areas(self, dfc: gpd.GeoDataFrame, agg_links: bool) -> gpd.GeoDataFrame:
        """_summary_

        Parameters
        ----------
        dfc : gpd.GeoDataFrame
            _description_
        agg_links : bool
            _description_

        Returns
        -------
        gpd.GeoDataFrame
            _description_
        """
        col_agg_from = "agg_area_from"
        col_agg_to = "agg_area_to"
        col_agg_group = "agg_areas_group"
        old_use_col = "agg_links_in_use"
        new_use_col = "agg_areas_in_use"

        dfs = dfc.copy()
        if col_agg_from not in dfs.columns:
            dfs.insert(len(dfs.columns) - 1, col_agg_from, None)
        if col_agg_to not in dfs.columns:
            dfs.insert(len(dfs.columns) - 1, col_agg_to, True)
        if col_agg_group not in dfs.columns:
            dfs.insert(len(dfs.columns) - 1, col_agg_group, None)
        if new_use_col not in dfs.columns:
            dfs.insert(len(dfs.columns) - 1, new_use_col, True)
        dfs[col_agg_from] = dfs.peilgebied_from.copy()
        dfs[col_agg_to] = dfs.peilgebied_to.copy()
        dfs[col_agg_group] = dfs.agg_links_group.copy()
        dfs[new_use_col] = dfs[old_use_col].copy()

        agg_peilgebieden = None
        if self.agg_peilgebieden_layer is not None:
            agg_peilgebieden = self.df_gpkg[self.agg_peilgebieden_layer].copy()

        self.df_gpkg["peilgebied"]["agg_area"] = None
        if agg_peilgebieden is None:
            return dfs
        else:
            for row in tqdm.tqdm(
                self.df_gpkg["peilgebied"].itertuples(),
                desc="Assign aggregate areas",
                total=len(self.df_gpkg["peilgebied"]),
                disable=self.disable_progress,
            ):
                # Only category 0 and 1
                if row.peilgebied_cat not in [0, 1]:
                    continue

                # Determine overlapping aggregate areas
                idx = agg_peilgebieden.sindex.query(row.geometry, predicate="intersects")
                df_agg = agg_peilgebieden.iloc[idx, :].copy()
                df_agg["geometry"] = df_agg.intersection(row.geometry)
                df_agg["overlap"] = df_agg.geometry.area / row.geometry.area

                # Assign an aggreation area only if the sum of overlaps exceeds
                # the pre-defined threshold.
                if df_agg["overlap"].sum() >= self.agg_areas_threshold:
                    agg_id = df_agg.at[df_agg.overlap.idxmax(), self.agg_peilgebieden_column]
                    self.df_gpkg["peilgebied"].at[row.Index, "agg_area"] = agg_id

            # Assign aggregate areas to crossings
            agg_lookup = self.df_gpkg["peilgebied"][["globalid", "agg_area"]].set_index("globalid").agg_area
            sel_from = ~pd.isna(dfs.peilgebied_from)
            sel_peilgebied_from = dfs.peilgebied_from.loc[sel_from].to_numpy()
            dfs.loc[sel_from, col_agg_from] = agg_lookup.loc[sel_peilgebied_from].to_numpy()
            sel_to = ~pd.isna(dfs.peilgebied_to)
            sel_peilgebied_to = dfs.peilgebied_to.loc[sel_to].to_numpy()
            dfs.loc[sel_to, col_agg_to] = agg_lookup.loc[sel_peilgebied_to].to_numpy()

            # Aggregate crossings with an identical agg_area 'from' and 'to'
            for (agg_from, agg_to), group in tqdm.tqdm(
                dfs.groupby([col_agg_from, col_agg_to], dropna=False),
                desc="Disable crossings in aggregation areas",
                disable=self.disable_progress,
            ):
                if pd.isna(agg_from) or pd.isna(agg_to) or agg_from != agg_to:
                    continue
                dfs.loc[group.index, col_agg_group] = agg_from
                dfs.loc[group.index, new_use_col] = False

            # Aggregate links between aggregate areas
            if agg_links:
                # Determine grouping variables
                dfs_filter = dfs[dfs[new_use_col]].copy()
                groupvars = self._extend_groupvars(dfs_filter, [col_agg_from, col_agg_to])

                for gvars, group in tqdm.tqdm(
                    dfs_filter.groupby(groupvars, dropna=False, sort=False),
                    desc="Aggregate links between aggregate areas",
                    disable=self.disable_progress,
                ):
                    # Only aggregate links with differing, non-nan from and to.
                    agg_from, agg_to = gvars[0], gvars[1]
                    if pd.isna(agg_from) or pd.isna(agg_to) or agg_from == agg_to:
                        continue

                    agg_areas_links_group = self.list_sep.join(map(str, gvars))
                    dfs.loc[group.index, col_agg_group] = agg_areas_links_group
                    dfs.loc[group.index, new_use_col] = False
                    lbl_choice = group.index[0]
                    if "stuw" in dfs_filter.columns:
                        struct_str = self._make_structure_string(dfs.stuw.loc[group.index])
                        if struct_str == "":
                            struct_str = None
                        dfs.at[lbl_choice, "stuw"] = struct_str
                    dfs.at[lbl_choice, new_use_col] = True

            return dfs
