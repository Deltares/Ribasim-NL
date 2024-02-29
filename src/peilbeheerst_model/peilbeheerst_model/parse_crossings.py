import itertools
import logging
import math
import pathlib

import fiona
import geopandas as gpd
import numpy as np
import numpy.typing as npt
import pandas as pd
import pydantic
import shapely.ops
import tqdm
from shapely.geometry import LineString, MultiLineString, MultiPoint, Point, Polygon


class ParseCrossings:
    def __init__(
        self,
        gpkg_path: pathlib.Path | str,
        allowed_distance: float = 0.5,
        search_radius_structure: float = 60.0,
        search_radius_HWS_BZM: float = 30.0,
        move_distance: float = 1e-3,
        almost_equal: float = 1e-6,
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
        allowed_distance : float, optional
            _description_, by default 0.5
        search_radius_structure : float, optional
            _description_, by default 60.0
        search_radius_HWS_BZM : float, optional
            _description_, by default 30.0
        move_distance : float, optional
            _description_, by default 1e-3
        almost_equal : float, optional
            _description_, by default 1e-6
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

        # Distance to consider a line on a boundary
        self.almost_zero = almost_zero

        # List separator used in serialization/deserialization.
        self.list_sep = list_sep

        # Boolean to silence the progress bars.
        self.disable_progress = disable_progress

        # read all layers of geopackage
        self.df_gpkg = {L: gpd.read_file(gpkg_path, layer=L) for L in fiona.listlayers(gpkg_path)}

        # Validate globalids
        for layername, df_layer in self.df_gpkg.items():
            if "globalid" not in df_layer.columns:
                continue
            if df_layer.globalid.str.contains(self.list_sep).any():
                raise ValueError(f"{layername}: contains the reserved character '{self.list_sep}'")

        if "peilgebied_cat" not in self.df_gpkg["peilgebied"].columns:
            self.df_gpkg["peilgebied"]["peilgebied_cat"] = 0
        self.peilgebied_cat_lookup = self.df_gpkg["peilgebied"].set_index("globalid").peilgebied_cat.copy()

        logger_name = f"{__name__.split('.')[0]}_{pathlib.Path(gpkg_path).stem}"
        self.log = logging.getLogger(logger_name)
        handlers = [logging.NullHandler()]
        if show_log:
            handlers.append(logging.StreamHandler())
        if logfile is not None:
            handlers.append(logging.FileHandler(pathlib.Path(logfile), "w"))
        for handler in handlers:
            formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
        self.log.setLevel(logging.DEBUG)

    @staticmethod
    @pydantic.validate_call(config={"strict": True})
    def polar(x1: float, x2: float, y1: float, y2: float) -> tuple[float, float]:
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
    def extend_linestrings(
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
                sina, cosa = self.polar(x1, x2, y1, y2)
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

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def parse_peilgebieden(self) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
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
        df_peilgebieden["geometry"] = df_peilgebieden.buffer(0)
        df_peilgebieden = df_peilgebieden[~df_peilgebieden.is_empty].copy()
        if pd.isna(df_peilgebieden.globalid).any():
            raise ValueError("One or more globalids of 'peilgebied' are null")

        # Determine the boundaries of the peilgebieden.
        df_peil_boundary = df_peilgebieden.copy()
        df_peil_boundary["geometry"] = df_peil_boundary.geometry.boundary

        return df_peilgebieden, df_peil_boundary

    @pydantic.validate_call(config={"strict": True})
    def find_crossings_with_peilgebieden(
        self,
        layer: str,
        group_stacked: bool = True,
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
        df_peilgebieden, df_peil_boundary = self.parse_peilgebieden()

        # Determine endpoints
        df_linesingle, df_endpoints = self.extend_linestrings(df_linesingle, df_peil_boundary)

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
                crossings = self.add_potential_crossing(
                    crossings,
                    row_line.Index,
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

        # Add waterlevels, structures and correct water flow based on these.
        dfc = self.add_waterlevels_to_crossings(dfc)
        dfc = self.find_structures_at_crossings(dfc, df_linesingle, df_endpoints, "stuw")
        dfc = self.find_structures_at_crossings(dfc, df_linesingle, df_endpoints, "gemaal")
        dfc = self.correct_water_flow(dfc)

        # If needed, check for multiple stacked/grouped crossings which can be
        # reduced to a single crossing or no crossing at all.
        line_groups = None
        if group_stacked:
            dfc, line_groups = self.find_stacked_crossings(
                layer, dfc, df_linesingle, df_endpoints, df_peilgebieden, reduce
            )
            dfc = self.correct_water_flow(dfc)

        if filterlayer is None:
            # return the found crossings.
            dfc = self.correct_structures(dfc, df_linesingle, df_endpoints, "stuw")
            dfc = self.correct_structures(dfc, df_linesingle, df_endpoints, "gemaal")
            if return_lines:
                return dfc, line_groups
            else:
                return dfc
        else:
            # Filter the crossings with another layer with overlapping lines
            df_filter, dfs = self.filter_crossings_with_layer(dfc, df_peilgebieden, filterlayer, write_debug)
            dfs = self.correct_structures(dfs, df_linesingle, df_endpoints, "stuw")
            dfs = self.correct_structures(dfs, df_linesingle, df_endpoints, "gemaal")
            dfs = self.correct_water_flow(dfs)
            return dfc, df_filter, dfs

    @pydantic.validate_call(config={"strict": True})
    def _classify_from_to_peilgebieden(self, pfrom: str | None, pto: str | None) -> tuple[int, str]:
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
        """
        df_single = self.df_gpkg[layername].copy()

        if pd.isna(df_single.globalid).any():
            raise ValueError(f"One or more globalids of '{layername}' are null")

        if id_as_index:
            df_single = df_single.set_index("globalid", inplace=False)

        df_single = df_single.explode(ignore_index=False, index_parts=True)
        df_single["geometry"] = df_single.make_valid()
        df_single = df_single[~df_single.is_empty].copy()

        return df_single

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def find_connections(self, line_index: tuple, df_endpoints: gpd.GeoDataFrame) -> gpd.GeoSeries:
        """_summary_

        Parameters
        ----------
        line_index : tuple
            _description_
        df_endpoints : gpd.GeoDataFrame
            _description_

        Returns
        -------
        gpd.GeoSeries
            _description_
        """
        endpoints_line = df_endpoints.geometry.loc[line_index]
        if not isinstance(endpoints_line, MultiPoint):
            endpoints_line = endpoints_line.to_numpy()[0]
        pot_conn = df_endpoints.geometry.iloc[df_endpoints.sindex.query(endpoints_line, predicate="intersects")].copy()

        return pot_conn

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def enforce_linestring(
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
    def make_merged_line(
        self,
        crossing: Point,
        line_index: tuple,
        df_endpoints: gpd.GeoDataFrame,
        df_linesingle: gpd.GeoDataFrame,
        pot_conn: gpd.GeoSeries,
        df_peilgebied: gpd.GeoDataFrame,
    ) -> tuple[LineString, str]:
        """_summary_

        Parameters
        ----------
        crossing : Point
            _description_
        line_index : tuple
            _description_
        df_endpoints : gpd.GeoDataFrame
            _description_
        df_linesingle : gpd.GeoDataFrame
            _description_
        pot_conn : gpd.GeoSeries
            _description_
        df_peilgebied : gpd.GeoDataFrame
            _description_

        Returns
        -------
        tuple[LineString, str]
            _description_
        """
        crossing_line = df_linesingle.geometry.at[line_index]
        endpoints_line = df_endpoints.geometry.loc[line_index]
        ring_buffer = crossing.buffer(self.allowed_distance)
        point_buffer = crossing.buffer(self.almost_equal)

        # Try and merge any connecting lines to a single, valid LineString
        if ring_buffer.intersects(endpoints_line):
            crossing_lines = df_linesingle.loc[pot_conn.intersects(ring_buffer).index.to_numpy()].copy()
            add_crossing_lines = crossing_lines.geometry.tolist()
            line_ids = self.list_sep.join(sorted(crossing_lines.globalid.tolist()))
            if len(add_crossing_lines) > 0:
                add_crossing_lines.append(crossing_line)
                line = MultiLineString(add_crossing_lines).intersection(ring_buffer)
            else:
                line = crossing_line.intersection(ring_buffer)
        else:
            line_ids = df_linesingle.globalid.at[line_index]
            line = crossing_line.intersection(ring_buffer)

        # Try and merge the MultiLineString to a single LineString
        line = self.enforce_linestring(line, crossing, point_buffer, df_peilgebied)

        return line, line_ids

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def make_merged_line_stacked(
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
        idx, idx_conn = self._find_closest_lines(geom, self.almost_equal, df_linesingle, df_endpoints, n_recurse=1)
        if len(idx_conn) == 0:
            self.log.warning(f"Crossing {geom} is not on or near a line, ignoring...")
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
    def add_potential_crossing(
        self,
        crossings: dict,
        line_index: tuple,
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
        line_index : tuple
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
        pot_conn = self.find_connections(line_index, df_endpoints)

        # crossings = {}
        for crossing in crossing_points.geometry:
            # Find crossing line with potentially added connections
            merged_crossing_line, merged_ids = self.make_merged_line(
                crossing,
                line_index,
                df_endpoints,
                df_linesingle,
                pot_conn,
                df_peilgebied,
            )

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
    def find_stacked_crossings(
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
            line_geom = self.make_merged_line_stacked(
                point_buffer1,
                row.geometry,
                df_linesingle,
                df_endpoints,
                reduce=reduce,
            )

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
                dfs.loc[group.index, "match_group"] = groupid
                dfs.loc[group.index, "match_stacked"] = len(group)
                dfs.loc[group.index, "match_group_unique"] = True
                dfs.loc[group.index, "in_use"] = True
                line_groups[groupid] = line_geom
                groupid += 1
            else:
                if line_geom.geom_type == "MultiLineString":
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
                                # Edge case where a peilgebied just barely
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
                                # Special case: correct the flow order of crossing_type=1.
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
    def add_waterlevels_to_crossings(self, dfc: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
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
    def filter_crossings_with_layer(
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

        dfs = dfc.copy()
        dfs_valid = dfs[dfs.in_use].copy()

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

            line_geom = line_groups[groupid]

            replace_crossing_candidates = group[group.in_use].copy()

            replace_stuw = None
            if "stuw" in dfs.columns:
                replace_stuw = ",".join(replace_crossing_candidates.stuw.dropna().unique())
            replace_gemaal = None
            if "gemaal" in dfs.columns:
                replace_gemaal = ",".join(replace_crossing_candidates.gemaal.dropna().unique())

            replace_crossing_vec = []
            if len(replace_crossing_candidates) == 0:
                replace_crossing_vec.append(None)
            elif len(replace_crossing_candidates) == 1:
                replace_crossing_vec.append(replace_crossing_candidates.iloc[0,].copy())
            else:
                if line_geom.geom_type == "LineString" or line_geom.geom_type == "MultiLineString":
                    for x0, x1 in itertools.combinations(list(line_geom.boundary.geoms), 2):
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
                            replace_crossing_vec.append(None)
                        else:
                            type_areas, str_areas = self._classify_from_to_peilgebieden(p0, p1)
                            matches = replace_crossing_candidates[replace_crossing_candidates.peilgebieden == str_areas]
                            if len(matches) > 0:
                                replace_crossing = replace_crossing_candidates.loc[matches.index[0], :].copy()
                                if replace_gemaal is not None:
                                    replace_crossing["gemaal"] = replace_gemaal
                                    replace_gemaal = None
                                if replace_stuw is not None:
                                    replace_crossing["stuw"] = replace_stuw
                                    replace_gemaal = None
                                replace_crossing_vec.append(replace_crossing)
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
                                replace_crossing["flip"] = None
                                replace_crossing["in_use"] = True
                                if replace_gemaal is not None:
                                    replace_crossing["gemaal"] = replace_gemaal
                                    replace_gemaal = None
                                if replace_stuw is not None:
                                    replace_crossing["stuw"] = replace_stuw
                                    replace_gemaal = None
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
                if replace_crossing is not None:
                    pfrom = replace_crossing.peilgebied_from
                    pto = replace_crossing.peilgebied_to
                    check_from = matching_crossings.peilgebied_from == pfrom
                    check_to = matching_crossings.peilgebied_to == pto
                    c_exists = matching_crossings.index[check_from & check_to]
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
            dfs = dfs.sort_index().reset_index(drop=True)

        return df_filter, dfs

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def find_structures_at_crossings(
        self,
        dfc: gpd.GeoDataFrame,
        df_linesingle: gpd.GeoDataFrame,
        df_endpoints: gpd.GeoDataFrame,
        structurelayer: str,
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
        if "in_use" in dfs.columns:
            df_filter = dfs[dfs.in_use].copy()
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
        filter: None | str = None,
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
        filter : None | str, optional
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
        # Find the line object nearest to the PoI
        if filter is None:
            idx = df_linesingle.sindex.query(poi.buffer(max_dist), predicate="intersects")
        elif filter == "nearest":
            idx = df_linesingle.sindex.nearest(poi, max_distance=max_dist)[1, :]
        else:
            raise ValueError(f"Unknown filter argument ({filter=})")

        # Return early if we did not find any lines
        if len(idx) == 0:
            return idx, idx

        # Recursively search for connected lines
        idx_conn = idx.copy()
        for _ in range(n_recurse):
            # Check for line ends ending on the current line
            line_buffer = df_linesingle.geometry.iloc[idx_conn].buffer(self.almost_equal)
            idx_conn_new1 = df_endpoints.sindex.query(line_buffer, predicate="intersects")[1, :]

            # Check for the current line ends ending on other lines
            point_buffer = df_endpoints.geometry.iloc[idx_conn].buffer(self.almost_equal)
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
    def _assign_structure(
        self,
        dfs: gpd.GeoDataFrame,
        orphaned_structures: list,
        df_filter: gpd.GeoDataFrame,
        df_linesingle: gpd.GeoDataFrame,
        df_endpoints: gpd.GeoDataFrame,
        df_structures: gpd.GeoDataFrame,
        structure_geom: shapely.geometry.Point,
        structure_id: str,
        structurelayer: str,
    ) -> tuple[gpd.GeoDataFrame, list]:
        """_summary_

        Parameters
        ----------
        orphaned_structures : list
            _description_
        dfs : gpd.GeoDataFrame
            _description_
        df_filter : gpd.GeoDataFrame
            _description_
        df_linesingle : gpd.GeoDataFrame
            _description_
        df_endpoints : gpd.GeoDataFrame
            _description_
        df_structures : gpd.GeoDataFrame
            _description_
        structure_geom : shapely.geometry.Point
            _description_
        structure_id : str
            _description_
        structurelayer : str
            _description_

        Returns
        -------
        tuple
            _description_
        """

        # Find the line objects nearest to the structure
        _, idxs = self._find_closest_lines(
            structure_geom,
            self.search_radius_structure,
            df_linesingle,
            df_endpoints,
            n_recurse=5,
            filter="nearest",
        )
        if len(idxs) == 0:
            self.log.warning(f"{structurelayer} '{structure_id}' has no line object nearby")
            return dfs, orphaned_structures

        # Find closest point on nearest line(s)
        df_line_geom = df_linesingle.iloc[idxs, :].copy()
        buff_line = df_line_geom.buffer(self.almost_equal)
        idx = df_filter.sindex.query(buff_line, predicate="intersects")
        df_close = df_filter.iloc[np.unique(idx[1, :]), :].copy()
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
        if pd.isna(dfs.at[lbl, structurelayer]):
            dfs.loc[df_stacked.index, structurelayer] = structure_id
        else:
            # Only add the new structure to the crossing(s) if the new
            # structure is closer by than the old structure.
            crossing = dfs.geometry.at[lbl]
            old_struct_ids = dfs.at[lbl, structurelayer].split(",")
            old_structs = df_structures.geometry.loc[old_struct_ids]
            if old_structs.distance(crossing).min() > structure_geom.distance(crossing):
                self.log.info(f"Replacing {structurelayer} at {crossing} with '{structure_id}'")
                for old_struct_id, old_struct in zip(old_struct_ids, old_structs):
                    orphaned_structures.append((old_struct_id, old_struct))
                dfs.loc[df_stacked.index, structurelayer] = structure_id

        return dfs, orphaned_structures

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def correct_structures(
        self,
        dfc: gpd.GeoDataFrame,
        df_linesingle: gpd.GeoDataFrame,
        df_endpoints: gpd.GeoDataFrame,
        structurelayer: str,
    ) -> gpd.GeoDataFrame:
        # Reference to the structure GeoDataFrame.
        df_structures = self.df_gpkg[structurelayer].copy()
        df_structures = df_structures.set_index("globalid")

        # Copy crossings and valid crossings.
        dfs = dfc.copy()
        df_filter = dfs[dfs.in_use].copy()

        # Find previously assigned structures that are now unassigned.
        orphaned_structures = []
        for structure_id, group in dfs.groupby(structurelayer, sort=False):
            if not group.in_use.any():
                for sid in structure_id.split(","):
                    orphaned_structures.append((sid, df_structures.geometry.at[sid]))

        df_sub_structures = pd.DataFrame(orphaned_structures, columns=["globalid", "geometry"])
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
                )
            df_sub_structures = pd.DataFrame(orphaned_structures, columns=["globalid", "geometry"])

        return dfs

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def correct_water_flow(self, dfc: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
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
    def add_double_links(self, dfc: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
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

        add_rows = []
        for row in tqdm.tqdm(
            dfs.itertuples(),
            total=len(dfs),
            desc="Add double links for crossings with 'stuw' and 'gemaal'",
            disable=self.disable_progress,
        ):
            if not pd.isna(row.stuw) and not pd.isna(row.gemaal):
                add_row = dfs.loc[row.Index, :].copy()
                # Keer peilen en peilgebieden om
                add_row.at["peilgebied_from"] = row.peilgebied_to
                add_row.at["peilgebied_to"] = row.peilgebied_from
                add_row.at["streefpeil_from"] = row.streefpeil_to
                add_row.at["streefpeil_to"] = row.streefpeil_from
                add_row.at["flip"] = "dubbel_link"
                add_rows.append(add_row)

        if len(add_rows) > 0:
            dfs = pd.concat([dfs, gpd.GeoDataFrame(add_rows, crs=dfs.crs)], ignore_index=False).sort_index()
            dfs = dfs.reset_index(drop=True)

        return dfs

    @pydantic.validate_call(config={"arbitrary_types_allowed": True, "strict": True})
    def aggregate_identical_links(self, dfc: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
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
        if "agg1_group" not in dfs.columns:
            dfs.insert(len(dfs.columns) - 1, "agg1_group", None)
        if "agg1_used" not in dfs.columns:
            dfs.insert(len(dfs.columns) - 1, "agg1_used", True)
        dfs["agg1_group"] = None
        dfs["agg1_used"] = dfs.in_use.copy()

        peilgebieden = self.df_gpkg["peilgebied"].copy()
        peilgebieden = peilgebieden[peilgebieden.peilgebied_cat == 0].copy()
        peilgebieden["geometry"] = peilgebieden.centroid
        peilgebieden = peilgebieden.set_index("globalid")

        dfs_filter = dfs[dfs.in_use & (dfs.crossing_type == "00")].copy()

        groupvars = ["peilgebied_from", "peilgebied_to"]
        if "gemaal" in dfs_filter.columns:
            groupvars.append("gemaal")

        if "stuw" in dfs_filter.columns:
            # Aggregate multiple objects of type stuw to a single stuw.
            dfs_filter.loc[~pd.isna(dfs_filter.stuw), "stuw"] = "stuw"
            groupvars.append("stuw")

        for gvars, group in tqdm.tqdm(
            dfs_filter.groupby(groupvars, dropna=False, sort=False),
            desc="Aggregate to single unique links",
            disable=self.disable_progress,
        ):
            pto = gvars[1]
            agg1_group = self.list_sep.join(map(str, gvars))
            dfs.loc[group.index, "agg1_group"] = agg1_group
            dfs.loc[group.index, "agg1_used"] = False
            lbl_choice = group.distance(peilgebieden.geometry.at[pto]).idxmin()
            if "stuw" in dfs_filter.columns:
                dfs.at[lbl_choice, "stuw"] = ",".join(dfs.stuw.loc[group.index].dropna().unique())
            dfs.at[lbl_choice, "agg1_used"] = True

        return dfs
