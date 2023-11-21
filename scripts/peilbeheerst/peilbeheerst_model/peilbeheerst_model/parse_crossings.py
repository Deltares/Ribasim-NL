import pathlib
from typing import Dict, Optional, Tuple, Union

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.ops
import tqdm
from pydantic import validate_call
from shapely.geometry import LineString, MultiLineString, MultiPoint, Point


class ParseCrossings:
    @validate_call
    def __init__(
        self,
        gpkg_path: Union[pathlib.Path, str],
        allowed_distance: float = 0.5,
        search_radius_structure: float = 5.0,
        almost_equal: float = 1e-6,
        disable_progress: bool = False,
        debug: bool = False,
    ) -> None:
        """_summary_

        Parameters
        ----------
        gpkg_path : Union[pathlib.Path, str]
            _description_
        allowed_distance : float, optional
            _description_, by default 0.5
        search_radius_structure : float, optional
            _description_, by default 5.0
        almost_equal : float, optional
            _description_, by default 1e-6
        disable_progress : bool, optional
            _description_, by default False
        debug : bool, optional
            _description_, by default False
        """
        # Maximum allowed distance difference between 2 points
        # to be considered the same crossing.
        self.allowed_distance = allowed_distance

        # Search radius for structures near a crossing.
        self.search_radius_structure = search_radius_structure

        # Radius to exclude a point itself from a buffer search.
        self.almost_equal = almost_equal

        # Boolean to silence the progress bars.
        self.disable_progress = disable_progress

        # Boolean to show debug messages
        self.debug = debug

        # read all layers of geopackage
        self.df_gpkg = {
            l: gpd.read_file(gpkg_path, layer=l) for l in fiona.listlayers(gpkg_path)
        }

    @validate_call
    def find_crossings_with_peilgebieden(
        self, layer: str, group_stacked: bool = True, filterlayer: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """_summary_

        Parameters
        ----------
        layer : str
            _description_
        group_stacked : bool, optional
            _description_, by default True
        filterlayer : Optional[str], optional
            _description_, by default None

        Returns
        -------
        gpd.GeoDataFrame
            _description_

        Raises
        ------
        ValueError
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

        endpoints = df_linesingle.copy()
        endpoints["geometry"] = df_linesingle.geometry.apply(
            lambda g: MultiPoint([g.coords[0], g.coords[-1]])
        )

        # Force valid peilgebieden with buffer=0 and remove empty geometries.
        df_peilgebieden = self.df_gpkg["peilgebied"].copy()
        df_peilgebieden["geometry"] = df_peilgebieden.buffer(0)
        df_peilgebieden = df_peilgebieden[~df_peilgebieden.is_empty].copy()
        if pd.isnull(df_peilgebieden.globalid).any():
            raise ValueError("One or more globalids of 'peilgebied' are null")

        # Determine the boundaries of the peilgebieden.
        df_peil_boundary = df_peilgebieden.copy()
        df_peil_boundary["geometry"] = df_peil_boundary.geometry.boundary

        # Find crossings of the lines with the peilgebieden.
        for row_line in tqdm.tqdm(
            df_linesingle.itertuples(),
            total=len(df_linesingle),
            desc=f"Find crossings for '{layer}'",
            disable=self.disable_progress,
        ):
            # Find the intersection of the current line with the peilgebieden.
            idx_peilgebieden = df_peil_boundary.sindex.query(
                row_line.geometry, predicate="intersects"
            )

            # If no areas intersect, continue with the following line.
            if len(idx_peilgebieden) == 0:
                continue

            # Add peilgebieden which completely contain the current line.
            idx_peilgebieden2 = df_peilgebieden.sindex.query(
                row_line.geometry, predicate="within"
            )
            idx_peilgebieden2 = list(
                set(idx_peilgebieden2).difference(set(idx_peilgebieden))
            )
            idx_within = df_peilgebieden.index[idx_peilgebieden2].values

            # Find crossings with the current line.
            df_points = df_peil_boundary.iloc[idx_peilgebieden, :].copy()
            df_points["geometry"] = df_points.intersection(row_line.geometry)

            # Subset of polygons of the intersecting areas.
            df_subsetpeil_poly = df_peilgebieden.loc[
                np.hstack([df_points.index.values, idx_within]), :
            ].copy()

            # Explode to single parts and remove empty or non-point geometries.
            df_points = df_points.explode(ignore_index=False, index_parts=True)
            df_points = df_points[
                (~df_points.is_empty) & (df_points.geometry.geom_type == "Point")
            ].copy()

            if len(df_subsetpeil_poly) == 0:
                # No areas intersect, continue with the following line.
                continue
            else:
                # At least 1 peilgebied match: Determine potential crossing
                # with these peilgebieden.
                crossings = self.add_potential_crossing(
                    row_line.Index,
                    row_line.geometry,
                    endpoints,
                    df_linesingle,
                    df_points,
                    df_subsetpeil_poly,
                )

                # Add the found crossings.
                for (pfrom, pto, _, _), crossing in crossings.items():
                    n_areas, str_areas = self._classify_from_to_peilgebieden(pfrom, pto)
                    dfc[layer].append(row_line.globalid)
                    dfc["crossing_type"].append(n_areas)
                    dfc["peilgebieden"].append(str_areas)
                    dfc["peilgebied_from"].append(pfrom)
                    dfc["peilgebied_to"].append(pto)
                    dfc["geometry"].append(crossing)

        # Create dataframe of (potential) crossings
        dfc = gpd.GeoDataFrame(dfc, geometry="geometry", crs="epsg:28992")

        # If needed, check for multiple stacked/grouped crossings which can be
        # reduced to a single crossing or no crossing at all.
        if group_stacked:
            dfc = self.find_stacked_crossings(layer, dfc)

        if filterlayer is None:
            # return the found crossings.
            return dfc
        else:
            # Filter the crossings with another layer with overlapping lines
            df_filter, dfs = self.filter_crossings_with_layer(
                dfc, filterlayer, group_stacked
            )
            return dfc, df_filter, dfs

    @staticmethod
    @validate_call
    def _classify_from_to_peilgebieden(
        pfrom: Optional[str], pto: Optional[str]
    ) -> Tuple[int, str]:
        """_summary_

        Parameters
        ----------
        pfrom : Optional[str]
            _description_
        pto : Optional[str]
            _description_

        Returns
        -------
        Tuple[int, str]
            _description_
        """
        crossing_areas = sorted([p for p in [pfrom, pto] if p is not None])
        n_areas = len(crossing_areas)
        str_areas = ",".join(crossing_areas)

        return n_areas, str_areas

    @validate_call
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

        if pd.isnull(df_single.globalid).any():
            raise ValueError(f"One or more globalids of '{layername}' are null")

        if id_as_index:
            df_single = df_single.set_index("globalid", inplace=False)

        df_single = df_single.explode(ignore_index=False, index_parts=True)
        df_single["geometry"] = df_single.make_valid()
        df_single = df_single[~df_single.is_empty].copy()

        return df_single

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def add_potential_crossing(
        self,
        line_index,
        crossing_line: LineString,
        df_endpoints: gpd.GeoDataFrame,
        df_linesingle: gpd.GeoDataFrame,
        crossing_points: gpd.GeoDataFrame,
        df_peilgebied: gpd.GeoDataFrame,
    ) -> Dict[Tuple[Optional[str], Optional[str], float, float], Point]:
        """_summary_

        Parameters
        ----------
        line_index : _type_
            _description_
        crossing_line : LineString
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
        Dict[Tuple[Optional[str], Optional[str], float, float], Point]
            _description_
        """
        endpoints_line = df_endpoints.geometry.loc[line_index]
        pot_conn = df_endpoints.geometry.iloc[
            df_endpoints.sindex.query(endpoints_line, predicate="intersects")
        ].copy()
        pot_conn = pot_conn[pot_conn.intersects(endpoints_line)].copy()
        pot_conn = pot_conn[pot_conn.index != line_index].copy()

        crossings = {}
        for crossing in crossing_points.geometry:
            # Find intersection with buffered point, excluding the point itself.
            ring_buffer = crossing.buffer(self.allowed_distance)
            point_buffer = crossing.buffer(self.almost_equal)

            # Try and merge any connecting lines to a single, valid LineString
            if ring_buffer.intersects(endpoints_line):
                add_crossing_lines = df_linesingle.geometry.loc[
                    pot_conn.intersects(ring_buffer).index.values
                ].tolist()
                if len(add_crossing_lines) > 0:
                    add_crossing_lines.append(crossing_line)
                    line = MultiLineString(add_crossing_lines).intersection(ring_buffer)
                else:
                    line = crossing_line.intersection(ring_buffer)
            else:
                line = crossing_line.intersection(ring_buffer)

            # Try and merge the MultiLineString to a single LineString
            if line.geom_type == "MultiLineString":
                line = shapely.ops.linemerge(line)

            # If we still have a MultiLineString, use the first LineString that
            # overlaps with the point.
            if line.geom_type == "MultiLineString":
                temp = gpd.GeoSeries([line]).explode(index_parts=False)
                line = temp[temp.intersects(point_buffer)].geometry.iat[0]

            # Make two lines of the single LineString, separated by the point.
            # This can still result in a single LineString if the point happens
            # to coincide with an endpoint of the line. Enforce MultiLineString
            # in that case.
            line = line.intersection(ring_buffer.difference(point_buffer))
            if line.geom_type == "LineString":
                line = MultiLineString([line])

            # Reference distance of crossing along line.
            crosspoint_dist = crossing_line.project(crossing)

            # Find area names of the intersecting peilgebieden.
            buffer_from = []
            buffer_to = []
            if line.geom_type == "MultiLineString":
                for subline in line.geoms:
                    if subline.geom_type == "LineString":
                        idxmatches = df_peilgebied[
                            df_peilgebied.intersects(subline)
                        ].index.tolist()
                        test_bound = df_peilgebied.geometry.loc[
                            idxmatches
                        ].boundary.intersection(subline)
                        test_bound = test_bound[
                            (~test_bound.is_empty)
                            & (test_bound.geom_type == "LineString")
                        ]
                        test_bound = test_bound[
                            test_bound.geom_equals(subline).values
                        ].index.tolist()
                        if len(test_bound) > 0:
                            idxmatches = sorted(
                                list(set(idxmatches).difference(test_bound))
                            )
                        matches = df_peilgebied.globalid.loc[idxmatches].tolist()
                        dist = crossing_line.project(Point(subline.coords[-1]))
                        if dist < crosspoint_dist:
                            buffer_from += matches
                        else:
                            buffer_to += matches

            # Reduce to sorted lists which only contain unique names.
            buffer_from = sorted(list(set(buffer_from)))
            buffer_to = sorted(list(set(buffer_to)))
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
                    crossings[(bfrom, bto, crossing.x, crossing.y)] = crossing

        return crossings

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def find_stacked_crossings(
        self, crossing_layer: str, dfc: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """_summary_

        Parameters
        ----------
        crossing_layer : str
            _description_
        dfc : gpd.GeoDataFrame
            _description_

        Returns
        -------
        gpd.GeoDataFrame
            _description_
        """

        df_linesingle = self._get_layer_as_singleparts(crossing_layer, id_as_index=True)

        dfs = dfc.copy()
        dfs.insert(len(dfs.columns) - 1, "match_group", 0)
        dfs.insert(len(dfs.columns) - 1, "match_stacked", 0)
        dfs.insert(len(dfs.columns) - 1, "match_composite", False)
        dfs.insert(len(dfs.columns) - 1, "match_group_unique", False)
        dfs.insert(len(dfs.columns) - 1, "in_use", False)

        groupid = 1
        new_rows = []
        for row in tqdm.tqdm(
            dfs.itertuples(),
            desc="Group geometrically stacked crossings",
            total=len(dfs),
            disable=self.disable_progress,
        ):
            line_id = row[dfs.columns.get_loc(crossing_layer) + 1]

            point_buffer1 = row.geometry.buffer(self.allowed_distance)
            group = dfs.iloc[
                dfs.sindex.query(point_buffer1, predicate="intersects"), :
            ].copy()
            group = group[
                (group.match_group == 0) & group.intersects(point_buffer1)
            ].copy()

            # Continue with the next iteration if we have no matches
            if len(group) == 0:
                continue

            # Find the line(s) on which the current crossing lies, or which are
            # connected to the line(s) of the current crossing. Remove points
            # in the group which are not on these lines.
            point_buffer2 = row.geometry.buffer(self.almost_equal)
            line_union = df_linesingle.geometry.iloc[
                df_linesingle.sindex.query(point_buffer2, predicate="intersects")
            ].copy()
            line_union = line_union[line_union.intersects(point_buffer2)].unary_union
            line_buffer = line_union.buffer(self.almost_equal)
            if line_buffer.is_valid:
                line_geom = df_linesingle.geometry.iloc[
                    df_linesingle.sindex.query(line_buffer, predicate="intersects")
                ].copy()
                line_geom = line_geom[line_geom.intersects(line_buffer)]
            else:
                line_geom = df_linesingle.geometry[
                    df_linesingle.distance(line_union) <= self.almost_equal
                ]

            line_geom = line_geom.intersection(line_buffer).unary_union
            if line_geom.geom_type == "MultiLineString":
                line_geom = shapely.ops.linemerge(line_geom)

            group = group[group.distance(line_geom) <= self.almost_equal].copy()

            if len(group) == 0:
                continue
            elif len(group) == 1:
                dfs.loc[group.index, "match_group"] = groupid
                dfs.loc[group.index, "match_stacked"] = len(group)
                dfs.loc[group.index, "match_group_unique"] = True
                dfs.at[group.index[0], "in_use"] = True
                groupid += 1
            else:
                group["dist_along"] = np.NaN
                for subrow in group.itertuples():
                    group.at[subrow.Index, "dist_along"] = line_geom.project(
                        subrow.geometry
                    )
                group = group.sort_values("dist_along")
                pfrom = group.peilgebied_from[
                    group.dist_along == group.dist_along.min()
                ].unique()
                pto = group.peilgebied_to[
                    group.dist_along == group.dist_along.max()
                ].unique()
                match_group_unique = True
                if len(pfrom) != 1 or len(pto) != 1:
                    if self.debug:
                        print(
                            f"Cannot find a unique point for point group '{groupid}' on line '{line_id}'"
                        )
                    match_group_unique = False
                pfrom = pfrom[0]
                pto = pto[0]

                if pfrom != pto:
                    if pfrom is None:
                        check_from = pd.isnull(group.peilgebied_from)
                    else:
                        check_from = group.peilgebied_from == pfrom
                    if pto is None:
                        check_to = pd.isnull(group.peilgebied_to)
                    else:
                        check_to = group.peilgebied_to == pto
                    entry_exists = check_from & check_to
                    if entry_exists.any():
                        # The entry exists, toggle it to 'in_use'.
                        dfs.at[group[entry_exists].index[0], "in_use"] = True
                    else:
                        # The entry does not exist yet, create a new composite entry
                        n_areas, str_areas = self._classify_from_to_peilgebieden(
                            pfrom, pto
                        )
                        new_row = dfs.loc[group.index[0], :].copy()
                        new_row["peilgebieden"] = str_areas
                        new_row["crossing_type"] = n_areas
                        new_row["peilgebied_from"] = pfrom
                        new_row["peilgebied_to"] = pto
                        new_row["match_group"] = groupid
                        new_row["match_stacked"] = len(group)
                        new_row["match_composite"] = True
                        new_row["match_group_unique"] = match_group_unique
                        new_row["in_use"] = True
                        new_rows.append(new_row)
                dfs.loc[group.index, "match_group"] = groupid
                dfs.loc[group.index, "match_stacked"] = len(group)
                dfs.loc[group.index, "match_group_unique"] = match_group_unique
                groupid += 1

        if len(new_rows) > 0:
            new_rows = gpd.GeoDataFrame(new_rows, geometry="geometry", crs=dfs.crs)
            dfs = pd.concat([dfs, new_rows], ignore_index=False)
            dfs = dfs.sort_index().reset_index(drop=True)

        return dfs

    @validate_call(config=dict(arbitrary_types_allowed=True))
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
        strfpl = (
            self.df_gpkg["streefpeil"]
            .copy()
            .set_index("globalid", inplace=False)
            .waterhoogte
        )

        # Copy crossings and valid crossings.
        dfs = dfc.copy()

        # Add streefpeil information to valid crossings
        dfs.insert(len(dfs.columns) - 1, "streefpeil_from", np.NaN)
        dfs.insert(len(dfs.columns) - 1, "streefpeil_to", np.NaN)
        for row in tqdm.tqdm(
            dfs.itertuples(),
            total=len(dfs),
            desc="Add water levels to crossings",
            disable=self.disable_progress,
        ):
            if not pd.isnull(row.peilgebied_from):
                dfs.at[row.Index, "streefpeil_from"] = strfpl.at[row.peilgebied_from]
            if not pd.isnull(row.peilgebied_to):
                dfs.at[row.Index, "streefpeil_to"] = strfpl.at[row.peilgebied_to]

        return dfs

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def filter_crossings_with_layer(
        self, dfc: gpd.GeoDataFrame, filterlayer: str, group_stacked: bool
    ):
        """_summary_

        Parameters
        ----------
        dfc : gpd.GeoDataFrame
            _description_
        filterlayer : str
            _description_
        group_stacked : bool
            _description_

        Returns
        -------
        _type_
            _description_
        """
        dfs = dfc.copy()
        dfs_valid = dfs[dfs.in_use].copy()

        # Determine crossings for the filter layer
        df_filter = self.find_crossings_with_peilgebieden(
            filterlayer, group_stacked=group_stacked
        )
        df_filter = df_filter[df_filter.in_use].copy()

        # Look-up table for filter layer lines
        crossing_lines = (
            self.df_gpkg[filterlayer].copy().set_index("globalid", inplace=False)
        )

        # Find out which crossings can be filtered out.
        add_crossings = []
        for line_id, group in tqdm.tqdm(
            df_filter.groupby(filterlayer, sort=False),
            desc=f"Filter crossings along '{filterlayer}'",
        ):
            # Skip of we have only a single filter crossing
            if len(group) == 0:
                continue

            # Sort crossings by distance along each line
            crossing_line = crossing_lines.at[line_id, "geometry"]
            for row in group.itertuples():
                group.at[row.Index, "dist_along"] = crossing_line.project(row.geometry)
            group = group.sort_values("dist_along")

            matching_crossings = []
            for row in group.itertuples():
                dist0 = dfs_valid.distance(row.geometry)
                matching_crossings.append(
                    dfs_valid[
                        (dist0 < self.allowed_distance)
                        & (dfs_valid.peilgebieden == row.peilgebieden)
                    ].copy()
                )
            matching_crossings = pd.concat(matching_crossings, ignore_index=False)

            if len(matching_crossings) <= 1:
                continue

            # Set all matching crossings in_use to False
            dfs.loc[matching_crossings.index.values, "in_use"] = False

            # if line_id == "{E3F47D83-52DE-4978-8FC4-518152C4C8DD}":
            #     print(group)
            #     print(matching_crossings)

            if len(group) > 1:
                pfrom = group.peilgebied_from[
                    group.dist_along == group.dist_along.min()
                ].unique()
                pto = group.peilgebied_to[
                    group.dist_along == group.dist_along.max()
                ].unique()
                if len(pfrom) != 1 or len(pto) != 1:
                    print(
                        f"Cannot find a unique point for a group of points on filter line '{line_id}'"
                    )
                pfrom = pfrom[0]
                pto = pto[0]

                first_from = group.peilgebied_from.iat[0]
                last_to = group.peilgebied_to.iat[-1]

                # if the first peilgebied differs from the last, add a single crossing
                # connecting them.
                if first_from != last_to:
                    new_row = matching_crossings.iloc[0, :].copy()
                    n_areas, str_areas = self._classify_from_to_peilgebieden(pfrom, pto)
                    new_row["crossing_type"] = n_areas
                    new_row["peilgebieden"] = str_areas
                    new_row["peilgebied_from"] = first_from
                    new_row["peilgebied_to"] = last_to
                    new_row["match_composite"] = True
                    new_row["in_use"] = True
                    new_row["geometry"] = group.geometry.iat[0]
                    add_crossings.append(new_row)

        if len(add_crossings) > 0:
            add_crossings = gpd.GeoDataFrame(add_crossings, crs=dfs.crs)
            dfs = pd.concat([dfs, add_crossings], ignore_index=False, sort=False)
            dfs = dfs.sort_index().reset_index(drop=True)

        return df_filter, dfs

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def find_structures_at_crossings(
        self, dfc: gpd.GeoDataFrame, structurelayer: str
    ) -> gpd.GeoDataFrame:
        """_summary_

        Parameters
        ----------
        dfc : gpd.GeoDataFrame
            _description_
        structurelayer : str
            _description_

        Returns
        -------
        gpd.GeoDataFrame
            _description_

        Raises
        ------
        ValueError
            _description_
        """
        # Reference to the structure GeoDataFrame.
        df_structures = self.df_gpkg[structurelayer].copy()

        # Copy crossings and valid crossings.
        dfs = dfc.copy()
        dfs.insert(len(dfs.columns) - 1, structurelayer, None)
        # dfs.insert(len(dfs.columns) - 1, f"{structurelayer} n", 0)

        df_filter = dfc[dfc.in_use].copy()

        # Add structures to crossings.
        for structure in tqdm.tqdm(
            df_structures.itertuples(),
            total=len(df_structures),
            desc=f"Add structures of '{structurelayer}' to crossings",
            disable=self.disable_progress,
        ):
            # Find crossings near the structure
            buffered_structure = structure.geometry.buffer(self.search_radius_structure)
            idx = df_filter.sindex.query(buffered_structure, predicate="intersects")

            # If no crossings are near, continue with the following structure.
            if len(idx) == 0:
                # print(f"Warning: {structurelayer} '{structure.globalid}' heeft geen crossings in de buurt")
                continue

            # Determine if the spatial index results actually do intersect.
            df_subset = dfs.loc[df_filter.index[idx].values, :].copy()
            df_subset = df_subset[pd.isnull(df_subset[structurelayer])].copy()
            df_subset = df_subset[
                df_subset.geometry.intersects(buffered_structure)
            ].copy()

            # If no crossings are near, continue with the following structure.
            if len(df_subset) == 0:
                # print(f"Warning: {structurelayer} '{structure.globalid}' heeft geen crossings in de buurt")
                continue

            lbl = df_subset.distance(structure.geometry).idxmin()
            if not pd.isnull(dfs.at[lbl, structurelayer]):
                raise ValueError("Crossing heeft al een kunstwerk...")
            dfs.at[lbl, structurelayer] = structure.globalid

        return dfs

    @validate_call(config=dict(arbitrary_types_allowed=True))
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
        dfs["flip"] = pd.NA
        for row in tqdm.tqdm(
            dfs.itertuples(),
            total=len(dfs),
            desc="Correct free water flow",
            disable=self.disable_progress,
        ):
            if pd.isnull([row.streefpeil_from, row.streefpeil_to]).any() > 0:
                continue

            if row.streefpeil_from < row.streefpeil_to:
                # Keer peilen en peilgebieden om
                dfs.at[row.Index, "peilgebied_from"] = row.peilgebied_to
                dfs.at[row.Index, "peilgebied_to"] = row.peilgebied_from
                dfs.at[row.Index, "streefpeil_from"] = row.streefpeil_to
                dfs.at[row.Index, "streefpeil_to"] = row.streefpeil_from
                dfs.at[row.Index, "flip"] = "streefpeil"

        # Gemaal: water is pumped from low to high
        for row in tqdm.tqdm(
            dfs.itertuples(),
            total=len(dfs),
            desc="Correct pumped water flow",
            disable=self.disable_progress,
        ):
            if pd.isnull([row.streefpeil_from, row.streefpeil_to]).any() > 0:
                continue

            if row.streefpeil_from > row.streefpeil_to and not pd.isnull(row.gemaal):
                # Keer peilen en peilgebieden om
                dfs.at[row.Index, "peilgebied_from"] = row.peilgebied_to
                dfs.at[row.Index, "peilgebied_to"] = row.peilgebied_from
                dfs.at[row.Index, "streefpeil_from"] = row.streefpeil_to
                dfs.at[row.Index, "streefpeil_to"] = row.streefpeil_from
                if pd.isnull(row.flip):
                    dfs.at[row.Index, "flip"] = "gemaal"
                else:
                    dfs.at[row.Index, "flip"] = "streefpeil,gemaal"

        return dfs

    @validate_call(config=dict(arbitrary_types_allowed=True))
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
        dfs["agg1_group"] = pd.NA
        dfs["agg1_used"] = dfs.in_use.copy()

        peilgebieden = self.df_gpkg["peilgebied"].copy()
        peilgebieden["geometry"] = peilgebieden.centroid
        peilgebieden = peilgebieden.set_index("globalid")

        dfs_filter = dfs[
            dfs.in_use & ~pd.isnull(dfs.peilgebied_from) & ~pd.isnull(dfs.peilgebied_to)
        ].copy()
        for (pfrom, pto), group in tqdm.tqdm(
            dfs_filter.groupby(["peilgebied_from", "peilgebied_to"], sort=False),
            desc=f"Aggregate to single unique links",
            disable=self.disable_progress,
        ):
            dfs.loc[group.index, "agg1_group"] = f"{pfrom},{pto}"
            dfs.loc[group.index, "agg1_used"] = False
            lbl_choice = group.distance(peilgebieden.geometry.at[pto]).idxmin()
            dfs.at[lbl_choice, "agg1_used"] = True

        return dfs
