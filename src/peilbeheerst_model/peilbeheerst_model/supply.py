"""
Labelling Ribasim-basins as "aanvoer"-basins, i.e., water can (and should) be supplied to these basins during shortages.

Labelling is executed based on Ribasim-model (basin nodes) and geo-files with "aanvoergebieden."
If necessary, labels can be modified manually.

Author: Gijs G. Hendrickx
"""

import abc
import os
import typing

import geopandas as gpd
import pandas as pd
import ribasim
from mypy.types_utils import AnyType


def _load_model(model: str | ribasim.Model) -> ribasim.Model:
    """Load Ribasim model.

    :param model: file/path to Ribasim model
    :type model: str

    :return: Ribasim model
    :rtype: ribasim.model.Model
    """
    if isinstance(model, ribasim.Model):
        return model

    if not model.endswith(".toml"):
        model += f"{os.sep}ribasim.toml"

    return ribasim.Model(filepath=model)


# TODO: Add the `special_load_geometry()`-function as a built-in feature via `kwargs`?
def _load_geometry(geometry: str | gpd.GeoDataFrame, **kwargs) -> gpd.GeoDataFrame:
    """Load geometry data of 'aanvoergebieden.'

    In case the 'aanvoergebieden' are not specified by a single file/layer, use the
    `supply_label.special_load_geometry()`-function to load the geometry and provide it as a
    geometry to the `SupplyBasin`-object (as `geometry`).

    :param geometry: file/path to geometry data

    :key layer: geopackage layer, defaults to None

    :type geometry: str, geopandas.GeoDataFrame
    :type layer: str, optional

    :return: geometry data
    :rtype: geopandas.GeoDataFrame
    """
    if isinstance(geometry, gpd.GeoDataFrame):
        return geometry.copy(deep=True)  # type: ignore

    if isinstance(geometry, str):
        if geometry.endswith(".shp"):
            data = gpd.read_file(geometry)
        elif geometry.endswith(".gpkg") or geometry.endswith(".gdb"):
            layer: str = kwargs.get("layer")
            data = gpd.read_file(geometry, layer=layer)
        else:
            msg = f"File-type not implemented: {geometry}"
            raise NotImplementedError(msg)
        return data

    msg = f"Unknown type for `geometry`: {type(geometry)}"
    raise TypeError(msg)


class SupplyBasin:
    """Labelling of Ribasim's basin nodes as 'aanvoergebieden' based on geometry data."""

    def __init__(self, model: str | ribasim.Model, geometry: str | gpd.GeoDataFrame, **kwargs):
        """Initiate object.

        :param model: Ribasim model, or file/path to a Ribasim model
        :param geometry: geometry data of 'aanvoergebieden', or file/path to this geometry data
        :param kwargs: optional arguments, potentially required to load/read geometry data
            see `._load_geometry()`

        :type model: str, ribasim.Model
        :type geometry: str, geopandas.GeoDataFrame
        :type kwargs: optional
        """
        self._model = _load_model(model)
        self._geometry = _load_geometry(geometry, **kwargs)  # type: ignore

    def __call__(self) -> pd.DataFrame:
        """Shortcut to execute labelling basins as 'aanvoergebied.'

        :return: basin-data including meta-label 'aanvoer' (bool)
        :rtype: geopandas.GeoDataFrame
        """
        return self.exec()

    def exec(self) -> pd.DataFrame:
        """Execute labelling basins as 'aanvoergebied'.

        :return: basin-data including meta-label 'aanvoer' (bool)
        :rtype: pandas.DataFrame
        """
        # determine which nodes fall within 'aanvoergebieden'
        # print(self.geometry)
        basin_nodes = self.basin_nodes
        nodes_in_polygons = gpd.sjoin(basin_nodes, self.geometry[["geometry"]], how="left")
        # with pd.option_context('display.max_rows', None):
        #     print(nodes_in_polygons)

        # check for double-polygon matching
        if len(nodes_in_polygons) != len(basin_nodes):
            msg = "Basin nodes are possibly double assigned to 'aanvoergebieden', please check the source data."
            raise ValueError(msg)

        # mark basins as 'aanvoergebieden'
        nodes_in_polygons["meta_aanvoer"] = nodes_in_polygons["index_right"].notna()
        # with pd.option_context('display.max_rows', None):
        #     print(nodes_in_polygons[["meta_aanvoer", "index_right"]])
        nodes_in_polygons.reset_index(inplace=True)

        # update basin areas
        basin_areas = self.basin_areas

        basin_areas = basin_areas.merge(nodes_in_polygons[["node_id", "meta_aanvoer"]], how="left", on="node_id")
        basin_areas.dropna(subset=["meta_aanvoer"], inplace=True)

        # updated data: basin areas
        self.basin_areas = basin_areas.copy(deep=True)
        return basin_areas

    @property
    def model(self) -> ribasim.Model:
        """Ribasim model."""
        return self._model

    @property
    def geometry(self) -> gpd.GeoDataFrame:
        """Geometry data."""
        return self._geometry

    @property
    def basin_nodes(self) -> gpd.GeoDataFrame:
        """Ribasim basin nodes."""
        return self.model.basin.node.df.copy(deep=True)

    @property
    def basin_areas(self) -> pd.DataFrame:
        """Ribasim basin areas."""
        return self.model.basin.area.df.copy(deep=True)

    @basin_areas.setter
    def basin_areas(self, df: gpd.GeoDataFrame) -> None:
        """Set Ribasim basin areas.

        :param df: data with basin areas
        :type df: geopandas.GeoDataFrame
        """
        if not isinstance(df, gpd.GeoDataFrame):
            msg = f"Ribasim basin areas must be a `geopandas.GeoDataFrame`; {type(df)} given."
            raise TypeError(msg)

        req_cols = "node_id", "meta_aanvoer"
        if not all(c in df.columns for c in req_cols):
            msg = f"Missing required columns: {req_cols}"
            raise ValueError(msg)

        self.model.basin.area.df = df.copy(deep=True)

    @staticmethod
    def __df_check(df: pd.DataFrame) -> None:
        """Check whether dataframe has been prepared before manually changing it.

        :param df: dataframe
        :type df: pandas.DataFrame
        """
        if "meta_aanvoer" not in df.columns:
            msg = 'DataFrame is missing the "meta_aanvoer"-column: Run it through `SupplyBasin.exec()` first.'
            raise KeyError(msg)

    def __modify_aanvoer(self, bool_supply: bool, *node_id: int) -> pd.DataFrame:
        """Modify basins' 'aanvoer'-labelling based on their node-ID.

        :param bool_supply: value of the 'aanvoer'-label
        :param node_id: basin node-ID

        :type bool_supply: bool
        :type node_id: int

        :return: basin-data with modified meta-label 'aanvoer'
        :rtype: pandas.DataFrame
        """
        # get basin areas
        basin_areas = self.basin_areas
        self.__df_check(basin_areas)

        # modify basin nodes 'aanvoer'-label
        for n in node_id:
            if n not in basin_areas["node_id"].values:
                msg = f"Node-ID {n} not found as basin node-ID."
                raise ValueError(msg)

            basin_areas.loc[basin_areas["meta_node_id"] == n, "meta_aanvoer"] = bool_supply
            print(f'Basin {n} "meta_aanvoer" set to {bool_supply}')

        # updated data: basin nodes
        self.basin_areas = basin_areas.copy(deep=True)
        return basin_areas

    def set_aanvoer_on(self, *node_id: int) -> pd.DataFrame:
        """Set basin as 'aanvoergebieden' based on its node-ID.

        :param node_id: basin node-ID
        :type node_id: int

        :return: basin-data with modified meta-label 'aanvoer'
        :rtype: pandas.DataFrame
        """
        return self.__modify_aanvoer(True, *node_id)

    def set_aanvoer_off(self, *node_id: int) -> pd.DataFrame:
        """Set basin as 'afvoergebieden' only based on its node-ID.

        :param node_id: basin node-ID
        :type node_id: int

        :return: basin-data with modified meta-label 'aanvoer'
        :rtype: pandas.DataFrame
        """
        return self.__modify_aanvoer(False, *node_id)


class SupplyWork(abc.ABC):
    """Labelling of Ribasim's pump/outlet nodes as 'aanvoer' or 'afvoer' based on basin labels."""

    _node_type: str

    def __init__(self, model: str | ribasim.Model):
        """Initiate object.

        :param model: Ribasim model, or file/path to a Ribasim model
        :type model: str, ribasim.Model
        """
        self._model = self._load_model(model)

    def __call__(self, **kwargs) -> pd.DataFrame:
        """Shortcut to execute labelling of 'kunstwerken' as 'aanvoer'.

        See `.exec()` for more information.

        :return: 'kunstwerk'-statics
        :rtype: pandas.DataFrame
        """
        return self.exec(**kwargs)  # type: ignore

    def _load_model(self, model: str | ribasim.Model) -> ribasim.Model:
        """Load and check Ribasim model.

        :param model: file/path to Ribasim model
        :type model: str

        :return: Ribasim model
        :rtype: ribasim.model.Model
        """
        _model = _load_model(model)

        if "meta_aanvoer" not in _model.basin.area.df.columns:
            msg = (
                f'Ribasim model has not been assigned its "aanvoergebieden"; '
                f"this must be done before using `{self.__class__.__name__}`."
            )
            raise ValueError(msg)

        return _model

    @property
    def model(self) -> ribasim.Model:
        """Ribasim model."""
        return self._model

    @property
    def basin_areas(self) -> gpd.GeoDataFrame:
        """Ribasim basin areas."""
        return self.model.basin.area.df.copy(deep=True)

    @property
    def basin_states(self) -> pd.DataFrame:
        """Ribasim basin states."""
        return self.model.basin.state.df.copy(deep=True)

    @property
    def level_boundaries(self) -> gpd.GeoDataFrame:
        """Ribasim level boundary nodes."""
        return self.model.level_boundary.node.df.copy(deep=True)

    def get_statics(self) -> pd.DataFrame:
        """Get Ribasim Static-data."""
        return getattr(self.model, self._node_type.lower()).static.df.copy(deep=True)

    @abc.abstractmethod
    def set_statics(self, df: pd.DataFrame) -> None:
        """Set Ribasim Static-data."""
        if not isinstance(df, pd.DataFrame):
            msg = f"Ribasim static-data must be a `pandas.DataFrame`; {type(df)} given."
            raise TypeError(msg)

        req_cols = "meta_to_node_id", "meta_aanvoer"
        if not all(c in df.columns for c in req_cols):
            msg = f"Missing required columns: {req_cols}"
            raise ValueError(msg)

    def exec(self, **kwargs) -> pd.DataFrame:
        """Set 'meta_aanvoer' of 'kunstwerk' based on connected nodes.

        :key overruling_enabled: in case a basin can be supplied directly from the 'hoofdwatersysteem', other supply-
            routes are "overruled", i.e., removed, defaults to True
        :type overruling_enabled: bool, optional

        :return: 'kunstwerk'-statics
        :rtype: pandas.DataFrame
        """
        # optional arguments
        overruling_enabled: bool = kwargs.get("overruling_enabled", True)

        # getting model data
        basin_areas = self.basin_areas.set_index("node_id")
        basin_states = self.basin_states.set_index("node_id")
        statics = self.get_statics()
        boundaries = self.level_boundaries

        # update statics based on 'meta_to_node_id'-label
        statics["meta_aanvoer"] = statics["meta_to_node_id"].isin(basin_areas[basin_areas["meta_aanvoer"]].index)
        self.set_statics(statics)
        # TODO: Add option to use links instead of meta_to_node_id?

        # overrule statics based on 'meta_from_node_id'-label
        if overruling_enabled:
            remove_nodes = set()
            # collect all nodes that are considered part of the 'hoofdwatersysteem'
            main_water_system_node_ids = (
                basin_states[basin_states["meta_categorie"] == "hoofdwater"].index.to_list()
                + boundaries.index.to_list()
            )
            # only consider basins and works that are considered for the 'aanvoer'-situation
            basin_sel = basin_areas[basin_areas["meta_aanvoer"]].index
            works_sel = statics.loc[statics["meta_aanvoer"], ["node_id", "meta_from_node_id", "meta_to_node_id"]]
            # initiate working variables (bi: basin node-ID)
            basin_main = {bi: [] for bi in basin_sel}
            basin_sub = {bi: [] for bi in basin_sel}
            # if 'meta_from_node_id' is labelled as part of the 'hoofdwatersysteem', add the 'kunstwerk'-ID to the basin
            # the water is going to, i.e., 'meta_to_node_id'
            for _, *row in works_sel.iterrows():
                i, f, t = row[0]  # i: 'kunstwerk' node-ID, f: from node-ID, t: to node-ID
                if f in main_water_system_node_ids and t not in main_water_system_node_ids:
                    basin_main[t].append(i)
                else:
                    basin_sub[t].append(i)
                # remove 'kunstwerk'-IDs that are flowing TO the 'hoofdwatersysteem'
                if t in main_water_system_node_ids and f not in main_water_system_node_ids:
                    remove_nodes.add(i)
            # collect 'kunstwerk'-IDs that should no longer be considered for the 'aanvoer'-situation
            for k, v in basin_main.items():
                if v:
                    remove_nodes.update(basin_sub[k])
            # disable 'meta_aanvoer' for collected 'kunstwerk'-IDs
            self.set_aanvoer_off(*remove_nodes)

        # update model data: 'kunstwerk' statics
        return self.get_statics()

    def __df_check(self, df: pd.DataFrame) -> None:
        """Check whether dataframe has been prepared before manually changing it.

        :param df: dataframe
        :type df: pandas.DataFrame
        """
        if "meta_aanvoer" not in df.columns:
            msg = (
                f'DataFrame is missing the "meta_aanvoer"-column: '
                f"Run it through `{self.__class__.__name__}.exec()` first."
            )
            raise KeyError(msg)

    def __modify_aanvoer(self, bool_supply: bool, *node_id: int) -> pd.DataFrame:
        """Modify 'kunstwerk' 'aanvoer'-labelling based on their node-ID.

        :param bool_supply: value of the 'aanvoer'-label
        :param node_id: 'kunstwerk' node-ID

        :type bool_supply: bool
        :type node_id: int

        :return: 'kunstwerk'-data with modified meta-label 'aanvoer'
        :rtype: pandas.DataFrame
        """
        # get statics
        statics = self.get_statics()
        self.__df_check(statics)

        # modify statics 'aanvoer'-label
        for n in node_id:
            if n not in statics["node_id"].values:
                msg = f"Node-ID {n} not found as {self._node_type.lower()} node-ID."
                raise ValueError(msg)

            statics.loc[statics["node_id"] == n, "meta_aanvoer"] = bool_supply
            print(f'{self._node_type} {n} "meta_aanvoer" set to {bool_supply}')

        # updated data: statics
        self.set_statics(statics)
        return statics

    def set_aanvoer_on(self, *node_id: int) -> pd.DataFrame:
        """Set 'kunstwerk' as 'aanvoer' based on its node-ID.

        :param node_id: 'kunstwerk' node-ID
        :type node_id: int

        :return: 'kunstwerk'-data with modified meta-label 'aanvoer'
        :rtype: pandas.DataFrame
        """
        return self.__modify_aanvoer(True, *node_id)

    def set_aanvoer_off(self, *node_id: int) -> pd.DataFrame:
        """Set 'kunstwerk' as 'afvoer'-only based on its node-ID.

        :param node_id: 'kunstwerk' node-ID
        :type node_id: int

        :return: 'kunstwerk'-data with modified meta-label 'aanvoer'
        :rtype: pandas.DataFrame
        """
        return self.__modify_aanvoer(False, *node_id)


class SupplyOutlet(SupplyWork):
    """Labelling of Ribasim's outlet nodes as 'aanvoer' or 'afvoer' based on basin labels."""

    _node_type = "Outlet"

    def set_statics(self, df: pd.DataFrame) -> None:
        """Set Ribasim outlets' Static-data.

        :param df: data with outlet statics
        :type df: pandas.DataFrame
        """
        super().set_statics(df)
        self.model.outlet.static.df = df.copy(deep=True)


class SupplyPump(SupplyWork):
    """Labelling of Ribasim's pump nodes as 'aanvoer' or 'afvoer' based on basin labels."""

    _node_type = "Pump"

    def set_statics(self, df: pd.DataFrame) -> None:
        """Set Ribasim pumps' Static-data.

        :param df: data with pump statics
        :type df: pandas.DataFrame
        """
        super().set_statics(df)
        self.model.pump.static.df = df.copy(deep=True)


def special_load_geometry(f_geometry: str, method: str, **kwargs) -> gpd.GeoDataFrame:
    """Loading geometry data in a non-straightforward way.

    Included special methods are:
     -  'extract':
        Extract 'aanvoer'-geometries from within a single geometry-file/-layer. This reflects the occurrence of
        labelling geometries as 'aanvoer' within a single `geopandas.GeoDataFrame`, where one column provides the
        information on this labelling. This method requires the optional argument `key`, which gives the key/column-name
        of the `geopandas.GeoDataFrame`-column in which the 'aanvoer'-flag is stored (optionally provided by a specific
        value using the optional argument `value`).

     -  'inverse':
        Subtract 'afvoer'-only geometries from total set of geometries to retrieve the 'aanvoer'-only geometries. The
        'inverse'-method requires the definition of at least one additional layer or file, and the total set of
        geometries (i.e., both 'aanvoer' and 'afvoer') is provided in the `f_geometry`-argument. In case of a *.gpkg-/
        *.gdb-file, the main/total geometry is given by the first layer in the `layers`-argument [optional]. Otherwise,
        additional files must be provided using the `extra_files`-argument [optional]. These additional files (or non-
        first layers) are subtracted from the main/total geometry.

     -  'merge':
        Merge multiple geometries to create the overall 'aanvoer'-geometry. Note that the `f_geometry` still requires a
        geometry file and additional files are provided using the `extra_files` argument [optional], or by providing
        multiple layers using the `layers`-argument [optional].

    :param f_geometry: geometry file
    :param method: loading method
        options: {'extract', 'inverse', 'merge'}

    :key export: export 'aanvoer'-geometry after modifications, defaults to False
    :key export_directory: directory to export the 'aanvoer'-geometry to, defaults to `f_geometry`'s directory
    :key export_filename: filename of the 'aanvoer'-geometry, defaults to 'aanvoer_mod.shp'
    :key extra_files: additional geometry files, defaults to None
        [relevant for `method`={'inverse', 'merge'} with `f_geometry`={'*.shp'}]
    :key key: key/column-label marking 'aanvoer'-geometries, defaults to None
        [required for `method`={'extract'}]
    :key layer: geometry layer from a *.gpkg-/*.gdb-file, defaults to None
        [relevant for `method`={'extract'} with `f_geometry`={'*.gpkg', '*.gdb'}]
    :key layers: geometry layers from a *.gpkg-/*.gdb-file, defaults to None
        [relevant for `method`={'inverse', 'merge'} with `f_geometry`={'*.gpkg', '*.gdb'}]
    :key value: value of 'aanvoer'-geometries, defaults to True
        [relevant for `method`={'extract'}]

    :type f_geometry: str
    :type method: str
    :type export: bool, optional
    :type export_directory: str, optional
    :type export_filename: str, optional
    :type extra_files: sequence[str], optional
    :type key: str, optional
    :type layer: str, optional
    :type layers: sequence[str], optional
    :type value: any, optional

    :return: 'aanvoer'-geometries
    :rtype: geopandas.GeoDataFrame

    :raise NotImplementedError: if `method` is not implemented
    :raise NotImplementedError: if file-extension is not implemented
    :raise ValueError: if additional *.shp-files are required but not provided
    :raise ValueError: if the provided number of files exceeds the maximum allowed for the method
    :raise ValueError: if no layers of the *.gpkg-/*.gdb-file are provided
    :raise ValueError: if not enough layers of the *.gpkg-/*.gdb-file are provided
    """
    # optional arguments
    export_modified_geo_data: bool = kwargs.get("export", False)
    export_directory: str = kwargs.get("export_directory")
    export_filename: str = kwargs.get("export_filename", "aanvoer_mod.shp")
    kw_extra_files: typing.Sequence[str] = kwargs.get("extra_files")
    kw_key: str = kwargs.get("key")
    kw_layer: str = kwargs.get("layer")
    kw_layers: typing.Sequence[str] = kwargs.get("layers")
    kw_value: AnyType = kwargs.get("value", True)

    def _load_multiple_geometries(
        file: str, extra_files: typing.Sequence[str] = None, layers: typing.Sequence[str] = None, max_files: int = None
    ) -> list[gpd.GeoDataFrame]:
        """Load multiple geometries.

        :param file: geometry file
        :param extra_files: additional geometry files, defaults to None
        :param layers: geometry layers from a *.gpkg-/*.gdb-file, defaults to None
        :param max_files: set a maximum number of files to be loaded, defaults to None

        :type file: str
        :type extra_files: sequence[str], optional
        :type layers: sequence[str], optional
        :type max_files: int, optional

        :return: list of geometry-data
        :rtype: list[geopandas.GeoDataFrame]

        :raise NotImplementedError: if file-extension is not implemented
        :raise ValueError: if additional *.shp-files are required but not provided
        :raise ValueError: if the provided number of files exceeds the maximum allowed for the method
        :raise ValueError: if no layers of the *.gpkg-/*.gdb-file are provided
        :raise ValueError: if not enough layers of the *.gpkg-/*.gdb-file are provided
        """
        if file.endswith(".shp"):
            if extra_files is None:
                msg = "Additional *.shp-files are required"
                if max_files is not None:
                    msg += f" (max. {max_files - 1})"
                raise ValueError(msg)
            if max_files is not None and len(extra_files) > max_files - 1:
                msg = (
                    f"Number of files exceeds max. number of files: "
                    f"{len(extra_files) + 1} > {max_files} "
                    f"(`f_geometry` counts as the first file)."
                )
                raise ValueError(msg)

            geometries = [gpd.read_file(file)] + [gpd.read_file(f) for f in extra_files]

        elif file.endswith(".gpkg") or file.endswith(".gdb"):
            if layers is None:
                msg = "Geopackage-layers not specified;"
                if max_files is not None:
                    msg += f" {max_files} layers required."
                raise ValueError(msg)
            if max_files is not None and len(layers) != max_files:
                msg = (
                    f"A *.gpkg-file requires the definition of {max_files} layer names: "
                    f"`layers={layers}` ({len(layers)})"
                )
                raise ValueError(msg)

            geometries = [gpd.read_file(file, layer=m) for m in layers]

        else:
            msg = f"File-type not supported: {file}"
            raise NotImplementedError(msg)

        return geometries

    # TODO: Allow multiple 'afvoer'-geometries
    def _inverse_geometry(total: gpd.GeoDataFrame, *afvoer: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Extract 'aanvoer'-geometries as the inverse of 'afvoer'-geometries.

        :param total: total geometry
        :param afvoer: 'afvoer'-geometry

        :type total: geopandas.GeoDataFrame
        :type afvoer: geopandas.GeoDataFrame

        :return: 'aanvoer'-geometry
        :rtype: geopandas.GeoDataFrame
        """
        out = total["geometry"].union_all()
        for g in afvoer:
            union = g["geometry"].union_all()
            out = out.difference(union)
        return gpd.GeoDataFrame({"label": "aanvoergebied", "geometry": out}, index=[0], crs="EPSG:28992")

    # TODO: Merging of geometries is not yet tested properly
    def _merge_geometry(*geometry: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Merge the 'aanvoergebieden' as specified by multiple files/layers/etc.

        :param geometry: 'aanvoer'-geometry
        :type geometry: geopandas.GeoDataFrame

        :return: 'aanvoer'-geometry
        :rtype: geopandas.GeoDataFrame
        """
        out = gpd.GeoDataFrame(pd.concat(geometry, ignore_index=True))
        return out

    def _extract_geometry(file: str, key: str, value: AnyType = True, layer: str = None) -> gpd.GeoDataFrame:
        """Extract 'aanvoergebieden' as labelled within the geopandas.GeoDataFrame.

        :param file: geometry-file
        :param key: key/column-label marking 'aanvoer'-geometries
        :param value: value of 'aanvoer'-geometries, defaults to True
        :param layer: geometry layer from a *.gpkg-/*.gdb-file, defaults to None

        :type file: str
        :type key: str
        :type value: any, optional
        :type layer: str, optional

        :return: 'aanvoergebied'-geometry
        :rtype: geopandas.GeoDataFrame

        :raise ValueError: if `key`-argument is `None`.
        """
        if key is None:
            msg = f"A key/column-name must be provided to extract the geometry; {key} given."
            raise ValueError(msg)

        gdf = _load_geometry(file, layer=layer)
        if isinstance(value, bool):
            out = gdf[gdf[key]]
        else:
            out = gdf[gdf[key] == value]
        return out

    # execute special load-method
    match method:
        case "inverse":
            geometry = _inverse_geometry(
                *_load_multiple_geometries(
                    file=str(f_geometry), extra_files=kw_extra_files, layers=kw_layers, max_files=2
                )
            )
        case "merge":
            geometry = _merge_geometry(
                *_load_multiple_geometries(file=str(f_geometry), extra_files=kw_extra_files, layers=kw_layers)
            )
        case "extract":
            geometry = _extract_geometry(str(f_geometry), kw_key, value=kw_value, layer=kw_layer)
        case _:
            msg = f"Unknown special method: {method}"
            raise NotImplementedError(msg)

    # export 'aanvoer'-geometry
    if export_modified_geo_data:
        export_directory = export_directory or os.path.dirname(f_geometry)
        _file = os.path.join(export_directory, export_filename)
        geometry.to_file(_file)
        print(f"Modified 'aanvoer'-geometry exported as {_file}")

    # return 'aanvoer'-geometry
    return geometry
