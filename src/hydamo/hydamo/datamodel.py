"""HyDAMO datamodel for ValidatieTool."""

import geopandas as gpd
import fiona
from typing import List, Dict, Literal
import re
import json
from shapely.geometry import LineString, MultiLineString, Point, Polygon, MultiPolygon
import numpy as np
from pathlib import Path
import warnings
import logging
from hydamo import geometry
from hydamo.styles import add_styles_to_geopackage

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

FIELD_TYPES_MAP_REV = fiona.schema.FIELD_TYPES_MAP_REV
FIELD_TYPES_MAP = fiona.schema.FIELD_TYPES_MAP
MODEL_CRS = "epsg:28992"
SCHEMAS_DIR = Path(__file__).parent.joinpath("data", "schemas")

GEOTYPE_MAPPING = {
    "LineString": LineString,
    "MultiLineString": MultiLineString,
    "Point": Point,
    "PointZ": Point,
    "Polygon": Polygon,
    "MultiPolygon": MultiPolygon,
}

DTYPE_MAPPING = {
    "string": "str",
    "integer": "int64",
    "date-time": "datetime",
    "number": "float",
}

default_properties = {"id": None, "dtype": "str", "required": False, "unique": False}


def map_definition(definition: Dict) -> List:
    """


    Parameters
    ----------
    definition : Dict
        HyDAMO defintion as specified in the HyDAMO JSON specification.

    Returns
    -------
    List
        Validation schema for the HyDAMO class.

    """

    # start with an empty list to populate
    result = []

    for k, v in definition.items():
        # convert geometry if shape
        if k == "shape":
            properties = {"id": "geometry"}
            dtype = v["type"]
            if not isinstance(dtype, list):
                dtype = [dtype]
            properties["dtype"] = dtype
            properties["required"] = True
            result.append(properties)

        # set properties if not shape
        else:
            properties = default_properties.copy()
            properties["id"] = k
            if "type" in v.keys():
                properties["dtype"] = DTYPE_MAPPING[v["type"]]
                if "format" in v.keys():
                    properties["dtype"] = DTYPE_MAPPING[v["format"]]

            # set required
            if "minItems" in v.keys():
                if v["minItems"] == 1:
                    properties["required"] = True

            # set unique
            if "uniqueItems" in v.keys():
                properties["unique"] = v["uniqueItems"]

            # set domain
            if "enum" in v.keys():
                properties["domain"] = [{"value": i} for i in v["enum"]]
            result.append(properties)
    return result


class ExtendedGeoDataFrame(gpd.GeoDataFrame):
    """A GeoPandas GeoDataFrame with extended properties and methods."""

    _metadata = ["required_columns", "geotype", "layer_name"] + gpd.GeoDataFrame._metadata

    def __init__(
        self,
        validation_schema: Dict,
        geotype: Literal[list(GEOTYPE_MAPPING.keys())],
        layer_name: str = "",
        required_columns: List = [],
        logger=logging,
        *args,
        **kwargs,
    ):

        # Check type
        required_columns = [i.lower() for i in required_columns]

        # Add required columns to column list
        # if "columns" in kwargs.keys():
        #     kwargs["columns"] += required_columns
        # else:
        kwargs["columns"] = required_columns

        super(ExtendedGeoDataFrame, self).__init__(*args, **kwargs)

        self.validation_schema = validation_schema
        self.required_columns = required_columns
        self.layer_name = layer_name
        self.geotype = geotype

        if "geometry" in self.columns:
            self.set_geometry("geometry", inplace=True)
            self.crs = MODEL_CRS
            if not "geometry" in self.required_columns:
                self.required_columns += ["geometry"]

    def _check_columns(self, gdf):
        """
        Check presence of columns in GeoDataFrame
        """
        present_columns = gdf.columns.tolist()
        for column in self.required_columns:
            if column not in present_columns:
                raise KeyError(
                    'Column "{}" not found. Got {}, Expected at least {}'.format(
                        column,
                        ", ".join(present_columns),
                        ", ".join(self.required_columns),
                    )
                )

    def _check_geotype(self):
        """
        Check geometry type
        """
        if self.geotype:
            if not all(
                any(isinstance(geo, GEOTYPE_MAPPING[i]) for i in self.geotype)
                for geo in self.geometry
            ):
                raise TypeError(
                    'Geometry-type "{}" required in layer "{}". The input feature-file has geometry type(s) {}.'.format(
                        re.findall("([A-Z].*)'", repr(self.geotype))[0],
                        self.layer_name,
                        self.geometry.type.unique().tolist(),
                    )
                )

    def _get_schema(self):
        """Return fiona schema dict from validation_schema."""
        properties = {
            i["id"]: i["dtype"] for i in self.validation_schema if i["id"] != "geometry"
        }
        # properties = {k: (v if v != "datetime" else "str") for k, v in properties}
        geometry = next(
            (i["dtype"] for i in self.validation_schema if i["id"] == "geometry"),
            None,
        )
        return dict(properties=properties, geometry=geometry)

    def set_data(self, gdf, layer="", index_col=None, check_columns=True, check_geotype=True, extra_attributes={}):
        """


        Parameters
        ----------
        gdf : GeoDataFrame
            GeoDataFrame with a HyDAMO object-layer
        index_col : str, optional
            Column to be used as index. The default is None.
        check_columns : bool, optional
            Check if all required columns are present in the GeoDataFrame.
            The default is True.
        check_geotype : bool, optional
            Check if the geometry is of the required type. The default is True.

        Returns
        -------
        None.

        """

        if not self.empty:
            self.delete_all()

        # reproject to crs if necessary
        if (gdf.crs is not None) and ("geometry" in self.required_columns):
            if f"epsg:{gdf.crs.to_epsg()}" == MODEL_CRS:
                gdf.set_crs(MODEL_CRS, inplace=True, allow_override=True)
            else:
                gdf.to_crs(MODEL_CRS, inplace=True)

        # Check columns
        gdf.columns = [i.lower() for i in gdf.columns]
        if check_columns:
            self._check_columns(gdf)

        # Copy content
        for col, values in gdf.items():
            self[col] = values.values

        if index_col is None:
            self.index = gdf.index
            self.index.name = gdf.index.name

        else:
            self.index = gdf[index_col]
            self.index.name = index_col

        # Check geometry types
        if check_geotype:
            self._check_geotype()
        
        # Set extra attribute-values
        for k,v in extra_attributes.items():
            if k not in self.columns:
                self[k] = v

    def delete_all(self):
        """
        Empty the dataframe
        """
        if not self.empty:
            self.iloc[:, 0] = np.nan
            self.dropna(inplace=True)

    def snap_to_branch(self, branches, snap_method, maxdist=5):
        """


        Parameters
        ----------
        branches : GeoDataFrame
            GeoDataFrame with branches
        snap_method : str
            Options for snapping
        maxdist : float, optional
            The maximal distance for snapping. The default is 5.

        Returns
        -------
        None.

        """

        """Snap the geometries to the branch."""
        geometry.find_nearest_branch(
            branches=branches, geometries=self, method=snap_method, maxdist=maxdist
        )


class HyDAMO:
    """Definition of the HyDAMO datamodel."""

    def __init__(
        self,
        version: str = "2.2",
        schemas_path: Path = SCHEMAS_DIR,
        ignored_layers: List = [
            "afvoeraanvoergebied",
            "imwa_geoobject",
            "leggerwatersysteem",
            "leggerwaterveiligheid",
            "waterbeheergebied",
        ],
    ):
        self.version = version
        self.schema_json = schemas_path.joinpath(f"HyDAMO_{version}.json")
        self.layers = []
        self.ignored_layers = ignored_layers

        self.init_datamodel()

    @property
    def data_layers(self):
        return [layer for layer in self.layers if not getattr(self, layer).empty]

    def init_datamodel(self):
        """Initialize DataModel from self.schemas_path."""
        self.validation_schemas: dict = {}

        # read schema as dict
        with open(self.schema_json) as src:
            schema = json.load(src)
            hydamo_layers = [
                Path(i["$ref"]).name for i in schema["properties"]["HyDAMO"]["anyOf"]
            ]
            self.layers = [i for i in hydamo_layers if not i in self.ignored_layers]

        for hydamo_layer in self.layers:
            definition = schema["definitions"][hydamo_layer]["properties"]
            layer_schema = map_definition(definition)
            self.validation_schemas[hydamo_layer] = layer_schema

            # add layer to data_model
            geotype = next(
                (i["dtype"] for i in layer_schema if i["id"] == "geometry"), None
            )

            required_columns = [
                i["id"]
                for i in [i for i in layer_schema if "required" in i.keys()]
                if i["required"]
            ]

            setattr(
                self,
                hydamo_layer,
                ExtendedGeoDataFrame(
                    validation_schema=layer_schema,
                    layer_name=hydamo_layer,
                    geotype=geotype,
                    required_columns=required_columns,
                ),
            )

    def get(self, layer: str, global_id: str):
        """
        Get a DataFrame row (feature) providing a layer an global_id.

        Parameters
        ----------
        layer : str
            DESCRIPTION.
        global_id : str
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        return getattr(self,layer).set_index("globalid").loc[global_id]

    def set_data(
        self, gdf, layer, index_col=None, check_columns=True, check_geotype=True,
        extra_values={}
    ):
        """


        Parameters
        ----------
        gdf : GeoDataFrame
            GeoDataFrame with a HyDAMO object-layer
        layer : TYPE
            HyDAMO layer to be set
        index_col : str, optional
            Column to be used as index. The default is None.
        check_columns : bool, optional
            Check if all required columns are present in the GeoDataFrame.
            The default is True.
        check_geotype : bool, optional
            Check if the geometry is of the required type. The default is True.

        Returns
        -------
        None.

        """

        getattr(self, layer).set_data(
            gdf,
            index_col=index_col,
            check_columns=check_columns,
            check_geotype=check_geotype,
            extra_values={}
        )

    def to_geopackage(self, file_path, use_schema=True):
        """

        Parameters
        ----------
        file_path : path-string
            Path-string where the file should be written to
        use_schema : bool, optional
            Use the schema to specify column-properties The default is True.

        Returns
        -------
        None.

        """

        for layer in self.layers:
            gdf = getattr(self, layer).copy()
            if not gdf.empty:
                if use_schema:
                    # match fiona layer schema keys with gdf.columns
                    schema = getattr(self, layer)._get_schema()
                    schema_cols = list(schema["properties"].keys()) + ["geometry"]
                    drop_cols = [i for i in gdf.columns if i not in schema_cols]
                    gdf.drop(columns=drop_cols, inplace=True)

                    schema["properties"] = {
                        k: v
                        for k, v in schema["properties"].items()
                        if k in gdf.columns
                    }

                    # write gdf to geopackage, including schema
                    if gdf.index.name in gdf.columns:
                        gdf.reset_index(drop=True, inplace=True)
                    gdf.to_file(
                        file_path,
                        layer=layer,
                        driver="GPKG",
                        schema=schema,
                    )
                else:
                    # write gdf to geopackage as is
                    if gdf.index.name in gdf.columns:
                        gdf = gdf.reset_index(drop=True).copy()
                    gdf.to_file(file_path, layer=layer, driver="GPKG")

        add_styles_to_geopackage(file_path)
                    
    @classmethod
    def from_geopackage(cls, file_path, check_columns=True, check_geotype=True):
        """
        Initializes HyDAMO class from GeoPackage

        Parameters
        ----------
        file_path : path-string
            Path-string to the hydamo GeoPackage
        check_columns : bool, optional
            Check if all required columns are present in the GeoDataFrame.
            The default is True.
        check_geotype : bool, optional
            Check if the geometry is of the required type. The default is True.

        Returns
        -------
        hydamo : HyDAMO
            HyDAMO object initialized with content of GeoPackage

        """
        hydamo = cls()
        for layer in fiona.listlayers(file_path):
            if layer in hydamo.layers:
                hydamo_layer = getattr(hydamo, layer)
                hydamo_layer.set_data(
                    gpd.read_file(file_path, layer=layer),
                    check_columns=check_columns,
                    check_geotype=check_geotype
                    )
        return hydamo
