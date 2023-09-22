from dataclasses import dataclass
from hydamo.datamodel import HyDAMO
import geopandas as gpd
from geopandas.sindex import PyGEOSSTRTreeIndex
import pandas as pd

COLUMNS = ["globalid", "naam", "nen3610id", "layer", "geometry"]
LAYERS = ["gemaal", "stuw"]

def hydamo_to_index_gdf(hydamo):
    
    def row_to_string(row):
        string = ""
        if not pd.isna(row.naam):
            string = row.naam
        if not pd.isna(row.nen3610id):
            string += f" ({row.nen3610id})"

        return string

    gdfs = []
    for layer in LAYERS:
        hydamo_layer = getattr(hydamo, layer)
        hydamo_layer["layer"] = layer
        gdfs += [hydamo_layer[COLUMNS]]
        gdf = pd.concat(gdfs)
        gdf["search_string"] = gdf.apply((lambda row:row_to_string(row)), axis=1)
    return gdf

@dataclass
class Index:
    gdf: gpd.GeoDataFrame()
    sindex: PyGEOSSTRTreeIndex = None
    
    def __post_init__(self):
        self.sindex = self.gdf.sindex

    @classmethod
    def from_hydamo(cls, hydamo:HyDAMO):
        gdf = hydamo_to_index_gdf(hydamo)
        return cls(gdf=gdf)

    def get_layer(self, global_id):
        return self.gdf.set_index("globalid").loc[global_id].layer

    def search_records(self, search_term, len_limit=None, output_format="list"):
        matches = self.gdf[
            self.gdf['search_string'].str.contains(
                search_term,
                case=False,
                na=False
                )
            ]

        if len_limit:
            matches.loc[:, ["len"]] = matches["search_string"].str.len()
            matches = matches.nsmallest(len_limit, "len")
            matches.drop(columns=["len"], inplace=True)
        
        if output_format == "list":
            matches = list(
                matches[
                    ["globalid", "naam"]
                    ].to_dict(orient="index").values()
                )
        elif output_format == "geojson":
            bbox = list(matches.total_bounds)
            matches = matches._to_geo()
            matches["bbox"] = bbox

        return matches

if __name__ == '__main__':
    from hydamo.datamodel import HyDAMO
    gpkg = r"d:\repositories\lhm-ribasim\data\hydamo.gpkg"
    hydamo=HyDAMO.from_geopackage(gpkg)
    index = Index.from_hydamo(hydamo)
    
    search_list = index.search_records("gou")
    search_json = index.search_records("drie d", output_format="geojson")
    
    global_id = "7085d571-9b85-4982-9e50-f011d93b59b4"
    layer = index.get_layer(global_id)
