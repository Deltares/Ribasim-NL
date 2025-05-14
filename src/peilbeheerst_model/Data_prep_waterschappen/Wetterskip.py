import geopandas as gpd

from ribasim_nl import CloudStorage

cloud = CloudStorage()

data_delivery = cloud.joinpath(
    "WetterskipFryslan",
    "aangeleverd",
    "Na_levering",
    "20250404_nieuwe_schematisatie",
    "beheerregister_WF.gdb",
    "beheerregister_WF.gdb",
)

cloud.synchronize(filepaths=[data_delivery])


def read_gpkg_layers(gpkg_path, engine="fiona", print_var=False):
    data = {}
    layers = gpd.list_layers(gpkg_path)
    for layer in layers.name:
        if print_var:
            print(layer)
        data_temp = gpd.read_file(gpkg_path, layer=layer, engine=engine)
        data[layer] = data_temp

    return data


Wetterskip = read_gpkg_layers(gpkg_path=data_delivery)
