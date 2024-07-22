import pandas as pd
import geopandas as gpd
from pathlib import Path


def read_original_data(waterboard_dir, hydamo_format, object_naam, waterboard):
    print(f'- read data for {waterboard} - {object_naam}')
    table_hydamo = hydamo_format[f'{object_naam}_hydamo'].loc[waterboard]
    path_data_original = Path(waterboard_dir, "1_ontvangen_data", table_hydamo['1_ontvangen_data'])
    layer_data_original = table_hydamo['layer_ontvangen_data']
    if table_hydamo['1_ontvangen_data'] == "Niet aanwezig":
        return table_hydamo, None
    if layer_data_original == '' :
        data_original = gpd.read_file(path_data_original)
    else:
        data_original = gpd.read_file(path_data_original, layer=layer_data_original)
    if data_original.geometry.isnull().all():
        data_original = data_original.drop('geometry', axis=1)
    return table_hydamo, data_original


def translate_data_to_hydamo_format(hydamo_columns, data_original_gdf):
    df = pd.DataFrame()
    for col in range(5, len(hydamo_columns)):
        if hydamo_columns[col] == '':
            df[hydamo_columns.index[col]] = ''
        else:      
            df[hydamo_columns.index[col]] = data_original_gdf[hydamo_columns[col]]

    if 'geometry' not in data_original_gdf.columns:
        df['geometry'] = None
    else:
        df['geometry'] = data_original_gdf.geometry
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs=28992) 
    return gdf


def check_if_object_on_hydroobject(data_hydamo, hydroobject_buffered, groupby="globalid"):
    # kunstwerk spatial joinen met hydroobject
    data_hydamo = gpd.sjoin(
        data_hydamo, 
        hydroobject_buffered[['code','buffer']].rename(columns={"code":"code_hydroobject"}),
        how='left'
    ).groupby(groupby).first()
    data_hydamo['code_hydroobject'].fillna('niet op hydroobject', inplace=True)
    data_hydamo = data_hydamo.drop(columns=['index_right'])
    # # geometry weer terug naar punt voor buffer
    # data_hydamo = data_hydamo.set_geometry("geometry")
    return data_hydamo


def export_to_geopackage(waterboard_dir, data_hydamo, hydamo_format, waterboard, hydamo_object):
    if data_hydamo is None:
        return
    print(f'- export {waterboard} - {hydamo_object}')
    table_hydamo = hydamo_format[f'{hydamo_object}_hydamo'].loc[waterboard]
    path_data_hydamo = Path(waterboard_dir, "3_verwerkte_data", table_hydamo['3_verwerkte_data'])
    layer_data_hydamo = table_hydamo['layer_verwerkte_data']
    layer_options = "ASPATIAL_VARIANT=GPKG_ATTRIBUTES"
    if data_hydamo.geometry.isnull().all():
        data_hydamo.to_file(path_data_hydamo, layer=layer_data_hydamo, driver="GPKG", layer_options=layer_options)
    else:
        data_hydamo.to_file(path_data_hydamo, layer=layer_data_hydamo, driver="GPKG")


def check_ids_hydamo_data(data_hydamo, waterboard_code, hydamo_object):
    if 'objectid' not in data_hydamo.columns:
        data_hydamo['objectid'] = 99999
    if 'nen3610id' not in data_hydamo.columns:
        data_hydamo['nen3610id'] = f"NL.WBHCODE.{waterboard_code}.{hydamo_object}" + data_hydamo['objectid'].astype(str)
    if 'globalid' not in data_hydamo.columns:
        data_hydamo['globalid'] = ''
    return data_hydamo
