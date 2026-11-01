"""Extract the postprocessed data of the level controlled water authorities from the crossings files of 2024.

The input models `<authority>_boezemmodel_<version>` were made from crossings files that `parse_crossings` wrote in 2024,
which hold a copy of the postprocessed data they were made from. This script writes that copy to
`<authority>/verwerkt/postprocessed.gpkg`, the source data of `pipeline/00c_parse_crossings.py`, without the column
`agg_area` that `parse_crossings` added to the peilgebieden. It was run once; see `docs/dev/reproducing-models.qmd`.
Wetterskip Fryslân is left out, since `pipeline/wetterskip_fryslan/00_postprocess.py` makes its postprocessed data.

- Rijnland: `rijnland_crossings_v04.gpkg`, since `v05` has hydro-objects added in April 2026.
- Schieland en de Krimpenerwaard: `hhsk_crossings_v04.gpkg`, of which `hhsk_crossings_voor_profielen...` is a copy with
  all hydro-objects.
- Hollandse Delta: the postprocessed data changed after its crossings were made in 2024. Its 26 peilgebieden
  `KRW_body_*` became 22, and the aggregation area `P-V03.005` was left out. The 22 are replaced by the 26 in the layer
  `peilgebied_met_streefpeil` of the `database_checks.gpkg` of its input model, with their streefpeil, and `P-V03.005`
  gets the geometry of its basin area in the input model.

Usage: pixi run python scripts/extract_postprocessed.py <crossings directory of the cloud> <authority> ...
"""

import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyogrio

from ribasim_nl import CloudStorage

CROSSINGS = {
    "AmstelGooienVecht": "agv_crossings_v05.gpkg",
    "Delfland": "delfland_crossings_v08.gpkg",
    "HollandseDelta": "hd_crossings_v06.gpkg",
    "HollandsNoorderkwartier": "hhnk_crossings_v26.gpkg",
    "Rijnland": "rijnland_crossings_v04.gpkg",
    "Rivierenland": "wsrl_crossings_v06.gpkg",
    "Scheldestromen": "scheldestromen_crossings_v02.gpkg",
    "SchielandendeKrimpenerwaard": "hhsk_crossings_v04.gpkg",
    "Zuiderzeeland": "zzl_crossings_v05.gpkg",
}


def input_layers(crossings: Path) -> dict[str, gpd.GeoDataFrame]:
    """The layers of a crossings file that are a copy of its input, in their order."""
    layers = {}
    for name, _ in pyogrio.list_layers(crossings):
        if name.startswith("crossings_") or name == "layer_styles":
            continue
        layer = gpd.read_file(crossings, layer=name)
        if name == "peilgebied":
            layer = layer.drop(columns="agg_area")
        layers[name] = layer
    return layers


def reconstruct_hollandse_delta(layers: dict[str, gpd.GeoDataFrame], checks: Path, areas: gpd.GeoDataFrame) -> None:
    """Restore the KRW water bodies and the aggregation area P-V03.005 of 2024, see the module docstring."""
    krw = gpd.read_file(checks, layer="peilgebied_met_streefpeil")
    krw = krw[krw["code"].str.startswith("KRW_body")].reset_index(drop=True)
    assert len(krw) == 26, f"expected 26 KRW water bodies, found {len(krw)}"

    peilgebied = layers["peilgebied"]
    old = peilgebied["code"].str.startswith("KRW_body")
    assert old.sum() == 22, f"expected 22 KRW water bodies, found {old.sum()}"
    new = gpd.GeoDataFrame(
        {
            "globalid": krw["globalid"],
            "code": krw["code"],
            "nen3610id": "dummy_nen3610id_peilgebied_" + krw["globalid"].str.rsplit("_", n=1).str[-1],
            "peilgebied_cat": 0,
            "meta_name": None,
        },
        geometry=krw.geometry.to_numpy(),
        crs=peilgebied.crs,
    )
    layers["peilgebied"] = pd.concat([peilgebied[~old], new], ignore_index=True)

    streefpeil = layers["streefpeil"]
    old_ids = set(peilgebied.loc[old, "globalid"])
    new = gpd.GeoDataFrame(
        {"waterhoogte": krw["waterhoogte"], "globalid": krw["globalid"]}, geometry=[None] * len(krw), crs=streefpeil.crs
    )
    layers["streefpeil"] = pd.concat([streefpeil[~streefpeil["globalid"].isin(old_ids)], new], ignore_index=True)

    aggregation = layers["aggregation_area"]
    assert "P-V03.005" not in set(aggregation["code"])
    # the basin of the peilgebied of P-V03.005, Voorne Oost-Noord_470, is Basin 330
    area = areas.loc[areas["node_id"] == 330, "geometry"]
    assert len(area) == 1
    new = gpd.GeoDataFrame(
        {
            "code": ["P-V03.005"],
            "nen3610id": ["Voorne Oost-Noord"],
            "globalid": ["dummy_globalid_agg_area_470"],
            "meta_name": [None],
        },
        geometry=area.to_numpy(),
        crs=aggregation.crs,
    )
    i = aggregation.index[aggregation["code"] == "P-V03.004"][0] + 1
    layers["aggregation_area"] = pd.concat([aggregation.iloc[:i], new, aggregation.iloc[i:]], ignore_index=True)


def main() -> None:
    """Extract the postprocessed data of the given authorities.

    The first argument is a directory with the crossings files of the cloud, `<authority>/verwerkt/Crossings/<file>`.
    """
    cloud = CloudStorage()
    source_dir = Path(sys.argv[1])
    for authority in sys.argv[2:]:
        target = cloud.joinpath(authority, "verwerkt", "postprocessed.gpkg")
        target.unlink(missing_ok=True)
        layers = input_layers(source_dir / authority / "verwerkt/Crossings" / CROSSINGS[authority])
        if authority == "HollandseDelta":
            model = cloud.model_dir(authority, "boezemmodel_2024_12_3")
            areas = gpd.read_file(model / "database.gpkg", layer="Basin / area")
            reconstruct_hollandse_delta(layers, model / "database_checks.gpkg", areas)
        for name, layer in layers.items():
            layer.to_file(target, layer=name)
        print(authority, {name: len(layer) for name, layer in layers.items()})


if __name__ == "__main__":
    main()
