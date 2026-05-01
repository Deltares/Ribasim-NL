"""Interactieve Leaflet HTML-viewer voor Ribasim validatieresultaten."""

import contextlib
import json
import urllib.request
from pathlib import Path

import geopandas as gpd
import numpy as np


def CreateHTMLViewer(
    model_folder: str | Path,
    output_subfolder: str = "Validatieresultaten_HTML",
    model_gpkg: str | None = None,
    include_lhm41: bool = True,
) -> None:
    """Maakt een interactieve HTML-viewer (Leaflet) met meetlocaties op een OSM-kaart.

    Parameters
    ----------
    model_folder
        Map van het Ribasim-model. Geopackages en figuren worden vanuit
        ``{model_folder}/results/`` gelezen.
    output_subfolder
        Naam van de submap binnen ``{model_folder}/results/`` waar de viewer wordt
        opgeslagen. Standaard ``Validatieresultaten_HTML``.
    model_gpkg
        Optioneel pad naar de Ribasim ``database.gpkg``. Als opgegeven worden de
        lagen ``Node`` en ``Link`` als achtergrondnetwerk toegevoegd aan de viewer.
        Deze functionaliteit zit er nog niet goed in, het netwerk is nog te groot.
    include_lhm41
        Als ``True`` (standaard) wordt de LHM 4.1 vergelijkingslaag toegevoegd als
        ``Validatie_resultaten_lhm41.gpkg`` beschikbaar is.
    """
    LEAFLET_VERSION = "1.9.4"
    LEAFLET_JS_URL = f"https://unpkg.com/leaflet@{LEAFLET_VERSION}/dist/leaflet.js"
    LEAFLET_CSS_URL = f"https://unpkg.com/leaflet@{LEAFLET_VERSION}/dist/leaflet.css"

    results_folder = Path(model_folder) / "results"
    output_folder = results_folder / output_subfolder
    layers_folder = output_folder / "layers"
    resources_folder = output_folder / "resources"
    layers_folder.mkdir(parents=True, exist_ok=True)
    resources_folder.mkdir(parents=True, exist_ok=True)

    # Download Leaflet lokaal (eenmalig) zodat de viewer offline werkt
    leaflet_js_path = resources_folder / "leaflet.js"
    leaflet_css_path = resources_folder / "leaflet.css"
    for url, path in [(LEAFLET_JS_URL, leaflet_js_path), (LEAFLET_CSS_URL, leaflet_css_path)]:
        if not path.exists():
            print(f"  Downloaden: {url}")
            try:
                urllib.request.urlretrieve(url, path)  # noqa: S310
            except Exception as e:
                print(f"  WAARSCHUWING: kon {url} niet downloaden ({e}). Valt terug op CDN.")

    images_folder = resources_folder / "images"
    images_folder.mkdir(parents=True, exist_ok=True)
    for img in ["layers.png", "layers-2x.png", "marker-icon.png", "marker-icon-2x.png", "marker-shadow.png"]:
        img_path = images_folder / img
        if not img_path.exists():
            with contextlib.suppress(Exception):
                urllib.request.urlretrieve(f"https://unpkg.com/leaflet@{LEAFLET_VERSION}/dist/images/{img}", img_path)

    # Lees validatie-geopackages
    gpkg_dag = results_folder / "Validatie_resultaten_all.gpkg"
    gpkg_dec = results_folder / "Validatie_resultaten_dec_all.gpkg"
    if not gpkg_dag.exists():
        gpkg_dag = results_folder / "Validatie_resultaten.gpkg"
    if not gpkg_dec.exists():
        gpkg_dec = results_folder / "Validatie_resultaten_dec.gpkg"

    # Kraan Waterverdeling Nederland - vaste lijst van MeetreeksC-namen
    KRAAN_LIJST = {
        "WIJK_DUURSTEDE_aanvoer HDSR",
        "Gouda HRR",
        "Megen dorp Hoofdkranen",
        "Wijk bij Duurstede kanaal Hoofdkranen",
        "Almen vechtstromen",
        "Rogatsluis WDOD",
        "Tacozijl wetterskip",
        "Teroelsterkolk wetterskip",
        "Gemaal Hoogland wetterskip",
        "Den Oever buiten",
        "Driel boven",
        "Hagestein boven",
        "Haringvlietsluizen binnen",
        "Houtrib noord",
        "IJmuiden binnen",
        "Kornwerderzand buiten",
        "Krabbersgat noord",
        "Schellingwoude inlaatsluis",
        "Tiel Waal",
        "Gemaal Vissering. totaal",
        "Gemaal Colijn (hoog).  pomp 1100",
        "Gemaal Colijn - laag. totaal debiet",
        "Gemaal De Blocq van Kuffeler - hoog. totaal debiet",
        "Gemaal De Blocq van Kuffeler - laag. totaal debiet",
        "Volkeraksluizen",
        "AANVOERDERGEMAAL_aanvoer HDSR",
        "Paradijssluis WDOD",
        "Doornenburg WSRL",
        "Gemaal Wortman. totaal debiet",
    }

    def _is_kraan(props: dict) -> bool:
        for key in ["koppelinfo", "MeetreeksC"]:
            val = str(props.get(key) or "")
            for deel in val.split(","):
                if deel.strip() in KRAAN_LIJST:
                    return True
        return False

    def _list_layers(gpkg_path: Path) -> list[str]:
        return gpd.list_layers(gpkg_path)["name"].tolist()

    def _norm_val(val):
        if isinstance(val, np.integer):
            return int(val)
        if isinstance(val, np.floating):
            return float(val) if not np.isnan(val) else None
        if isinstance(val, float) and np.isnan(val):
            return None
        return val

    def _gpkg_to_features(gpkg_path: Path) -> list[dict]:
        features = []
        for layer in _list_layers(gpkg_path):
            gdf = gpd.read_file(gpkg_path, layer=layer).to_crs(epsg=4326)
            for _, row in gdf.iterrows():
                if row.geometry is None:
                    continue
                props = {col: _norm_val(row[col]) for col in gdf.columns if col != "geometry"}
                # Normaliseer Aan/Af naar consistente sleutel 'aan_af'; LHM41 gebruikt 'Categorie'
                for key in ["Aan/Af", "Aan_Af", "AanAf"]:
                    if key in props and props[key] is not None:
                        props["aan_af"] = str(props[key])
                        break
                if "aan_af" not in props and props.get("Categorie"):
                    props["aan_af"] = str(props["Categorie"])
                features.append(
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [round(row.geometry.x, 6), round(row.geometry.y, 6)],
                        },
                        "properties": props,
                    }
                )
        return features

    features_dag = _gpkg_to_features(gpkg_dag) if gpkg_dag.exists() else []
    features_dec = _gpkg_to_features(gpkg_dec) if gpkg_dec.exists() else []

    gpkg_lhm41 = results_folder / "Validatie_resultaten_lhm41.gpkg"
    features_lhm41 = _gpkg_to_features(gpkg_lhm41) if (include_lhm41 and gpkg_lhm41.exists()) else []
    has_lhm41 = len(features_lhm41) > 0

    # Voeg kraan_waterverdeling_nl toe aan alle features op basis van de vaste lijst
    for feat in features_dag + features_dec + features_lhm41:
        feat["properties"]["kraan_waterverdeling_nl"] = "Ja" if _is_kraan(feat["properties"]) else "Nee"

    # ── Cross-referentie dag ↔ decade: figuurpaden + statistieken ───────────────
    dec_props_lookup = {
        f["properties"]["koppelinfo"]: f["properties"] for f in features_dec if f["properties"].get("koppelinfo")
    }
    dag_props_lookup = {
        f["properties"]["koppelinfo"]: f["properties"] for f in features_dag if f["properties"].get("koppelinfo")
    }

    stat_keys = ["NSE", "KGE", "RelBias", "P10_reldev", "P25_reldev", "P75_reldev", "P90_reldev", "RMSE", "MAE"]

    for feat in features_dag:
        ki = feat["properties"].get("koppelinfo", "")
        p = feat["properties"]
        dec_p = dec_props_lookup.get(ki + "_decade", {})
        p["figure_path_dag"] = p.get("figure_path", "")
        p["figure_path_dec"] = dec_p.get("figure_path", "")
        for k in stat_keys:
            p[f"{k}_dec"] = dec_p.get(k)

    for feat in features_dec:
        ki = feat["properties"].get("koppelinfo", "")
        p = feat["properties"]
        dag_p = dag_props_lookup.get(ki.replace("_decade", ""), {})
        p["figure_path_dec"] = p.get("figure_path", "")
        p["figure_path_dag"] = dag_p.get("figure_path", "")
        for k in stat_keys:
            p[f"{k}_dec"] = p.get(k)

    # ── Schrijf GeoJSON als losse JS-bestanden ───────────────────────────────────
    def _write_js(features: list, var_name: str, js_path: Path) -> int:
        geojson = json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False)
        js_path.write_text(f"var {var_name} = {geojson};", encoding="utf-8")
        return len(features)

    n_dag = _write_js(features_dag, "geojsonDag", layers_folder / "dag.js")
    n_dec = _write_js(features_dec, "geojsonDec", layers_folder / "dec.js")
    print(f"  Dag-laag:    {n_dag} locaties")
    print(f"  Decade-laag: {n_dec} locaties")
    if has_lhm41:
        n_lhm41 = _write_js(features_lhm41, "geojsonLhm41", layers_folder / "lhm41.js")
        print(f"  LHM41-laag:  {n_lhm41} groepen")

    # ── Model netwerk (optioneel) ────────────────────────────────────────────────
    netwerk_script_tag = ""
    netwerk_lagen_html = ""
    has_netwerk = False

    if model_gpkg and Path(model_gpkg).exists():
        node_ok = link_ok = False

        try:
            gdf_nodes = gpd.read_file(model_gpkg, layer="Node").to_crs(epsg=4326)
            node_cols = [c for c in ["node_id", "node_type", "name"] if c in gdf_nodes.columns]
            node_feats = []
            for _, row in gdf_nodes.iterrows():
                if row.geometry is None:
                    continue
                node_feats.append(
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [round(row.geometry.x, 5), round(row.geometry.y, 5)],
                        },
                        "properties": {c: _norm_val(row[c]) for c in node_cols},
                    }
                )
            n_nodes = _write_js(node_feats, "geojsonNodes", layers_folder / "nodes.js")
            print(f"  Nodes:       {n_nodes}")
            node_ok = True
        except Exception as e:
            print(f"  Kon Node-laag niet lezen: {e}")

        # Links — alle modelverbindingen
        try:
            gdf_links = gpd.read_file(model_gpkg, layer="Link").to_crs(epsg=4326)
            link_cols = [c for c in ["link_id", "from_node_id", "to_node_id", "link_type"] if c in gdf_links.columns]
            link_feats = []
            for _, row in gdf_links.iterrows():
                geom = row.geometry
                if geom is None:
                    continue
                if geom.geom_type == "LineString":
                    coords = [[round(c[0], 4), round(c[1], 4)] for c in geom.coords]
                elif geom.geom_type == "MultiLineString":
                    coords = [[round(c[0], 4), round(c[1], 4)] for line in geom.geoms for c in line.coords]
                else:
                    continue
                link_feats.append(
                    {
                        "type": "Feature",
                        "geometry": {"type": "LineString", "coordinates": coords},
                        "properties": {c: _norm_val(row[c]) for c in link_cols},
                    }
                )
            n_links = _write_js(link_feats, "geojsonLinks", layers_folder / "links.js")
            print(f"  Links:       {n_links}")
            link_ok = True
        except Exception as e:
            print(f"  Kon Link-laag niet lezen: {e}")

        # Basin / area polygons — gesimplificeerd voor kleinere bestandsgrootte
        basin_ok = False
        try:
            available = gpd.list_layers(model_gpkg)["name"].tolist()
            basin_layer = next((n for n in available if "basin" in n.lower() and "area" in n.lower()), None)
            if basin_layer:
                gdf_basin = gpd.read_file(model_gpkg, layer=basin_layer).to_crs(epsg=4326)
                # Simplificeer geometrie: ~50m tolerantie, nauwelijks zichtbaar op normale zoom
                gdf_basin["geometry"] = gdf_basin["geometry"].simplify(0.0005, preserve_topology=True)
                gdf_basin = gdf_basin[gdf_basin["geometry"].notna() & ~gdf_basin["geometry"].is_empty]
                keep = [c for c in ["node_id"] if c in gdf_basin.columns] + ["geometry"]
                import json as _json

                basin_feats = _json.loads(gdf_basin[keep].to_json(show_bbox=False)).get("features", [])
                n_basin = _write_js(basin_feats, "geojsonBasin", layers_folder / "basin.js")
                print(f"  Basin vlakken: {n_basin} (gesimplificeerd)")
                basin_ok = True
            else:
                print(f"  Geen Basin/area laag gevonden (beschikbaar: {available})")
        except Exception as e:
            print(f"  Kon Basin/area-laag niet lezen: {e}")

        has_netwerk = node_ok or link_ok or basin_ok
        if has_netwerk:
            scripts = []
            if basin_ok:
                scripts.append('<script src="layers/basin.js"></script>')
            # links.js wordt lazy geladen bij eerste toggle - NIET hier als <script>
            if node_ok:
                scripts.append('<script src="layers/nodes.js"></script>')
            netwerk_script_tag = "\n".join(scripts)
            netwerk_lagen_html = ""
            if basin_ok:
                netwerk_lagen_html += """\
                <div class="laag-item">
                    <label class="laag-label" for="tog-basin">
                        <span class="laag-bol" style="background:#aed6f1; border:1px solid #2980b9; border-radius:2px;"></span>Basin vlakken
                    </label>
                    <label class="toggle"><input type="checkbox" id="tog-basin" checked><span class="toggle-slider"></span></label>
                </div>"""
            if link_ok:
                netwerk_lagen_html += """
                <div class="laag-item">
                    <label class="laag-label" for="tog-links">
                        <span class="laag-bol" style="background:#1a5276; border-radius:2px;"></span>Modelverbindingen
                    </label>
                    <label class="toggle"><input type="checkbox" id="tog-links"><span class="toggle-slider"></span></label>
                </div>"""
            if node_ok:
                netwerk_lagen_html += """
                <div class="laag-item">
                    <label class="laag-label" for="tog-nodes">
                        <span class="laag-bol" style="background:#4575b4;"></span>Modelknopen
                    </label>
                    <label class="toggle"><input type="checkbox" id="tog-nodes"><span class="toggle-slider"></span></label>
                </div>"""

    # ── Leaflet referenties (lokaal of CDN als fallback) ─────────────────────────
    leaflet_css_ref = (
        '<link rel="stylesheet" href="resources/leaflet.css"/>'
        if leaflet_css_path.exists()
        else f'<link rel="stylesheet" href="{LEAFLET_CSS_URL}"/>'
    )
    leaflet_js_ref = (
        '<script src="resources/leaflet.js"></script>'
        if leaflet_js_path.exists()
        else f'<script src="{LEAFLET_JS_URL}"></script>'
    )

    # ── HTML template ────────────────────────────────────────────────────────────
    html = """\
<!DOCTYPE html>
<html lang="nl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="initial-scale=1,user-scalable=no,maximum-scale=1,width=device-width">
    <title>Ribasim LHM - Validatie Resultaten</title>
    LEAFLET_CSS_REF
    LEAFLET_JS_REF
    <style>
        *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
        :root {
            --blauw:  #2980b9;
            --accent: #1a6fa8;
            --licht:  #d6eaf8;
            --border: #d1d1d1;
            --tekst:  #222;
            --grijs:  #666;
            --panel:  270px;
        }
        html, body { height: 100%; font-family: Arial, sans-serif; font-size: 13px; color: var(--tekst); }
        .app { display: flex; flex-direction: column; height: 100vh; overflow: hidden; }

        .header {
            flex: 0 0 46px; background: var(--blauw); color: #fff;
            display: flex; align-items: center; padding: 0 18px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.25); z-index: 1000;
        }
        .header h1 { font-size: 15px; font-weight: 600; letter-spacing: 0.2px; }

        .inhoud { display: flex; flex: 1; overflow: hidden; min-height: 0; }

        .panel {
            width: var(--panel); flex-shrink: 0; background: #fff;
            border-right: 1px solid var(--border); display: flex;
            flex-direction: column; overflow-y: auto; z-index: 500;
            box-shadow: 2px 0 8px rgba(0,0,0,0.07);
        }
        .sectie { padding: 14px 16px; border-bottom: 1px solid var(--border); }
        .sectie:last-child { border-bottom: none; }
        .sectie-titel {
            font-size: 12px; font-weight: 600; color: var(--tekst); margin-bottom: 10px;
            border-bottom: 2px solid var(--accent); padding-bottom: 4px;
        }

        .laag-item {
            display: flex; align-items: center; justify-content: space-between;
            padding: 5px 0; border-bottom: 1px solid #f0f4f8;
        }
        .laag-item:last-child { border-bottom: none; }
        .laag-label { display: flex; align-items: center; gap: 8px; font-size: 12px; cursor: pointer; flex: 1; }
        .laag-bol { width: 12px; height: 12px; border-radius: 50%; border: 1px solid rgba(0,0,0,0.2); flex-shrink: 0; }
        .toggle { position: relative; width: 34px; height: 18px; flex-shrink: 0; }
        .toggle input { opacity: 0; width: 0; height: 0; }
        .toggle-slider {
            position: absolute; inset: 0; background: #ccc;
            border-radius: 18px; cursor: pointer; transition: background 0.2s;
        }
        .toggle-slider::before {
            content: ''; position: absolute; width: 12px; height: 12px; left: 3px; top: 3px;
            background: #fff; border-radius: 50%;
            transition: transform 0.2s; box-shadow: 0 1px 3px rgba(0,0,0,0.2);
        }
        .toggle input:checked + .toggle-slider { background: var(--accent); }
        .toggle input:checked + .toggle-slider::before { transform: translateX(16px); }

        .ws-knoppen { display: flex; gap: 5px; margin-bottom: 9px; }
        .ws-knop {
            flex: 1; font-size: 11px; padding: 3px 6px;
            border: 1px solid var(--border); border-radius: 3px;
            background: #f8fbfd; cursor: pointer; color: var(--grijs); transition: all 0.15s;
        }
        .ws-knop:hover { border-color: var(--accent); color: var(--accent); }
        .ws-lijst { display: flex; flex-direction: column; gap: 3px; max-height: 200px; overflow-y: auto; }
        .ws-item { display: flex; align-items: center; gap: 7px; font-size: 12px; padding: 2px 0; cursor: pointer; }
        .ws-item input[type=checkbox] { accent-color: var(--accent); width: 13px; height: 13px; flex-shrink: 0; cursor: pointer; }

        .kaartlaag-knoppen { display: flex; gap: 5px; flex-wrap: wrap; }
        .btn-kaartlaag {
            flex: 1; min-width: 60px; padding: 5px 6px; font-size: 11px; font-weight: 600;
            border: 1.5px solid var(--border); border-radius: 4px;
            background: #f8fbfd; color: var(--grijs); cursor: pointer;
            text-align: center; transition: all 0.15s;
        }
        .btn-kaartlaag:hover { border-color: var(--accent); color: var(--accent); }
        .btn-kaartlaag.actief { background: var(--accent); border-color: var(--accent); color: #fff; }

        .leg-rij { display: flex; align-items: center; gap: 8px; margin-bottom: 5px; font-size: 12px; }
        .leg-bol { width: 13px; height: 13px; border-radius: 50%; border: 1px solid #666; flex-shrink: 0; }

        #map { flex: 1; min-width: 0; }

        .leaflet-popup-content-wrapper { max-height: 85vh; overflow: hidden; }
        .leaflet-popup-content {
            min-width: 650px; max-height: calc(85vh - 40px);
            overflow-y: auto; overflow-x: hidden; font-size: 12px; padding-right: 4px;
        }
        .pop-title { font-weight: bold; font-size: 13px; margin-bottom: 2px; }
        .pop-ws    { color: #555; margin-bottom: 4px; font-size: 11px; }
        .pop-meta  { color: #888; font-size: 10px; margin-bottom: 8px; }
        .pop-stats-lbl { font-size: 10px; color: #888; font-style: italic; margin-bottom: 4px; }
        .pop-stats { display: grid; grid-template-columns: repeat(3,1fr); gap: 4px 8px; margin-bottom: 12px; }
        .pop-stat  { background: #f5f5f5; border-radius: 4px; padding: 4px 7px; }
        .pop-stat b { display: block; font-size: 10px; color: #888; }
        .pop-fig-label {
            font-weight: bold; font-size: 11px; color: #333;
            background: #e8e8e8; border-radius: 3px; padding: 3px 8px; margin: 10px 0 5px 0;
        }
        .pop-img { width: 620px; border: 1px solid #ddd; border-radius: 3px; display: block; }

        ::-webkit-scrollbar { width: 8px; }
        ::-webkit-scrollbar-track { background: #f1f1f1; }
        ::-webkit-scrollbar-thumb { background: #c1c1c1; border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: #a1a1a1; }
    </style>
</head>
<body>
<div class="app">
    <div class="header">
        <h1>Ribasim LHM &mdash; Validatie Resultaten</h1>
    </div>

    <div class="inhoud">
        <div class="panel">

            <!-- Lagen -->
            <div class="sectie">
                <div class="sectie-titel">Lagen</div>
                <div class="laag-item">
                    <label class="laag-label" for="tog-dag">
                        <span class="laag-bol" style="background:var(--accent)"></span>Dagwaarden
                    </label>
                    <label class="toggle"><input type="checkbox" id="tog-dag" checked><span class="toggle-slider"></span></label>
                </div>
                <div class="laag-item">
                    <label class="laag-label" for="tog-dec">
                        <span class="laag-bol" style="background:var(--accent)"></span>Decadewaarden
                    </label>
                    <label class="toggle"><input type="checkbox" id="tog-dec"><span class="toggle-slider"></span></label>
                </div>
                <div class="laag-item">
                    <label class="laag-label" for="tog-pdok">
                        <span class="laag-bol" style="background:#3498db; border-radius:2px;"></span>Waterschapsgrenzen
                    </label>
                    <label class="toggle"><input type="checkbox" id="tog-pdok" checked><span class="toggle-slider"></span></label>
                </div>
                LHM41_LAAG_HTML
                NETWERK_LAGEN_HTML
            </div>

            <!-- Waterschap filter -->
            <div class="sectie">
                <div class="sectie-titel">Waterschappen</div>
                <div class="ws-knoppen">
                    <button class="ws-knop" id="btn-alles-aan" onclick="wsAllesToggle()">Alles uit</button>
                </div>
                <div class="ws-lijst" id="ws-lijst"></div>
            </div>

            <!-- Filter tijdsreeks -->
            <div class="sectie" id="p95-sectie" style="display:none">
                <div class="sectie-titel">Filter op basis van gehele tijdsreeks</div>
                <div class="ws-lijst" id="p95-lijst"></div>
            </div>

            <!-- Filter Aan/Af -->
            <div class="sectie" id="aanaf-sectie" style="display:none">
                <div class="sectie-titel">Aan/Af type</div>
                <div class="ws-lijst" id="aanaf-lijst"></div>
            </div>

            <!-- Filter Kraan Waterverdeling Nederland -->
            <div class="sectie" id="kraan-sectie" style="display:none">
                <div class="sectie-titel">Kraan Waterverdeling NL</div>
                <div class="ws-lijst" id="kraan-lijst"></div>
            </div>

            <!-- Achtergrondkaart -->
            <div class="sectie">
                <div class="sectie-titel">Achtergrondkaart</div>
                <div class="kaartlaag-knoppen">
                    <button class="btn-kaartlaag actief" onclick="setBasemap('osm', this)">OSM</button>
                    <button class="btn-kaartlaag" onclick="setBasemap('topo', this)">Topo</button>
                    <button class="btn-kaartlaag" onclick="setBasemap('luchtfoto', this)">Luchtfoto</button>
                </div>
            </div>

            <!-- NSE Legenda -->
            <div class="sectie">
                <div class="sectie-titel">Legenda NSE</div>
                <div class="leg-rij"><span class="leg-bol" style="background:rgba(26,150,65,0.9)"></span>0,75 - 1,0</div>
                <div class="leg-rij"><span class="leg-bol" style="background:rgba(166,217,106,0.9)"></span>0,50 - 0,75</div>
                <div class="leg-rij"><span class="leg-bol" style="background:rgba(255,255,192,0.9)"></span>0,25 - 0,50</div>
                <div class="leg-rij"><span class="leg-bol" style="background:rgba(253,174,97,0.9)"></span>0,00 - 0,25</div>
                <div class="leg-rij"><span class="leg-bol" style="background:rgba(215,25,28,0.9)"></span>&lt; 0,00</div>
                <div class="leg-rij"><span class="leg-bol" style="background:#808080"></span>n.v.t.</div>
            </div>

        </div><!-- /panel -->

        <div id="map"></div>
    </div>
</div>

<script src="layers/dag.js"></script>
<script src="layers/dec.js"></script>
LHM41_SCRIPT_TAG
NETWERK_SCRIPT_TAG
<script>
// ── Achtergrondkaarten ────────────────────────────────────────────────────────
var basemaps = {
    osm: L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        maxZoom: 19, attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
    }),
    topo: L.tileLayer('https://service.pdok.nl/brt/achtergrondkaart/wmts/v2_0/standaard/EPSG:3857/{z}/{x}/{y}.png', {
        maxZoom: 19, attribution: '&copy; <a href="https://www.pdok.nl">PDOK</a> - BRT Achtergrondkaart'
    }),
    luchtfoto: L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
        maxZoom: 19, attribution: '&copy; Esri - World Imagery'
    })
};
var huidigeBasiskaart = 'osm';
function setBasemap(naam, btn) {
    map.removeLayer(basemaps[huidigeBasiskaart]);
    basemaps[naam].addTo(map).bringToBack();
    huidigeBasiskaart = naam;
    document.querySelectorAll('.btn-kaartlaag').forEach(function(b) { b.classList.remove('actief'); });
    btn.classList.add('actief');
}

// ── Kaart + panes ─────────────────────────────────────────────────────────────
var map = L.map('map', {zoomControl: true, layers: [basemaps.osm]}).setView([52.3, 5.5], 8);
map.createPane('nettewerkPane').style.zIndex = 390;
map.createPane('validatiePane').style.zIndex  = 420;

var pdokLayer = L.tileLayer.wms('https://service.pdok.nl/hwh/waterschapsgrenzenimso/wms/v1_0', {
    layers: 'waterschap', format: 'image/png', transparent: true, version: '1.3.0',
    attribution: 'PDOK - Waterschapsgrenzen'
});
pdokLayer.addTo(map);

// ── NSE kleur ─────────────────────────────────────────────────────────────────
function nseColor(nse) {
    if (nse === null || nse === undefined || isNaN(nse)) return '#808080';
    if (nse <  0.00) return 'rgba(215,25,28,0.9)';
    if (nse <  0.25) return 'rgba(253,174,97,0.9)';
    if (nse <  0.50) return 'rgba(255,255,192,0.9)';
    if (nse <  0.75) return 'rgba(166,217,106,0.9)';
    return 'rgba(26,150,65,0.9)';
}

// ── Node kleur (Ribasim knoptypen) ────────────────────────────────────────────
var nodeColors = {
    Basin: '#4575b4', Pump: '#9970ab', TabulatedRatingCurve: '#5aae61',
    LevelBoundary: '#f46d43', FlowBoundary: '#d73027', ManningResistance: '#8c510a',
    LinearResistance: '#bf812d', Terminal: '#333', Outlet: '#1a9850',
    FractionalFlow: '#888', UserDemand: '#e76f51', DiscreteControl: '#f4a261',
    PidControl: '#e9c46a'
};
function nodeColor(type) { return nodeColors[type] || '#666'; }

// ── Popup helpers ─────────────────────────────────────────────────────────────
function fmt(v, dec) {
    return (v !== null && v !== undefined && !isNaN(v)) ? v.toFixed(dec) : 'n.v.t.';
}
function imgFromTag(tag, label) {
    if (!tag) return '';
    var match = tag.match(/src="([^"]+)"/);
    if (!match) return '';
    return '<div class="pop-fig-label">' + label + '</div>'
         + '<img class="pop-img" src="' + match[1] + '" '
         + 'onerror="this.parentNode.style.display=\\'none\\'" />';
}

// Popup: statistieken altijd van decadewaarden; dag-plot boven, dec-plot onder.
function makePopup(p) {
    var linkTxt = p.link_id != null ? 'link_id: ' + p.link_id : '';
    var stats = '<div class="pop-stats-lbl">Statistieken (decadewaarden)</div>'
        + '<div class="pop-stats">'
        + '<div class="pop-stat"><b>NSE</b>'      + fmt(p.NSE_dec, 2)        + '</div>'
        + '<div class="pop-stat"><b>KGE</b>'      + fmt(p.KGE_dec, 2)        + '</div>'
        + '<div class="pop-stat"><b>Bias</b>'     + fmt(p.RelBias_dec, 1)    + ' %</div>'
        + '<div class="pop-stat"><b>P10 afw.</b>' + fmt(p.P10_reldev_dec, 1) + ' %</div>'
        + '<div class="pop-stat"><b>P25 afw.</b>' + fmt(p.P25_reldev_dec, 1) + ' %</div>'
        + '<div class="pop-stat"><b>P75 afw.</b>' + fmt(p.P75_reldev_dec, 1) + ' %</div>'
        + '<div class="pop-stat"><b>P90 afw.</b>' + fmt(p.P90_reldev_dec, 1) + ' %</div>'
        + '<div class="pop-stat"><b>RMSE</b>'     + fmt(p.RMSE_dec, 3)       + '</div>'
        + '</div>';
    var figs = imgFromTag(p.figure_path_dec, 'Decadewaarden')
             + imgFromTag(p.figure_path_dag, 'Dagwaarden');
    return '<div class="pop-title">' + (p.koppelinfo || '') + '</div>'
         + '<div class="pop-ws">'   + (p.waterschap || '') + '</div>'
         + (linkTxt ? '<div class="pop-meta">' + linkTxt + '</div>' : '')
         + stats + figs;
}

// ── Model netwerk ─────────────────────────────────────────────────────────────
var layerBasin = null, layerLinks = null, layerNodes = null;

// Basin vlakken - lichtblauw transparant
if (typeof geojsonBasin !== 'undefined' && geojsonBasin) {
    layerBasin = L.geoJSON(geojsonBasin, {
        style: { color: '#2980b9', weight: 0.8, opacity: 0.6, fillColor: '#aed6f1', fillOpacity: 0.25 },
        pane: 'nettewerkPane'
    }).addTo(map);
}

// Modelverbindingen - lazy geladen bij eerste toggle (canvas renderer voor prestaties)
var _linksGeladen = false;
var _canvasRenderer = L.canvas({ pane: 'nettewerkPane' });
function _maakLinksLaag() {
    if (typeof geojsonLinks === 'undefined' || !geojsonLinks) return;
    var arrowGroup = L.layerGroup();
    var linesLayer = L.geoJSON(geojsonLinks, {
        style: { color: '#1a5276', weight: 1.5, opacity: 0.7 },
        renderer: _canvasRenderer,
        onEachFeature: function(feat, layer) {
            var lid = feat.properties.link_id;
            if (lid != null) layer.bindTooltip('link_id: ' + lid, {sticky: true});
            // Richtingspijl op 75% van de lijn
            var coords = feat.geometry.coordinates;
            if (coords.length >= 2) {
                var idx = Math.max(1, Math.floor(coords.length * 0.75));
                var c1 = coords[idx - 1], c2 = coords[idx];
                var bearing = Math.atan2(
                    (c2[0] - c1[0]) * Math.cos(c1[1] * Math.PI / 180),
                    c2[1] - c1[1]
                ) * 180 / Math.PI;
                var mid = L.latLng((c1[1] + c2[1]) / 2, (c1[0] + c2[0]) / 2);
                arrowGroup.addLayer(L.marker(mid, {
                    icon: L.divIcon({
                        html: '<div style="width:0;height:0;'
                            + 'border-left:4px solid transparent;'
                            + 'border-right:4px solid transparent;'
                            + 'border-bottom:9px solid #1a5276;'
                            + 'transform:rotate(' + bearing + 'deg);'
                            + 'transform-origin:4px 9px;"></div>',
                        className: '', iconSize: [8, 9], iconAnchor: [4, 5]
                    }),
                    pane: 'nettewerkPane', interactive: false
                }));
            }
        }
    });
    layerLinks = L.featureGroup([linesLayer, arrowGroup]);
}

// Modelknopen - cirkel gekleurd per knooptype, popup met type + node_id
if (typeof geojsonNodes !== 'undefined' && geojsonNodes) {
    layerNodes = L.geoJSON(geojsonNodes, {
        pointToLayer: function(feat, ll) {
            return L.circleMarker(ll, {
                radius: 6, fillColor: nodeColor(feat.properties.node_type),
                color: '#fff', weight: 0.8, opacity: 1, fillOpacity: 0.9,
                pane: 'nettewerkPane'
            });
        },
        onEachFeature: function(feat, layer) {
            var p = feat.properties;
            var popup = '<b>' + (p.node_type || 'onbekend') + '</b>'
                      + (p.node_id != null ? '<br>node_id: ' + p.node_id : '')
                      + (p.name ? '<br>' + p.name : '');
            layer.bindPopup(popup, {maxWidth: 220});
        }
    });
}

// ── Validatie lagen ───────────────────────────────────────────────────────────
function maakLaag(geojson) {
    return L.geoJSON(geojson, {
        pointToLayer: function(feat, ll) {
            return L.circleMarker(ll, {
                radius: 7, fillColor: nseColor(feat.properties.NSE),
                color: '#333', weight: 0.8, opacity: 1, fillOpacity: 0.9,
                pane: 'validatiePane'
            });
        },
        onEachFeature: function(feat, layer) {
            layer.bindPopup(makePopup(feat.properties), {maxWidth: 480});
        }
    });
}

var layerDag = maakLaag(geojsonDag);
var layerDec = maakLaag(geojsonDec);
layerDag.addTo(map);

// ── LHM 4.1 vergelijkingslaag ─────────────────────────────────────────────
var layerLhm41 = null;
LHM41_LAAG_JS

if (geojsonDag && geojsonDag.features && geojsonDag.features.length > 0) {
    map.fitBounds(layerDag.getBounds().pad(0.05));
}

// ── Laag-toggles ─────────────────────────────────────────────────────────────
document.getElementById('tog-dag').addEventListener('change', function() {
    if (this.checked) layerDag.addTo(map); else map.removeLayer(layerDag);
});
document.getElementById('tog-dec').addEventListener('change', function() {
    if (this.checked) layerDec.addTo(map); else map.removeLayer(layerDec);
});
document.getElementById('tog-pdok').addEventListener('change', function() {
    if (this.checked) pdokLayer.addTo(map); else map.removeLayer(pdokLayer);
});
var _togLhm41 = document.getElementById('tog-lhm41');
if (_togLhm41) {
    _togLhm41.addEventListener('change', function() {
        if (!layerLhm41) return;
        if (this.checked) layerLhm41.addTo(map); else map.removeLayer(layerLhm41);
    });
}
var _nettewerkToggles = {
    'tog-basin': function() { return layerBasin; },
    'tog-nodes': function() { return layerNodes; }
};
Object.keys(_nettewerkToggles).forEach(function(id) {
    var el = document.getElementById(id);
    if (!el) return;
    el.addEventListener('change', function() {
        var lyr = _nettewerkToggles[id]();
        if (!lyr) return;
        if (this.checked) lyr.addTo(map); else map.removeLayer(lyr);
    });
});
// tog-links: lazy load links.js bij eerste inschakeling
var _togLinks = document.getElementById('tog-links');
if (_togLinks) {
    _togLinks.addEventListener('change', function() {
        if (this.checked) {
            if (!_linksGeladen) {
                _linksGeladen = true;
                var s = document.createElement('script');
                s.src = 'layers/links.js';
                s.onload = function() { _maakLinksLaag(); if (layerLinks) layerLinks.addTo(map); };
                document.head.appendChild(s);
            } else {
                if (layerLinks) layerLinks.addTo(map);
            }
        } else {
            if (layerLinks) map.removeLayer(layerLinks);
        }
    });
}

// ── Waterschap filter ─────────────────────────────────────────────────────────
var _lhm41Ws = (typeof geojsonLhm41 !== 'undefined' && geojsonLhm41)
    ? (geojsonLhm41.features || []).map(function(f) { return f.properties.Waterschap || '(onbekend)'; })
    : [];
var alleWs = [...new Set(
    (geojsonDag.features || []).map(function(f) { return f.properties.waterschap || '(onbekend)'; })
    .concat(_lhm41Ws)
)].sort();
var wsLijst = document.getElementById('ws-lijst');
alleWs.forEach(function(ws) {
    var lbl = document.createElement('label');
    lbl.className = 'ws-item';
    lbl.innerHTML = '<input type="checkbox" value="' + ws.replace(/"/g, '&quot;') + '" checked> ' + ws;
    lbl.querySelector('input').addEventListener('change', updateFilter);
    wsLijst.appendChild(lbl);
});
function wsAllesToggle() {
    var alleAan = document.querySelectorAll('#ws-lijst input:checked').length === alleWs.length;
    document.querySelectorAll('#ws-lijst input').forEach(function(cb) { cb.checked = !alleAan; });
    document.getElementById('btn-alles-aan').textContent = alleAan ? 'Alles aan' : 'Alles uit';
    updateFilter();
}

// ── Filter op basis van gehele tijdsreeks ─────────────────────────────────────
var P95_COL     = 'P95_P05_alle_beschikbare_data';
var P95_COL_LHM = 'P95_klasse';
var _lhm41P95 = (typeof geojsonLhm41 !== 'undefined' && geojsonLhm41)
    ? (geojsonLhm41.features || []).map(function(f) { return f.properties[P95_COL_LHM]; }).filter(function(v) { return v != null; })
    : [];
var alleP95 = [...new Set(
    (geojsonDag.features || [])
        .map(function(f) { return f.properties[P95_COL]; })
        .filter(function(v) { return v != null; })
        .concat(_lhm41P95)
)].sort();
if (alleP95.length > 0) {
    document.getElementById('p95-sectie').style.display = '';
    var p95Lijst = document.getElementById('p95-lijst');
    alleP95.forEach(function(val) {
        var lbl = document.createElement('label');
        lbl.className = 'ws-item';
        lbl.innerHTML = '<input type="checkbox" value="' + val + '" checked> ' + val;
        lbl.querySelector('input').addEventListener('change', updateFilter);
        p95Lijst.appendChild(lbl);
    });
}

// ── Filter Aan/Af type ────────────────────────────────────────────────────────
var _alleFeatsAanAf = (geojsonDag.features || [])
    .concat((typeof geojsonLhm41 !== 'undefined' && geojsonLhm41) ? (geojsonLhm41.features || []) : []);
var alleAanAf = [...new Set(
    _alleFeatsAanAf.map(function(f) { return f.properties.aan_af; }).filter(function(v) { return v != null && v !== ''; })
)].sort();
if (alleAanAf.length > 0) {
    document.getElementById('aanaf-sectie').style.display = '';
    var aanafLijst = document.getElementById('aanaf-lijst');
    alleAanAf.forEach(function(val) {
        var lbl = document.createElement('label');
        lbl.className = 'ws-item';
        lbl.innerHTML = '<input type="checkbox" value="' + val.replace(/"/g, '&quot;') + '" checked> ' + val;
        lbl.querySelector('input').addEventListener('change', updateFilter);
        aanafLijst.appendChild(lbl);
    });
}

// ── Filter Kraan Waterverdeling Nederland ─────────────────────────────────────
var alleKraan = ['Ja', 'Nee'];
document.getElementById('kraan-sectie').style.display = '';
var kraanLijst = document.getElementById('kraan-lijst');
alleKraan.forEach(function(val) {
    var lbl = document.createElement('label');
    lbl.className = 'ws-item';
    lbl.innerHTML = '<input type="checkbox" value="' + val + '" checked> ' + val;
    lbl.querySelector('input').addEventListener('change', updateFilter);
    kraanLijst.appendChild(lbl);
});

// ── Gecombineerd filter ───────────────────────────────────────────────────────
function updateFilter() {
    var gesWs = new Set();
    document.querySelectorAll('#ws-lijst input:checked').forEach(function(cb) { gesWs.add(cb.value); });

    var gesP95 = null;
    if (alleP95.length > 0) {
        gesP95 = new Set();
        document.querySelectorAll('#p95-lijst input:checked').forEach(function(cb) { gesP95.add(cb.value); });
    }
    var gesAanAf = null;
    if (alleAanAf.length > 0) {
        gesAanAf = new Set();
        document.querySelectorAll('#aanaf-lijst input:checked').forEach(function(cb) { gesAanAf.add(cb.value); });
    }
    var gesKraan = new Set();
    document.querySelectorAll('#kraan-lijst input:checked').forEach(function(cb) { gesKraan.add(cb.value); });

    [layerDag, layerDec].forEach(function(lyr) {
        lyr.eachLayer(function(marker) {
            var p   = marker.feature.properties;
            var ws  = p.waterschap || '(onbekend)';
            var ok  = gesWs.has(ws)
                   && (gesP95   === null || gesP95.has(p[P95_COL]))
                   && (gesAanAf === null || gesAanAf.has(p.aan_af || ''))
                   && gesKraan.has(p.kraan_waterverdeling_nl || 'Nee');
            marker.setStyle({opacity: ok ? 1 : 0, fillOpacity: ok ? 0.9 : 0});
            marker.setRadius(ok ? 7 : 0);
        });
    });
    if (typeof layerLhm41 !== 'undefined' && layerLhm41) {
        layerLhm41.eachLayer(function(marker) {
            var p   = marker.feature.properties;
            var ws  = p.Waterschap || '(onbekend)';
            var ok  = gesWs.has(ws)
                   && (gesP95   === null || gesP95.has(p[P95_COL_LHM]))
                   && (gesAanAf === null || gesAanAf.has(p.aan_af || ''))
                   && gesKraan.has(p.kraan_waterverdeling_nl || 'Nee');
            marker.setStyle({opacity: ok ? 1 : 0, fillOpacity: ok ? 0.85 : 0});
            marker.setRadius(ok ? 8 : 0);
        });
    }
}
</script>
</body>
</html>"""

    # ── LHM41 HTML/JS bouwstenen ─────────────────────────────────────────────
    if has_lhm41:
        lhm41_script_tag = '<script src="layers/lhm41.js"></script>'
        lhm41_laag_html = """\
                <div class="laag-item">
                    <label class="laag-label" for="tog-lhm41">
                        <span class="laag-bol" style="background:#27ae60;"></span>LHM 4.1 vergelijking
                    </label>
                    <label class="toggle"><input type="checkbox" id="tog-lhm41"><span class="toggle-slider"></span></label>
                </div>"""
        lhm41_laag_js = """\
function makePopupLhm41(p) {
    var stats = '<div class="pop-stats-lbl">Statistieken (decadewaarden)</div>'
        + '<div class="pop-stats">'
        + '<div class="pop-stat"><b>NSE Rib.</b>'      + fmt(p.NSE_rib,      2) + '</div>'
        + '<div class="pop-stat"><b>NSE LHM</b>'       + fmt(p.NSE_lhm,      2) + '</div>'
        + '<div class="pop-stat"><b>CumJaar Rib.</b>'  + fmt(p.CumJaar_rib,  1) + ' %</div>'
        + '<div class="pop-stat"><b>CumJaar LHM</b>'   + fmt(p.CumJaar_lhm,  1) + ' %</div>'
        + '<div class="pop-stat"><b>CumZomer Rib.</b>' + fmt(p.CumZomer_rib, 1) + ' %</div>'
        + '<div class="pop-stat"><b>CumZomer LHM</b>'  + fmt(p.CumZomer_lhm, 1) + ' %</div>'
        + '</div>';
    return '<div class="pop-title">' + (p.koppelinfo || p.MeetreeksC || '') + '</div>'
         + '<div class="pop-ws">'   + (p.Waterschap || '') + '</div>'
         + '<div class="pop-meta">Categorie: ' + (p.Categorie || '') + '  |  P95: ' + (p.P95_klasse || '') + ' m³/s</div>'
         + stats
         + imgFromTag(p.figure_path, 'LHM 4.1 vergelijking');
}
if (typeof geojsonLhm41 !== 'undefined' && geojsonLhm41) {
    layerLhm41 = L.geoJSON(geojsonLhm41, {
        pointToLayer: function(feat, ll) {
            return L.circleMarker(ll, {
                radius: 8, fillColor: nseColor(feat.properties.NSE_rib),
                color: '#1a5e38', weight: 1.5, opacity: 1, fillOpacity: 0.85,
                pane: 'validatiePane'
            });
        },
        onEachFeature: function(feat, layer) {
            layer.bindPopup(makePopupLhm41(feat.properties), {maxWidth: 480});
        }
    });
}"""
    else:
        lhm41_script_tag = ""
        lhm41_laag_html = ""
        lhm41_laag_js = "// geen LHM 4.1 vergelijkingslaag beschikbaar"

    html = (
        html.replace("LEAFLET_CSS_REF", leaflet_css_ref)
        .replace("LEAFLET_JS_REF", leaflet_js_ref)
        .replace("NETWERK_SCRIPT_TAG", netwerk_script_tag)
        .replace("NETWERK_LAGEN_HTML", netwerk_lagen_html)
        .replace("LHM41_SCRIPT_TAG", lhm41_script_tag)
        .replace("LHM41_LAAG_HTML", lhm41_laag_html)
        .replace("LHM41_LAAG_JS", lhm41_laag_js)
    )

    output_path = output_folder / "index.html"
    output_path.write_text(html, encoding="utf-8")

    print(f"HTML-viewer opgeslagen: {output_path}")
