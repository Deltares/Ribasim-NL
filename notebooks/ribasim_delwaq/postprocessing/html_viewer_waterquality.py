"""Create an interactive HTML viewer for DELWAQ water-quality validation."""

import contextlib
import importlib
import json
import urllib.request
from pathlib import Path

import geopandas as gpd
import pandas as pd
import plot_correction_factor_histograms
import validation_scatterplots
import xarray as xr
from plot_waterquality_comparisons import save_comparison_plot, seasonal_means
from ribasim import Model
from validation_factors import calculate_validation_factor

MODEL_PATH = Path(r"C:/projects/2024/LWKM/06_Ribasim/02_models/LHM/lhm_coupled_full")
VALIDATION_PATH = Path(r"P:/11212767-lwkm2/RibasimNL/Validatie/Waterkwaliteit")
PARAMETER_COMPONENTS = {
    "Ntot": ["NO3", "NH4", "OON"],
    "Ptot": ["PO4", "AAP", "OOP"],
}
LOCATION_LIMIT = None  # None
RECREATE_PLOTS = True  # Set to False to reuse existing Ntot and Ptot PNG files.


def create_waterquality_html_viewer(
    model_path: Path = MODEL_PATH,
    validation_path: Path = VALIDATION_PATH,
    output_subfolder: str = "html_viewer",
    location_limit: int | None = LOCATION_LIMIT,
    recreate_plots: bool = RECREATE_PLOTS,
) -> Path:
    """Create a Leaflet viewer with DELWAQ and monitoring concentration plots.

    Set ``location_limit`` to ``None`` to create plots for all mapped locations.
    Set ``recreate_plots`` to ``False`` to reuse existing figure files.
    """
    if location_limit is not None and location_limit < 1:
        raise ValueError("location_limit must be at least 1 or None.")
    delwaq_folder = model_path / "delwaq"
    delwaq_map_path = delwaq_folder / "delwaq_map.nc"
    toml_path = model_path / "lhm_coupled.toml"
    model_gpkg_path = model_path / "input" / "database.gpkg"
    mapping_path = validation_path / "output_validate/basins_doorgaand_joined_monlocs.shp"
    observations_path = validation_path / "KRWMeetwaarden_1990_2025_20260615_1432.parquet"
    locations_path = validation_path / "WKP_KRW-monitoringlocaties-oppervlaktewater_Nederland_2025_20250915090837.csv"

    for path, description in [
        (delwaq_map_path, "DELWAQ map output"),
        (toml_path, "Ribasim model"),
        (model_gpkg_path, "Ribasim model GeoPackage"),
        (mapping_path, "Monitoring-to-node mapping"),
        (observations_path, "Monitoring data"),
        (locations_path, "Monitoring location metadata"),
    ]:
        if not path.is_file():
            raise FileNotFoundError(f"{description} not found: {path}")

    output_folder = model_path / output_subfolder
    figures_folder = output_folder / "figures"
    parameter_figure_folders = {parameter: figures_folder / parameter for parameter in PARAMETER_COMPONENTS}
    validation_factor_folder = figures_folder / "validation_factors"
    validation_scatterplot_folder = figures_folder / "validation_scatterplots"
    resources_folder = output_folder / "resources"
    output_folder.mkdir(parents=True, exist_ok=True)
    figures_folder.mkdir(exist_ok=True)
    for folder in parameter_figure_folders.values():
        folder.mkdir(exist_ok=True)
    validation_factor_folder.mkdir(exist_ok=True)
    validation_scatterplot_folder.mkdir(exist_ok=True)
    resources_folder.mkdir(exist_ok=True)

    basin_layers = gpd.list_layers(model_gpkg_path)["name"].tolist()
    basin_layer = next((name for name in basin_layers if "basin" in name.lower() and "area" in name.lower()), None)
    if basin_layer is None:
        raise ValueError(f"No Basin/area layer found in {model_gpkg_path}. Available layers: {basin_layers}")
    basins = gpd.read_file(model_gpkg_path, layer=basin_layer).to_crs(epsg=4326)
    basins = basins[basins.geometry.notna() & ~basins.geometry.is_empty]
    basins["geometry"] = basins.geometry.simplify(0.0001, preserve_topology=True)
    basin_columns = [column for column in ["node_id"] if column in basins.columns] + ["geometry"]
    basin_geojson = json.loads(basins[basin_columns].to_json(show_bbox=False))
    (output_folder / "basins.js").write_text(f"const basins = {json.dumps(basin_geojson)};", encoding="utf-8")

    model_nodes = gpd.read_file(model_gpkg_path, layer="Node").to_crs(epsg=4326)
    required_node_columns = {"meta_categorie", "name"}
    missing_node_columns = required_node_columns.difference(model_nodes.columns)
    if missing_node_columns:
        raise ValueError(f"Node layer is missing required columns: {sorted(missing_node_columns)}")
    model_nodes = model_nodes[model_nodes.geometry.notna() & ~model_nodes.geometry.is_empty]
    node_columns = [column for column in ["node_id", "name", "meta_categorie"] if column in model_nodes.columns] + [
        "geometry"
    ]
    for category, variable_name in [("RWZI", "rwziNodes"), ("buitenlandse aanvoer", "foreignInflowNodes")]:
        node_geojson = json.loads(
            model_nodes.loc[model_nodes["meta_categorie"] == category, node_columns].to_json(show_bbox=False)
        )
        filename = "rwzi_nodes.js" if category == "RWZI" else "foreign_inflow_nodes.js"
        (output_folder / filename).write_text(f"const {variable_name} = {json.dumps(node_geojson)};", encoding="utf-8")

    leaflet_version = "1.9.4"
    leaflet_js_url = f"https://unpkg.com/leaflet@{leaflet_version}/dist/leaflet.js"
    leaflet_css_url = f"https://unpkg.com/leaflet@{leaflet_version}/dist/leaflet.css"
    leaflet_js_path = resources_folder / "leaflet.js"
    leaflet_css_path = resources_folder / "leaflet.css"
    for url, path in [(leaflet_js_url, leaflet_js_path), (leaflet_css_url, leaflet_css_path)]:
        if not path.exists():
            with contextlib.suppress(Exception):
                urllib.request.urlretrieve(url, path)  # noqa: S310

    dataset = xr.open_dataset(delwaq_map_path)
    model = Model.read(toml_path)
    mapping = gpd.read_file(mapping_path).dropna(subset=["loc_Code", "node_id"])
    if mapping.crs is None:
        mapping = mapping.set_crs(epsg=28992)
    observations = pd.read_parquet(observations_path)
    location_metadata = pd.read_csv(
        locations_path,
        sep=";",
        usecols=["lokaleCode", "naam", "waterbeheerderNaam"],
    ).rename(
        columns={
            "lokaleCode": "loc_Code",
            "naam": "location_name",
            "waterbeheerderNaam": "water_authority",
        }
    )

    available_substances = {
        name.removeprefix("ribasim_")
        for name, data_array in dataset.data_vars.items()
        if name.startswith("ribasim_") and {"nTimesDlwq", "ribasim_nNodes"}.issubset(data_array.dims)
    }
    for parameter, components in PARAMETER_COMPONENTS.items():
        missing = set(components).difference(available_substances)
        if missing:
            raise ValueError(f"DELWAQ output is missing components for {parameter}: {sorted(missing)}")

    locations = (
        mapping[["loc_Code", "node_id", "geometry"]]
        .drop_duplicates(subset=["loc_Code", "node_id"])
        .merge(location_metadata, on="loc_Code", how="left")
        .sort_values("loc_Code")
    )
    locations = gpd.GeoDataFrame(locations, geometry="geometry", crs=mapping.crs).to_crs(epsg=4326)
    if location_limit is not None:
        locations = locations.head(location_limit)
    print(f"Processing {len(locations)} mapped monitoring locations.")
    observations = observations.copy()
    observations["datum"] = pd.to_datetime(observations["datum"], format="%d-%m-%Y", errors="coerce")
    model_start = pd.Timestamp(dataset["nTimesDlwq"].min().item())
    model_end = pd.Timestamp(dataset["nTimesDlwq"].max().item())
    observations = observations.loc[observations["datum"].between(model_start, model_end)].copy()

    features: list[dict] = []
    segment_cache: dict[int, int] = {}
    for location in locations.itertuples(index=False):
        node_id = int(location.node_id)
        if node_id not in model.node.df.index:
            continue

        if node_id not in segment_cache:
            node = model.node.df.loc[node_id]
            distance_squared = (dataset["ribasim_node_x"] - node.geometry.x) ** 2 + (
                dataset["ribasim_node_y"] - node.geometry.y
            ) ** 2
            segment_index = int(distance_squared.argmin().item())
            distance = float(distance_squared.isel(ribasim_nNodes=segment_index).item() ** 0.5)
            if distance > 1e-3:
                continue
            segment_cache[node_id] = segment_index

        location_observations = observations.loc[
            observations["KRW_monitoringslocatie"] == location.loc_Code
        ].sort_values("datum")
        if not all((location_observations["parameter"] == parameter).any() for parameter in PARAMETER_COMPONENTS):
            continue

        segment_index = segment_cache[node_id]
        modelled_concentrations = {
            parameter: sum(
                dataset[f"ribasim_{component}"].isel(ribasim_nNodes=segment_index) for component in components
            ).load()
            for parameter, components in PARAMETER_COMPONENTS.items()
        }
        ntot_modelled = modelled_concentrations["Ntot"]
        ntot_observed = location_observations.loc[location_observations["parameter"] == "Ntot"].set_index("datum")[
            "meetwaarde"
        ]
        ntot_seasonal_bias_percent = _mean_seasonal_relative_bias(ntot_modelled.to_series(), ntot_observed)
        validation_factors = {
            parameter: calculate_validation_factor(
                modelled_concentrations[parameter].to_series(),
                location_observations.loc[location_observations["parameter"] == parameter].set_index("datum")[
                    "meetwaarde"
                ],
            )
            for parameter in PARAMETER_COMPONENTS
        }
        mean_modelled_concentrations = {
            parameter: float(modelled_concentrations[parameter].mean().item()) for parameter in PARAMETER_COMPONENTS
        }
        figure_paths = {}
        for parameter, components in PARAMETER_COMPONENTS.items():
            figure_path = parameter_figure_folders[parameter] / f"{location.loc_Code}.png"
            if recreate_plots:
                save_comparison_plot(
                    figure_path,
                    dataset,
                    location_observations,
                    segment_index,
                    location.loc_Code,
                    location.location_name,
                    node_id,
                    parameter,
                    components,
                )
            elif not figure_path.is_file():
                raise FileNotFoundError(
                    f"Existing {parameter} plot not found: {figure_path}. Set RECREATE_PLOTS = True to create it."
                )
            figure_paths[parameter] = f"figures/{parameter}/{figure_path.name}"

        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [location.geometry.x, location.geometry.y]},
                "properties": {
                    "loc_code": location.loc_Code,
                    "location_name": location.location_name if pd.notna(location.location_name) else location.loc_Code,
                    "water_authority": location.water_authority if pd.notna(location.water_authority) else "Unknown",
                    "node_id": node_id,
                    "segment_index": segment_index,
                    "ntot_seasonal_bias_percent": ntot_seasonal_bias_percent,
                    "ntot_validation_factor": validation_factors["Ntot"],
                    "validation_factors": validation_factors,
                    "mean_modelled_concentrations": mean_modelled_concentrations,
                    "figure_paths": figure_paths,
                },
            }
        )

    dataset.close()
    if not features:
        raise ValueError(
            "No locations have a valid node mapping, DELWAQ segment, and Ntot/Ptot monitoring data in the run period."
        )

    histogram_module = importlib.reload(plot_correction_factor_histograms)
    validation_factor_paths = histogram_module.create_correction_factor_histograms(
        model_path=model_path,
        validation_path=validation_path,
        output_folder=validation_factor_folder,
        location_limit=location_limit if location_limit is not None else "all",
    )
    validation_factor_figures = {
        path.stem.removeprefix("correction_factors_krw_tussenevaluatie_"): path.relative_to(output_folder).as_posix()
        for path in validation_factor_paths
    }
    validation_factor_maxima = {
        parameter: max(
            (
                feature["properties"]["validation_factors"][parameter]
                for feature in features
                if feature["properties"]["validation_factors"][parameter] is not None
            ),
            default=None,
        )
        for parameter in PARAMETER_COMPONENTS
    }
    scatterplot_module = importlib.reload(validation_scatterplots)
    validation_scatterplot_paths = scatterplot_module.create_validation_scatterplots(
        model_path=model_path,
        validation_path=validation_path,
        output_folder=validation_scatterplot_folder,
        location_limit=location_limit if location_limit is not None else "all",
    )
    validation_scatterplot_figures = {
        path.stem.removeprefix("validation_scatterplots_"): path.relative_to(output_folder).as_posix()
        for path in validation_scatterplot_paths
    }

    features_path = output_folder / "locations.js"
    feature_collection = {"type": "FeatureCollection", "features": features}
    features_path.write_text(
        f"const locations = {json.dumps(feature_collection, ensure_ascii=False)};", encoding="utf-8"
    )
    _write_html(
        output_folder,
        leaflet_js_path.exists(),
        leaflet_css_path.exists(),
        leaflet_js_url,
        leaflet_css_url,
        model_start,
        model_end,
        validation_factor_figures,
        validation_factor_maxima,
        validation_scatterplot_figures,
    )
    print(f"Created {len(features)} location plots in {output_folder}")
    return output_folder / "index.html"


def _mean_seasonal_relative_bias(modelled: pd.Series, observed: pd.Series) -> float | None:
    """Return average absolute seasonal error as a percentage of monitored Ntot."""
    paired_seasonal_means = pd.concat(
        {
            "modelled": seasonal_means(modelled),
            "observed": seasonal_means(observed),
        },
        axis="columns",
    ).dropna()
    paired_seasonal_means = paired_seasonal_means.loc[paired_seasonal_means["observed"] != 0]
    if paired_seasonal_means.empty:
        return None
    relative_error = (
        100
        * (paired_seasonal_means["modelled"] - paired_seasonal_means["observed"]).abs()
        / paired_seasonal_means["observed"]
    )
    return float(relative_error.mean())


def _write_html(
    output_folder: Path,
    has_local_js: bool,
    has_local_css: bool,
    leaflet_js_url: str,
    leaflet_css_url: str,
    model_start: pd.Timestamp,
    model_end: pd.Timestamp,
    validation_factor_figures: dict[str, str],
    validation_factor_maxima: dict[str, float | None],
    validation_scatterplot_figures: dict[str, str],
) -> None:
    """Write the Leaflet map and popup viewer."""
    css_ref = "resources/leaflet.css" if has_local_css else leaflet_css_url
    js_ref = "resources/leaflet.js" if has_local_js else leaflet_js_url
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>DELWAQ water quality validation</title>
	<link rel="stylesheet" href="{css_ref}">
	<style>
        html, body {{ height: 100%; margin: 0; font-family: sans-serif; }}
        .app {{ display: flex; height: 100%; }}
        .sidebar {{ width: 250px; flex-shrink: 0; padding: 18px; border-right: 1px solid #d5d5d5; background: #fff; z-index: 1000; }}
        .sidebar h1 {{ margin: 0 0 5px; font-size: 16px; }}
        .sidebar p {{ margin: 0 0 18px; color: #555; font-size: 12px; line-height: 1.4; }}
        .section-title {{ margin: 18px 0 8px; padding-bottom: 4px; border-bottom: 1px solid #ddd; font-size: 12px; font-weight: bold; }}
        .layer-control {{ display: flex; align-items: center; gap: 8px; font-size: 13px; }}
        .layer-control input {{ accent-color: #1677b8; width: 16px; height: 16px; }}
        .authority-list {{ display: flex; flex-direction: column; gap: 5px; max-height: 310px; overflow-y: auto; }}
        .authority-control {{ display: flex; align-items: center; gap: 7px; font-size: 12px; }}
        .legend {{ margin-top: 18px; padding-top: 12px; border-top: 1px solid #ddd; font-size: 12px; color: #444; }}
        .legend-row {{ display: flex; align-items: center; gap: 7px; margin-top: 7px; }}
        .dot {{ width: 12px; height: 12px; border-radius: 50%; background: rgb(128 128 128 / 30%); border: 1px solid #777; }}
        .validation-factor-legend {{ margin-top: 12px; padding-top: 8px; border-top: 1px solid #ddd; }}
        .validation-factor-bullet {{ width: 12px; height: 12px; border: 1px solid #555; border-radius: 50%; }}
        .factor-very-low {{ background: #001f6b; }}
        .factor-low {{ background: #78c7ec; }}
        .factor-match {{ background: #fff; }}
        .factor-high {{ background: #ff9f1c; }}
        .factor-very-high {{ background: #b0121b; }}
        .concentration-very-low {{ background: #f7fbff; }}
        .concentration-low {{ background: #c6dbef; }}
        .concentration-medium {{ background: #6baed6; }}
        .concentration-high {{ background: #2171b5; }}
        .concentration-very-high {{ background: #08306b; }}
        .bias-labels {{ justify-content: space-between; gap: 0; }}
        .basin {{ width: 12px; height: 12px; background: #8ecae6; border: 1px solid #287da5; }}
        .rwzi {{ width: 6px; height: 6px; background: #83d4ef; border: 1px solid #287da5; }}
        .foreign-inflow {{ width: 6px; height: 6px; background: #f6c343; border: 1px solid #9c7500; }}
        .rwzi-label {{ background: transparent; border: 0; box-shadow: none; color: #155b8f; font-size: 11px; font-weight: 600; }}
        .foreign-inflow-label {{ background: transparent; border: 0; box-shadow: none; color: #806000; font-size: 11px; font-weight: 600; }}
        #map {{ flex: 1; min-width: 0; }}
        .header {{ position: absolute; z-index: 1000; top: 12px; left: calc(250px + (100% - 250px) / 2); transform: translateX(-50%); padding: 9px 12px; background: white; border-radius: 4px; box-shadow: 0 1px 5px #777; }}
		.header b {{ display: block; font-size: 14px; }}
		.header span {{ color: #555; font-size: 12px; }}
		.leaflet-popup-content {{ min-width: 720px; }}
		.popup-title {{ font-size: 14px; font-weight: bold; margin-bottom: 6px; }}
        .popup-validation-factors {{ display: flex; gap: 14px; margin-bottom: 8px; padding: 7px 9px; background: #f1f6fa; border-left: 3px solid #1677b8; font-size: 13px; }}
		.popup-meta {{ color: #555; margin-bottom: 8px; }}
		.popup-image {{ width: 700px; max-width: 100%; border: 1px solid #ddd; }}
        .chart-toggle {{ position: absolute; z-index: 1001; top: 14px; right: 16px; padding: 8px 10px; background: #fff; border: 1px solid #aaa; border-radius: 4px; cursor: pointer; font-size: 13px; }}
        .scatter-toggle {{ position: absolute; z-index: 1001; top: 52px; right: 16px; padding: 8px 10px; background: #fff; border: 1px solid #aaa; border-radius: 4px; cursor: pointer; font-size: 13px; }}
        .chart-panel {{ position: absolute; z-index: 1001; top: 90px; right: 16px; width: min(720px, calc(100% - 300px)); max-height: calc(100% - 108px); overflow: auto; padding: 12px; background: #fff; border: 1px solid #aaa; border-radius: 4px; box-shadow: 0 1px 5px #777; }}
        .chart-tabs {{ display: flex; gap: 6px; margin-bottom: 8px; }}
        .chart-tab {{ padding: 5px 10px; background: #fff; border: 1px solid #aaa; border-radius: 3px; cursor: pointer; }}
        .chart-tab.active {{ background: #1677b8; border-color: #1677b8; color: #fff; }}
        .validation-factor-image {{ display: block; width: 100%; }}
	</style>
</head>
<body>
    <div class="app">
        <aside class="sidebar">
            <h1>Water Quality</h1>
            <p>Ntot and Ptot validation<br>{model_start:%Y-%m-%d} to {model_end:%Y-%m-%d}</p>
            <div class="section-title">Graphs in popup</div>
            <label class="layer-control"><input type="checkbox" id="toggle-ntot" checked> Ntot</label>
            <label class="layer-control"><input type="checkbox" id="toggle-ptot" checked> Ptot</label>
            <div class="section-title">Water authorities</div>
            <div id="authority-list" class="authority-list"></div>
            <div class="section-title">Map layers</div>
            <label class="layer-control"><input type="checkbox" id="toggle-basins" checked> Basin areas</label>
            <label class="layer-control"><input type="checkbox" id="toggle-rwzi" checked> RWZI</label>
            <label class="layer-control"><input type="checkbox" id="toggle-foreign-inflow" checked> Buitenlandse Aanvoer</label>
            <div class="legend"><b>Legend</b><div class="legend-row"><span class="dot"></span> Monitoring location</div><div class="legend-row"><span class="basin"></span> Ribasim basin</div><div class="legend-row"><span class="rwzi"></span> RWZI</div><div class="legend-row"><span class="foreign-inflow"></span> Buitenlandse Aanvoer</div><div class="validation-factor-legend"><label for="marker-colouring"><b>Marker colouring</b></label><select id="marker-colouring"><option value="ratio-Ntot">Validation factor Ntot</option><option value="ratio-Ptot">Validation factor Ptot</option><option value="mean-Ntot">Mean concentration Ntot</option><option value="mean-Ptot">Mean concentration Ptot</option></select><div id="marker-colouring-legend"></div></div></div>
        </aside>
        <div id="map"></div>
    </div>
    <div class="header"><b>DELWAQ Water Quality Validation</b><span>Click a location marker to see Ntot and Ptot</span></div>
    <button id="chart-toggle" class="chart-toggle" type="button" aria-expanded="false">Cobserved / Cmodelled charts</button>
    <button id="scatter-toggle" class="scatter-toggle" type="button" aria-expanded="false">Validation scatter plots</button>
    <section id="chart-panel" class="chart-panel" hidden>
        <div class="chart-tabs"><button class="chart-tab active" type="button" data-parameter="Ntot">Ntot</button><button class="chart-tab" type="button" data-parameter="Ptot">Ptot</button></div>
        <img id="validation-factor-image" class="validation-factor-image" alt="Validation-factor histogram">
    </section>
    <section id="scatter-panel" class="chart-panel" hidden>
        <div class="chart-tabs"><button class="scatter-tab active" type="button" data-parameter="Ntot">Ntot</button><button class="scatter-tab" type="button" data-parameter="Ptot">Ptot</button></div>
        <img id="validation-scatterplot-image" class="validation-factor-image" alt="Validation scatter plot">
    </section>
    <script src="{js_ref}"></script>
    <script src="basins.js"></script>
    <script src="rwzi_nodes.js"></script>
    <script src="foreign_inflow_nodes.js"></script>
    <script src="locations.js"></script>
	<script>
        const validationFactorFigures = {json.dumps(validation_factor_figures)};
        const validationFactorMaxima = {json.dumps(validation_factor_maxima)};
        const validationScatterplotFigures = {json.dumps(validation_scatterplot_figures)};
        const map = L.map('map');
        L.tileLayer('https://service.pdok.nl/brt/achtergrondkaart/wmts/v2_0/standaard/EPSG:3857/{{z}}/{{x}}/{{y}}.png', {{
            maxZoom: 19,
            attribution: '&copy; <a href="https://www.pdok.nl">PDOK</a> - BRT Achtergrondkaart',
        }}).addTo(map);
        map.createPane('basinPane');
        map.getPane('basinPane').style.zIndex = 410;
        map.createPane('markerPane');
        map.getPane('markerPane').style.zIndex = 420;
        map.createPane('modelNodePane');
        map.getPane('modelNodePane').style.zIndex = 415;
        const basinLayer = L.geoJSON(basins, {{ pane: 'basinPane', interactive: false, style: {{ color: '#287da5', weight: 1, fillColor: '#8ecae6', fillOpacity: 0.22 }} }}).addTo(map);
        const rwziLayer = L.geoJSON(rwziNodes, {{
            pane: 'modelNodePane',
            pointToLayer: (feature, latlng) => L.marker(latlng, {{ pane: 'modelNodePane', icon: L.divIcon({{ className: 'rwzi-label', html: `<span style="display:inline-block;width:6px;height:6px;background:#83d4ef;border:1px solid #287da5;margin-right:5px;vertical-align:middle"></span>${{feature.properties.name || 'RWZI'}}` }}) }}),
        }}).addTo(map);
        const foreignInflowLayer = L.geoJSON(foreignInflowNodes, {{
            pane: 'modelNodePane',
            pointToLayer: (feature, latlng) => L.marker(latlng, {{ pane: 'modelNodePane', icon: L.divIcon({{ className: 'foreign-inflow-label', html: `<span style="display:inline-block;width:6px;height:6px;background:#f6c343;border:1px solid #9c7500;margin-right:5px;vertical-align:middle"></span>${{feature.properties.name || 'Buitenlandse Aanvoer'}}` }}) }}),
        }}).addTo(map);
        const markerLayer = L.layerGroup([], {{ pane: 'markerPane' }}).addTo(map);
        function validationFactorColor(factor) {{
            if (factor === null || !Number.isFinite(factor)) return '#808080';
            if (factor < 0.5) return '#001f6b';
            if (factor < 0.9) return '#78c7ec';
            if (factor <= 1.1) return '#ffffff';
            if (factor <= 2) return '#ff9f1c';
            return '#b0121b';
        }}
        const concentrationColors = ['#f7fbff', '#c6dbef', '#6baed6', '#2171b5', '#08306b'];
        function concentrationBreaks(parameter) {{
            const values = markers.map((entry) => entry.props.mean_modelled_concentrations[parameter]).filter(Number.isFinite).sort((first, second) => first - second);
            if (!values.length) return [];
            return [0, 1, 2, 3, 4, 5].map((index) => values[Math.round(index * (values.length - 1) / 5)]);
        }}
        function concentrationColor(value, breaks) {{
            if (!Number.isFinite(value) || !breaks.length) return '#808080';
            return concentrationColors.find((color, index) => value <= breaks[index + 1]) || concentrationColors.at(-1);
        }}
        const markers = [];
		const bounds = [];
		for (const feature of locations.features) {{
			const [longitude, latitude] = feature.geometry.coordinates;
			const props = feature.properties;
            const marker = L.circleMarker([latitude, longitude], {{ pane: 'markerPane', radius: 7, color: '#555', fillColor: validationFactorColor(props.validation_factors.Ntot), fillOpacity: 0.9, weight: 1 }});
            marker.bindPopup(makePopup(props), {{ maxWidth: 760 }});
			marker.addTo(markerLayer);
            markers.push({{ marker, props }});
			bounds.push([latitude, longitude]);
		}}
        map.fitBounds(bounds, {{ padding: [30, 30] }});
        function makePopup(props) {{
            const showNtot = document.getElementById('toggle-ntot').checked;
            const showPtot = document.getElementById('toggle-ptot').checked;
            const figures = [];
            if (showNtot) figures.push(`<img class="popup-image" src="${{props.figure_paths.Ntot}}" alt="Ntot comparison plot">`);
            if (showPtot) figures.push(`<img class="popup-image" src="${{props.figure_paths.Ptot}}" alt="Ptot comparison plot">`);
            const validationFactor = (parameter) => {{
                const factor = props.validation_factors[parameter];
                return factor === null ? 'No comparable annual means' : factor.toFixed(3);
            }};
            return `<div class="popup-title">${{props.location_name}}</div><div class="popup-validation-factors"><span><b>Ntot validation factor:</b> ${{validationFactor('Ntot')}}</span><span><b>Ptot validation factor:</b> ${{validationFactor('Ptot')}}</span></div><div class="popup-meta">${{props.loc_code}} | ${{props.water_authority}} | Ribasim node ${{props.node_id}} | DELWAQ segment ${{props.segment_index}}</div>${{figures.join('')}}`;
        }}
        function updatePopups() {{ markers.forEach((entry) => entry.marker.setPopupContent(makePopup(entry.props))); }}
        document.getElementById('toggle-ntot').addEventListener('change', updatePopups);
        document.getElementById('toggle-ptot').addEventListener('change', updatePopups);
        const chartToggle = document.getElementById('chart-toggle');
        const chartPanel = document.getElementById('chart-panel');
        const scatterToggle = document.getElementById('scatter-toggle');
        const scatterPanel = document.getElementById('scatter-panel');
        const validationFactorImage = document.getElementById('validation-factor-image');
        const validationScatterplotImage = document.getElementById('validation-scatterplot-image');
        const markerColouring = document.getElementById('marker-colouring');
        const markerColouringLegend = document.getElementById('marker-colouring-legend');
        function updateMarkerColouring() {{
            const [kind, parameter] = markerColouring.value.split('-');
            if (kind === 'ratio') {{
                const maximum = validationFactorMaxima[parameter];
                markerColouringLegend.innerHTML = `<b>${{parameter}} validation factor: Cobserved / Cmodelled</b><div class="legend-row"><span class="validation-factor-bullet factor-very-low"></span> 0.07 to 0.5</div><div class="legend-row"><span class="validation-factor-bullet factor-low"></span> 0.5 to 0.9</div><div class="legend-row"><span class="validation-factor-bullet factor-match"></span> 0.9 to 1.1</div><div class="legend-row"><span class="validation-factor-bullet factor-high"></span> 1.1 to 2</div><div class="legend-row"><span class="validation-factor-bullet factor-very-high"></span> ${{maximum === null ? 'No data' : `2 to ${{maximum.toFixed(3)}}`}}</div>`;
                markers.forEach((entry) => entry.marker.setStyle({{ fillColor: validationFactorColor(entry.props.validation_factors[parameter]) }}));
                return;
            }}
            const breaks = concentrationBreaks(parameter);
            markerColouringLegend.innerHTML = `<b>${{parameter}} mean modelled concentration</b>${{concentrationColors.map((color, index) => `<div class="legend-row"><span class="validation-factor-bullet" style="background:${{color}}"></span> ${{breaks.length ? `${{breaks[index].toFixed(2)}} to ${{breaks[index + 1].toFixed(2)}}` : 'No data'}}</div>`).join('')}}`;
            markers.forEach((entry) => entry.marker.setStyle({{ fillColor: concentrationColor(entry.props.mean_modelled_concentrations[parameter], breaks) }}));
        }}
        markerColouring.addEventListener('change', updateMarkerColouring);
        updateMarkerColouring();
        function showValidationFactorFigure(parameter) {{
            validationFactorImage.src = validationFactorFigures[parameter];
            document.querySelectorAll('.chart-tab').forEach((tab) => tab.classList.toggle('active', tab.dataset.parameter === parameter));
        }}
        chartToggle.addEventListener('click', () => {{
            chartPanel.hidden = !chartPanel.hidden;
            chartToggle.setAttribute('aria-expanded', String(!chartPanel.hidden));
            if (!chartPanel.hidden) {{ scatterPanel.hidden = true; scatterToggle.setAttribute('aria-expanded', 'false'); }}
        }});
        document.querySelectorAll('.chart-tab').forEach((tab) => tab.addEventListener('click', () => showValidationFactorFigure(tab.dataset.parameter)));
        showValidationFactorFigure('Ntot');
        function showValidationScatterplot(parameter) {{
            validationScatterplotImage.src = validationScatterplotFigures[parameter];
            document.querySelectorAll('.scatter-tab').forEach((tab) => tab.classList.toggle('active', tab.dataset.parameter === parameter));
        }}
        scatterToggle.addEventListener('click', () => {{
            scatterPanel.hidden = !scatterPanel.hidden;
            scatterToggle.setAttribute('aria-expanded', String(!scatterPanel.hidden));
            if (!scatterPanel.hidden) {{ chartPanel.hidden = true; chartToggle.setAttribute('aria-expanded', 'false'); }}
        }});
        document.querySelectorAll('.scatter-tab').forEach((tab) => tab.addEventListener('click', () => showValidationScatterplot(tab.dataset.parameter)));
        showValidationScatterplot('Ntot');
        const authorities = [...new Set(markers.map((entry) => entry.props.water_authority))].sort();
        const authorityList = document.getElementById('authority-list');
        for (const authority of authorities) {{
            const label = document.createElement('label');
            label.className = 'authority-control';
            label.innerHTML = `<input type="checkbox" value="${{authority}}" checked> ${{authority}}`;
            authorityList.appendChild(label);
        }}
        function updateAuthorityFilter() {{
            const active = new Set([...authorityList.querySelectorAll('input:checked')].map((input) => input.value));
            markers.forEach((entry) => {{
                if (active.has(entry.props.water_authority)) {{ entry.marker.addTo(markerLayer); }} else {{ markerLayer.removeLayer(entry.marker); }}
            }});
        }}
        authorityList.addEventListener('change', updateAuthorityFilter);
        document.getElementById('toggle-basins').addEventListener('change', (event) => {{
            if (event.target.checked) {{ basinLayer.addTo(map); }} else {{ map.removeLayer(basinLayer); }}
        }});
        document.getElementById('toggle-rwzi').addEventListener('change', (event) => {{
            if (event.target.checked) {{ rwziLayer.addTo(map); }} else {{ map.removeLayer(rwziLayer); }}
        }});
        document.getElementById('toggle-foreign-inflow').addEventListener('change', (event) => {{
            if (event.target.checked) {{ foreignInflowLayer.addTo(map); }} else {{ map.removeLayer(foreignInflowLayer); }}
        }});
	</script>
</body>
</html>"""
    (output_folder / "index.html").write_text(html, encoding="utf-8")


if __name__ == "__main__":
    create_waterquality_html_viewer()
