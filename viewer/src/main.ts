import "maplibre-gl/dist/maplibre-gl.css";
import "uplot/dist/uPlot.min.css";
import "./style.css";

import type { PickingInfo } from "@deck.gl/core";
import { PathLayer, ScatterplotLayer } from "@deck.gl/layers";
import { MapboxOverlay } from "@deck.gl/mapbox";
import * as maplibregl from "maplibre-gl";
// maplibre resolves its worker at runtime, which bundlers cannot follow
import maplibreWorkerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url";
import { Protocol } from "pmtiles";
import { fileUrl, loadManifest, type Manifest } from "./data";
import { loadNetwork, type Group, type Network } from "./network";
import { Panel, type Selection } from "./panel";

type Color = [number, number, number];

const NODE_COLORS: Record<string, Color> = {
  Basin: [31, 120, 180],
  ContinuousControl: [90, 90, 90],
  DiscreteControl: [40, 40, 40],
  FlowBoundary: [177, 89, 40],
  FlowDemand: [231, 41, 138],
  Junction: [150, 150, 150],
  LevelBoundary: [0, 160, 160],
  LevelDemand: [102, 166, 30],
  LinearResistance: [253, 191, 111],
  ManningResistance: [255, 127, 0],
  Outlet: [106, 61, 154],
  PidControl: [90, 90, 90],
  Pump: [227, 26, 28],
  TabulatedRatingCurve: [51, 160, 44],
  Terminal: [60, 60, 60],
  UserDemand: [230, 171, 2],
};
const LINK_COLORS: Record<string, Color> = {
  flow: [70, 70, 70],
  control: [200, 60, 60],
  listen: [110, 110, 220],
};
const DEFAULT_COLOR: Color = [128, 128, 128];
const HIDDEN_BY_DEFAULT = new Set(["ContinuousControl", "DiscreteControl", "PidControl", "control", "listen"]);
const HIGHLIGHT: [number, number, number, number] = [255, 140, 0, 255];
const BASEMAP = "https://service.pdok.nl/brt/achtergrondkaart/wmts/v2_0/grijs/EPSG:3857/{z}/{x}/{y}.png";

function el<K extends keyof HTMLElementTagNameMap>(tag: K, className = "", text = ""): HTMLElementTagNameMap[K] {
  const element = document.createElement(tag);
  element.className = className;
  element.textContent = text;
  return element;
}

function hashParams(): URLSearchParams {
  return new URLSearchParams(location.hash.slice(1));
}

function selectionFromHash(): Selection | null {
  const params = hashParams();
  for (const kind of ["node", "link"] as const) {
    const id = Number(params.get(kind));
    if (params.has(kind) && Number.isInteger(id)) return { kind, id };
  }
  return null;
}

function writeSelectionToHash(selection: Selection | null): void {
  const params = hashParams();
  params.delete("node");
  params.delete("link");
  if (selection) params.set(selection.kind, String(selection.id));
  // Keep MapLibre's `map=zoom/lat/lon` readable instead of percent-encoded
  history.replaceState(history.state, "", `#${params.toString().replaceAll("%2F", "/")}`);
}

class Viewer {
  private readonly visible = new Map<string, boolean>();
  private selection: Selection | null = null;
  private readonly overlay: MapboxOverlay;
  private readonly panel: Panel;

  constructor(
    private readonly map: maplibregl.Map,
    private readonly root: HTMLElement,
    private readonly manifest: Manifest,
    private readonly network: Network,
  ) {
    for (const group of [...network.nodeGroups, ...network.linkGroups]) {
      this.visible.set(group.type, !HIDDEN_BY_DEFAULT.has(group.type));
    }
    this.visible.set("basin_area", true);
    this.visible.set("waterboards", true);

    // Interleaved mode reads `map.transform`, which maplibre-gl 6 no longer exposes
    this.overlay = new MapboxOverlay({ interleaved: false, getTooltip: (info) => this.tooltip(info) });
    map.addControl(this.overlay);
    this.panel = new Panel(root, manifest, network, {
      select: (selection) => this.select(selection),
      zoomTo: (selection) => this.zoomTo(selection),
      close: () => this.select(null),
    });
    this.addMapLayers();
    this.addLayerControl();
    this.addSearch();
    map.on("click", (event: maplibregl.MapMouseEvent) => this.onClick(event));
    this.render();

    const selection = selectionFromHash();
    if (selection) {
      this.select(selection);
      if (!hashParams().has("map")) this.zoomTo(selection);
    }
  }

  private addMapLayers(): void {
    const { map, manifest } = this;
    map.addSource("basin_area", {
      type: "vector",
      url: `pmtiles://${fileUrl(manifest.files.basin_area)}`,
      promoteId: { basin_area: "node_id" },
    });
    map.addLayer({
      id: "basin_area",
      type: "fill",
      source: "basin_area",
      "source-layer": "basin_area",
      paint: {
        "fill-color": ["case", ["boolean", ["feature-state", "selected"], false], "#ff8c00", "#6fa8dc"],
        "fill-opacity": ["case", ["boolean", ["feature-state", "selected"], false], 0.4, 0.15],
      },
    });
    map.addLayer({
      id: "basin_area_outline",
      type: "line",
      source: "basin_area",
      "source-layer": "basin_area",
      minzoom: 9,
      paint: { "line-color": "#3d85c6", "line-width": 0.5 },
    });
    map.addSource("waterboards", { type: "geojson", data: fileUrl(manifest.files.waterboards) });
    map.addLayer({
      id: "waterboards",
      type: "line",
      source: "waterboards",
      paint: { "line-color": "#333", "line-width": 1.5, "line-dasharray": [4, 2] },
    });
  }

  private nodeLayer(group: Group) {
    const selected =
      this.selection?.kind === "node" ? this.network.nodeRow.get(this.selection.id) : undefined;
    const isSelected = selected !== undefined && this.network.nodeType[selected] === group.type;
    return new ScatterplotLayer({
      id: `node-${group.type}`,
      data: { length: group.rows.length, attributes: { getPosition: { value: group.positions, size: 2 } } },
      visible: this.visible.get(group.type),
      getFillColor: NODE_COLORS[group.type] ?? DEFAULT_COLOR,
      getLineColor: [255, 255, 255],
      stroked: true,
      lineWidthUnits: "pixels",
      getLineWidth: 0.5,
      radiusUnits: "meters",
      getRadius: group.type === "Basin" ? 60 : 40,
      radiusMinPixels: 1.5,
      radiusMaxPixels: group.type === "Basin" ? 6 : 5,
      pickable: true,
      autoHighlight: true,
      highlightColor: HIGHLIGHT,
      highlightedObjectIndex: isSelected ? this.network.nodeLocal[selected] : -1,
    });
  }

  private linkLayer(group: Group) {
    const selected =
      this.selection?.kind === "link" ? this.network.linkRow.get(this.selection.id) : undefined;
    const isSelected = selected !== undefined && this.network.linkType[selected] === group.type;
    return new PathLayer({
      id: `link-${group.type}`,
      data: {
        length: group.rows.length,
        startIndices: group.startIndices,
        attributes: { getPath: { value: group.positions, size: 2 } },
      },
      _pathType: "open",
      positionFormat: "XY",
      visible: this.visible.get(group.type),
      getColor: LINK_COLORS[group.type] ?? DEFAULT_COLOR,
      widthUnits: "pixels",
      getWidth: group.type === "flow" ? 1.5 : 1,
      widthMinPixels: 1,
      pickable: true,
      autoHighlight: true,
      highlightColor: HIGHLIGHT,
      highlightedObjectIndex: isSelected ? this.network.linkLocal[selected] : -1,
    });
  }

  private render(): void {
    const { network } = this;
    this.overlay.setProps({
      layers: [...network.linkGroups.map((g) => this.linkLayer(g)), ...network.nodeGroups.map((g) => this.nodeLayer(g))],
    });
    for (const id of ["basin_area", "basin_area_outline"]) {
      this.map.setLayoutProperty(id, "visibility", this.visible.get("basin_area") ? "visible" : "none");
    }
    this.map.setLayoutProperty("waterboards", "visibility", this.visible.get("waterboards") ? "visible" : "none");
  }

  /** Map a deck.gl pick back to a node or link. */
  private picked(info: PickingInfo): Selection | null {
    if (!info.layer || info.index < 0) return null;
    const [kind, type] = info.layer.id.split(/-(.*)/s);
    if (kind === "node") {
      const group = this.network.nodeGroups.find((g) => g.type === type);
      return group ? { kind, id: this.network.nodeId[group.rows[info.index]] } : null;
    }
    const group = this.network.linkGroups.find((g) => g.type === type);
    return group ? { kind: "link", id: this.network.linkId[group.rows[info.index]] } : null;
  }

  private tooltip(info: PickingInfo) {
    const selection = this.picked(info);
    if (!selection) return null;
    const { network } = this;
    if (selection.kind === "node") {
      return { text: `${network.nodeType[network.nodeRow.get(selection.id)!]} #${selection.id}` };
    }
    const row = network.linkRow.get(selection.id)!;
    return { text: `${network.linkType[row]} link #${selection.id}\n${network.fromNodeId[row]} → ${network.toNodeId[row]}` };
  }

  private onClick(event: maplibregl.MapMouseEvent): void {
    const info = this.overlay.pickObject({ x: event.point.x, y: event.point.y, radius: 4 });
    const picked = info ? this.picked(info) : null;
    if (picked) return this.select(picked);
    if (this.visible.get("basin_area")) {
      const [area] = this.map.queryRenderedFeatures(event.point, { layers: ["basin_area"] });
      if (area?.id !== undefined) return this.select({ kind: "node", id: Number(area.id) });
    }
    this.select(null);
  }

  select(selection: Selection | null): void {
    const previous = this.selection;
    if (previous?.kind === "node") {
      this.map.setFeatureState({ source: "basin_area", sourceLayer: "basin_area", id: previous.id }, { selected: false });
    }
    this.selection = selection;
    if (selection?.kind === "node") {
      this.map.setFeatureState({ source: "basin_area", sourceLayer: "basin_area", id: selection.id }, { selected: true });
    }
    writeSelectionToHash(selection);
    this.render();
    if (selection) this.panel.show(selection);
    else this.panel.hide();
  }

  zoomTo(selection: Selection): void {
    const { network } = this;
    if (selection.kind === "node") {
      const row = network.nodeRow.get(selection.id);
      if (row === undefined) return;
      this.map.flyTo({ center: [network.nodeX[row], network.nodeY[row]], zoom: Math.max(this.map.getZoom(), 14) });
      return;
    }
    const row = network.linkRow.get(selection.id);
    if (row === undefined) return;
    const bounds = new maplibregl.LngLatBounds();
    for (const nodeId of [network.fromNodeId[row], network.toNodeId[row]]) {
      const nodeRow = network.nodeRow.get(nodeId);
      if (nodeRow !== undefined) bounds.extend([network.nodeX[nodeRow], network.nodeY[nodeRow]]);
    }
    if (!bounds.isEmpty()) this.map.fitBounds(bounds, { padding: 80, maxZoom: 15 });
  }

  private checkbox(key: string, label: string, color?: Color, count?: number): HTMLLabelElement {
    const input = el("input");
    input.type = "checkbox";
    input.checked = this.visible.get(key) ?? false;
    input.addEventListener("change", () => {
      this.visible.set(key, input.checked);
      this.render();
    });
    const row = el("label");
    row.append(input);
    if (color) {
      const swatch = el("span", "swatch");
      swatch.style.background = `rgb(${color.join(",")})`;
      row.append(swatch);
    }
    row.append(count === undefined ? label : `${label} (${count.toLocaleString()})`);
    return row;
  }

  private addLayerControl(): void {
    const { network, manifest } = this;
    const details = el("details", "layers");
    details.open = true;
    details.append(el("summary", "", "Layers"));
    details.append(el("h4", "", "Nodes"));
    for (const group of network.nodeGroups) {
      details.append(this.checkbox(group.type, group.type, NODE_COLORS[group.type] ?? DEFAULT_COLOR, group.rows.length));
    }
    details.append(el("h4", "", "Links"));
    for (const group of network.linkGroups) {
      details.append(this.checkbox(group.type, group.type, LINK_COLORS[group.type] ?? DEFAULT_COLOR, group.rows.length));
    }
    details.append(el("h4", "", "Areas"));
    details.append(this.checkbox("basin_area", "Basin / area"), this.checkbox("waterboards", "Water boards"));
    const version = manifest.ribasim_version ? `, Ribasim ${manifest.ribasim_version}` : "";
    details.append(el("p", "note", `${manifest.model}${version}`));
    this.root.append(details);
  }

  private addSearch(): void {
    const input = el("input", "search");
    input.type = "search";
    input.placeholder = "Node or link id";
    input.addEventListener("keydown", (event) => {
      if (event.key !== "Enter") return;
      const id = Number(input.value.trim());
      const kind = this.network.nodeRow.has(id) ? "node" : this.network.linkRow.has(id) ? "link" : null;
      input.classList.toggle("not-found", kind === null);
      if (kind === null) return;
      this.select({ kind, id });
      this.zoomTo({ kind, id });
    });
    this.root.append(input);
  }
}

async function main(): Promise<void> {
  const root = document.getElementById("ribasim-viewer");
  if (!root) throw new Error("Missing #ribasim-viewer element");
  root.classList.add("ribasim-viewer");
  const status = el("div", "status", "Loading model…");
  const container = el("div", "map");
  root.append(container, status);

  try {
    maplibregl.setWorkerUrl(maplibreWorkerUrl);
    const protocol = new Protocol();
    maplibregl.addProtocol("pmtiles", protocol.tile);
    const manifest = await loadManifest();
    const map = new maplibregl.Map({
      container,
      hash: "map",
      bounds: hashParams().has("map") ? undefined : manifest.bounds,
      fitBoundsOptions: { padding: 20 },
      style: {
        version: 8,
        sources: {
          basemap: {
            type: "raster",
            tiles: [BASEMAP],
            tileSize: 256,
            maxzoom: 19,
            attribution: 'Kaartgegevens &copy; <a href="https://www.kadaster.nl">Kadaster</a>',
          },
        },
        layers: [{ id: "basemap", type: "raster", source: "basemap" }],
      },
    });
    map.addControl(new maplibregl.NavigationControl(), "top-right");
    map.addControl(new maplibregl.ScaleControl(), "bottom-left");
    const [network] = await Promise.all([loadNetwork(manifest), map.once("load")]);
    new Viewer(map, root, manifest, network);
    status.remove();
  } catch (error) {
    status.textContent = `Failed to load the model viewer: ${error}`;
    status.classList.add("error");
    throw error;
  }
}

main();
