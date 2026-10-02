import "maplibre-gl/dist/maplibre-gl.css";
import "uplot/dist/uPlot.min.css";
import "./style.css";

import type { PickingInfo } from "@deck.gl/core";
import { PathStyleExtension } from "@deck.gl/extensions";
import { IconLayer, PathLayer, ScatterplotLayer, TextLayer } from "@deck.gl/layers";
import { MapboxOverlay } from "@deck.gl/mapbox";
import * as maplibregl from "maplibre-gl";
// maplibre resolves its worker at runtime, which bundlers cannot follow
import maplibreWorkerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url";
import { Protocol } from "pmtiles";
import { BASEMAPS, basemapStyle, DEFAULT_BASEMAP, withOverlays } from "./basemaps";
import { ColorScale, formatNumber, normalize } from "./colors";
import { fileUrl, loadManifest, type Manifest } from "./data";
import { loadNetwork, type Group, type Network } from "./network";
import { Panel, type Selection } from "./panel";
import { ResultFrames } from "./results";
import {
  ARROW_ICON,
  ARROW_SIZE_PX,
  DEFAULT_LINK_STYLE,
  DEFAULT_NODE_COLOR,
  LINK_STYLES,
  LINK_WIDTH_PX,
  loadIconAtlas,
  NODE_ICON_SIZE_PX,
  NODE_STYLES,
  nodeIconPng,
  type Color,
  type IconAtlas,
} from "./styles";
import { TimeBar, type FlowStyle } from "./timebar";

const HIDDEN_BY_DEFAULT = new Set(["ContinuousControl", "DiscreteControl", "PidControl", "control", "listen"]);
const HIGHLIGHT: [number, number, number, number] = [255, 140, 0, 255];
const NO_DATA: [number, number, number, number] = [170, 170, 170, 255];
const DASHED = [new PathStyleExtension({ dash: true })];
const DASH_ARRAY: [number, number] = [4, 2];
// Icons shrink with the map below this size in meters, so dense areas stay readable when zoomed out
const NODE_ICON_SIZE_M = 320;
const ARROW_SIZE_M = 60;
// Link widths in pixels for the smallest and largest flow in the width style; the color scale is
// logarithmic over orders of magnitude, so a cubic ramp keeps all but the larger flows thin
const FLOW_WIDTH_PX: [number, number] = [1, 8];
const FLOW_WIDTH_POWER = 3;
const NO_FLOW_WIDTH_PX = 0.5;
// Details that are too small to see when zoomed out, and slow to draw
const ARROW_MIN_ZOOM = 10;
const BASIN_OUTLINE_MIN_ZOOM = 9;
const LABEL_MIN_ZOOM = 13;
const MAX_LABELS = 2000;
// Sources and layers kept when the base map style changes
const OVERLAY_SOURCES = new Set(["basin_area", "waterboards"]);

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

function writeHash(updates: Record<string, string | null>): void {
  const params = hashParams();
  for (const [key, value] of Object.entries(updates)) {
    if (value === null) params.delete(key);
    else params.set(key, value);
  }
  // Keep MapLibre's `map=zoom/lat/lon` readable instead of percent-encoded
  history.replaceState(history.state, "", `#${params.toString().replaceAll("%2F", "/")}`);
}

function writeSelectionToHash(selection: Selection | null): void {
  writeHash({
    node: selection?.kind === "node" ? String(selection.id) : null,
    link: selection?.kind === "link" ? String(selection.id) : null,
  });
}

/** Per-feature colors, widths and values of the current time step, indexed like the features of a layer group. */
interface Coloring {
  colors: Uint8Array;
  /** Line widths in pixels for the width style */
  widths: Float32Array;
  values: Float32Array;
  version: number;
}

function coloring(scale: ColorScale, values: Float32Array, local: Int32Array, length: number, version: number): Coloring {
  const colors = new Uint8Array(length * 4);
  for (let i = 0; i < length; i++) colors.set(NO_DATA, i * 4);
  const widths = new Float32Array(length).fill(NO_FLOW_WIDTH_PX);
  const localValues = new Float32Array(length).fill(Number.NaN);
  const [minWidth, maxWidth] = FLOW_WIDTH_PX;
  values.forEach((value, i) => {
    const index = local[i];
    if (index < 0) return;
    localValues[index] = value;
    if (!scale.write(value, colors, index * 4)) colors.set(NO_DATA, index * 4);
    const t = normalize(scale.variable, value);
    if (!Number.isNaN(t)) widths[index] = minWidth + t ** FLOW_WIDTH_POWER * (maxWidth - minWidth);
  });
  return { colors, widths, values: localValues, version };
}

interface Label {
  position: [number, number];
  text: string;
}

class Viewer {
  private readonly visible = new Map<string, boolean>();
  private basemap = DEFAULT_BASEMAP;
  private basinAreaColors: { scale: ColorScale; values: Float32Array } | null = null;
  private selection: Selection | null = null;
  private readonly overlay: MapboxOverlay;
  private readonly panel: Panel;
  private frames: ResultFrames | null = null;
  private timeBar: TimeBar | null = null;
  /** Result index to the index in the "flow" link group and "Basin" node group, or -1 */
  private resultIndex: { flow: Int32Array; basin: Int32Array; basinIds: Int32Array } | null = null;
  private flowColoring: Coloring | null = null;
  private basinColoring: Coloring | null = null;
  private basinAreasColored = false;
  private frameToken = 0;
  private labels: Label[] = [];
  /** Which zoom-dependent details are shown, to redraw only when that changes */
  private detail = "";

  constructor(
    private readonly map: maplibregl.Map,
    private readonly root: HTMLElement,
    private readonly manifest: Manifest,
    private readonly network: Network,
    private readonly icons: IconAtlas,
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
    // Search and layer list share a column, so the search choices push the list down
    const sidebar = el("div", "sidebar");
    sidebar.append(...this.searchControl(), this.layerControl());
    root.append(sidebar);
    map.on("click", (event: maplibregl.MapMouseEvent) => this.onClick(event));
    map.on("zoom", () => {
      if (this.detail !== this.zoomDetail()) this.render();
    });
    map.on("moveend", () => {
      this.updateLabels();
      this.render();
    });
    this.render();
  }

  async initResults(): Promise<void> {
    if (!this.manifest.results) return;
    const results = this.manifest.results!;
    const frames = new ResultFrames(results);
    const { network } = this;
    const [flowIds, basinIds] = await Promise.all([frames.featureIds("flow"), frames.featureIds("basin")]);
    const flow = flowIds.map((id) => {
      const row = network.linkRow.get(id);
      return row !== undefined && network.linkType[row] === "flow" ? network.linkLocal[row] : -1;
    });
    const basin = basinIds.map((id) => {
      const row = network.nodeRow.get(id);
      return row !== undefined && network.nodeType[row] === "Basin" ? network.nodeLocal[row] : -1;
    });
    this.resultIndex = { flow, basin, basinIds };
    this.frames = frames;

    const date = hashParams().get("t");
    const step = frames.times.findIndex((time) => time.toISOString().slice(0, 10) === date);
    this.timeBar = new TimeBar(this.root, results, frames.times, { step: Math.max(step, 0) }, () =>
      this.updateResults(),
    );
    await this.updateResults();
  }

  private groupLength(kind: "flow" | "basin"): number {
    const groups = kind === "flow" ? this.network.linkGroups : this.network.nodeGroups;
    return groups.find((group) => group.type === (kind === "flow" ? "flow" : "Basin"))?.rows.length ?? 0;
  }

  private async updateResults(): Promise<void> {
    const { frames, timeBar, resultIndex } = this;
    if (!frames || !timeBar || !resultIndex) return;
    const results = this.manifest.results!;
    const token = ++this.frameToken;
    const { step, basin, flow } = timeBar.state;
    const [flowValues, basinValues] = await Promise.all([
      flow ? frames.frame("flow", flow, step) : null,
      basin ? frames.frame("basin", basin, step) : null,
    ]);
    if (token !== this.frameToken) return;

    // The variables have the color scale limits set in the time bar
    const flowVariable = timeBar.variable("flow");
    const basinVariable = timeBar.variable("basin");
    const flowScale = flowVariable ? new ColorScale(flowVariable) : null;
    const basinScale = basinVariable ? new ColorScale(basinVariable) : null;
    this.flowColoring =
      flowScale && flowValues
        ? coloring(flowScale, flowValues, resultIndex.flow, this.groupLength("flow"), token)
        : null;
    this.basinColoring =
      basinScale && basinValues
        ? coloring(basinScale, basinValues, resultIndex.basin, this.groupLength("basin"), token)
        : null;
    this.colorBasinAreas(basinScale, basinValues);
    writeHash({ t: frames.times[step].toISOString().slice(0, 10) });
    this.panel.setStep(step);
    this.updateLabels();
    this.render();

    const next = step + results.steps_per_row_group;
    if (flow) frames.prefetch("flow", flow, next);
    if (basin) frames.prefetch("basin", basin, next);
  }

  private colorBasinAreas(scale: ColorScale | null, values: Float32Array | null): void {
    const target = { source: "basin_area", sourceLayer: "basin_area" };
    this.basinAreaColors = scale && values ? { scale, values } : null;
    if (!scale || !values) {
      if (!this.basinAreasColored) return;
      this.map.removeFeatureState(target);
      this.basinAreasColored = false;
      if (this.selection?.kind === "node") this.map.setFeatureState({ ...target, id: this.selection.id }, { selected: true });
      return;
    }
    this.resultIndex!.basinIds.forEach((id, i) => this.map.setFeatureState({ ...target, id }, { color: scale.css(values[i]) }));
    this.basinAreasColored = true;
  }

  private async setBasemap(id: string): Promise<void> {
    this.basemap = id;
    const style = await basemapStyle(id);
    if (this.basemap !== id) return;
    this.map.setStyle(style, { transformStyle: (current, next) => withOverlays(current, next, OVERLAY_SOURCES) });
    // A new style can drop feature states and layer visibility, so restore them
    this.map.once("styledata", () => {
      this.render();
      if (this.selection?.kind === "node") {
        this.map.setFeatureState(
          { source: "basin_area", sourceLayer: "basin_area", id: this.selection.id },
          { selected: true },
        );
      }
      if (this.basinAreaColors) {
        this.basinAreasColored = false;
        this.colorBasinAreas(this.basinAreaColors.scale, this.basinAreaColors.values);
      }
    });
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
        "fill-color": [
          "case",
          ["boolean", ["feature-state", "selected"], false],
          "#ff8c00",
          ["to-color", ["coalesce", ["feature-state", "color"], "#f7f7f7"]],
        ],
        // Unfilled as in QGIS; transparent fills are still found by queryRenderedFeatures for clicks
        "fill-opacity": [
          "case",
          ["boolean", ["feature-state", "selected"], false],
          0.5,
          ["!=", ["feature-state", "color"], null],
          0.75,
          0,
        ],
      },
    });
    map.addLayer({
      id: "basin_area_outline",
      type: "line",
      source: "basin_area",
      "source-layer": "basin_area",
      minzoom: BASIN_OUTLINE_MIN_ZOOM,
      paint: {
        "line-color": "#000",
        "line-width": ["interpolate", ["linear"], ["zoom"], BASIN_OUTLINE_MIN_ZOOM, 0.3, 12, 1.36],
        "line-opacity": ["interpolate", ["linear"], ["zoom"], BASIN_OUTLINE_MIN_ZOOM, 0.4, 11, 1],
      },
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
    const common = {
      id: `node-${group.type}`,
      data: group.data,
      visible: this.visible.get(group.type),
      pickable: true,
      autoHighlight: true,
      highlightColor: HIGHLIGHT,
      highlightedObjectIndex: isSelected ? this.network.nodeLocal[selected] : -1,
    };
    if (group.type in this.icons.mapping) {
      return new IconLayer({
        ...common,
        iconAtlas: this.icons.atlas,
        iconMapping: this.icons.mapping,
        getIcon: () => group.type,
        sizeUnits: "meters",
        getSize: NODE_ICON_SIZE_M,
        sizeMinPixels: 6,
        sizeMaxPixels: NODE_ICON_SIZE_PX,
      });
    }
    // Node types without an icon, or when the icons failed to load
    return new ScatterplotLayer({
      ...common,
      getFillColor: NODE_STYLES[group.type] ?? DEFAULT_NODE_COLOR,
      getLineColor: [0, 0, 0],
      stroked: true,
      lineWidthUnits: "pixels",
      getLineWidth: 0.5,
      radiusUnits: "meters",
      getRadius: NODE_ICON_SIZE_M / 2,
      radiusMinPixels: 2,
      radiusMaxPixels: NODE_ICON_SIZE_PX / 2,
    });
  }

  private linkLayer(group: Group) {
    const selected =
      this.selection?.kind === "link" ? this.network.linkRow.get(this.selection.id) : undefined;
    const isSelected = selected !== undefined && this.network.linkType[selected] === group.type;
    const coloring = group.type === "flow" ? this.flowColoring : null;
    const flowStyle: FlowStyle | null = coloring ? (this.timeBar?.state.flowStyle ?? "color") : null;
    const style = LINK_STYLES[group.type] ?? DEFAULT_LINK_STYLE;
    // Colored links are wider, so the color is visible
    const byWidth = coloring !== null && flowStyle === "width";
    return new PathLayer({
      id: `link-${group.type}`,
      data: group.data,
      _pathType: "open",
      positionFormat: "XY",
      visible: this.visible.get(group.type),
      getColor:
        coloring && flowStyle === "color"
          ? (_: unknown, { index }: { index: number }) =>
              coloring.colors.subarray(index * 4, index * 4 + 4) as unknown as Color
          : style.color,
      widthUnits: "pixels",
      getWidth: byWidth
        ? (_: unknown, { index }: { index: number }) => coloring.widths[index]
        : flowStyle === "color"
          ? 2 * LINK_WIDTH_PX
          : LINK_WIDTH_PX,
      widthMinPixels: byWidth ? 0 : 1,
      updateTriggers: { getColor: [coloring?.version, flowStyle], getWidth: [coloring?.version, flowStyle] },
      ...(style.dashed ? { extensions: DASHED, getDashArray: DASH_ARRAY, dashJustified: true } : {}),
      pickable: true,
      autoHighlight: true,
      highlightColor: HIGHLIGHT,
      highlightedObjectIndex: isSelected ? this.network.linkLocal[selected] : -1,
    });
  }

  /** Direction arrows halfway along each link, as the QGIS plugin draws them. */
  private arrowLayer(group: Group) {
    return new IconLayer({
      id: `arrow-${group.type}`,
      data: group.arrows!,
      visible: this.visible.get(group.type) && this.map.getZoom() >= ARROW_MIN_ZOOM,
      iconAtlas: this.icons.atlas,
      iconMapping: this.icons.mapping,
      getIcon: () => ARROW_ICON,
      getColor: (LINK_STYLES[group.type] ?? DEFAULT_LINK_STYLE).color,
      sizeUnits: "meters",
      getSize: ARROW_SIZE_M,
      sizeMaxPixels: ARROW_SIZE_PX,
    });
  }

  /** Flow values next to the links in view, only when zoomed in far enough to avoid clutter. */
  private updateLabels(): void {
    const coloring = this.flowColoring;
    const flow = this.network.linkGroups.find((group) => group.type === "flow");
    const labels: Label[] = [];
    if (coloring && flow && this.visible.get("flow") && this.map.getZoom() >= LABEL_MIN_ZOOM) {
      const bounds = this.map.getBounds();
      const positions = flow.arrows!.attributes.getPosition.value;
      for (let i = 0; i < flow.rows.length && labels.length < MAX_LABELS; i++) {
        const position: [number, number] = [positions[2 * i], positions[2 * i + 1]];
        const value = coloring.values[i];
        if (Number.isFinite(value) && bounds.contains(position)) labels.push({ position, text: formatNumber(value) });
      }
    }
    // A new array makes deck.gl rebuild the layer, so keep the old one if both are empty
    if (labels.length > 0 || this.labels.length > 0) this.labels = labels;
  }

  private labelLayer() {
    return new TextLayer<Label>({
      id: "flow-labels",
      data: this.labels,
      getPosition: (label) => label.position,
      getText: (label) => label.text,
      getSize: 11,
      getColor: [20, 20, 20],
      getPixelOffset: [0, -12],
      fontFamily: "system-ui, sans-serif",
      background: true,
      getBackgroundColor: [255, 255, 255, 210],
      backgroundPadding: [2, 1],
    });
  }

  private zoomDetail(): string {
    const zoom = this.map.getZoom();
    return [ARROW_MIN_ZOOM, LABEL_MIN_ZOOM].map((min) => zoom >= min).join();
  }

  private render(): void {
    const { network } = this;
    this.detail = this.zoomDetail();
    this.overlay.setProps({
      layers: [
        ...network.linkGroups.map((g) => this.linkLayer(g)),
        ...network.linkGroups.map((g) => this.arrowLayer(g)),
        ...network.nodeGroups.map((g) => this.nodeLayer(g)),
        this.labelLayer(),
      ],
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
    const value = (coloring: Coloring | null, kind: "flow" | "basin") => {
      const variable = this.timeBar?.state[kind];
      if (!coloring || !variable) return "";
      const { label, units } = this.manifest.results![kind].variables[variable];
      return `\n${label}: ${formatNumber(coloring.values[info.index])} ${units}`;
    };
    if (selection.kind === "node") {
      const nodeType = network.nodeType[network.nodeRow.get(selection.id)!];
      const extra = nodeType === "Basin" ? value(this.basinColoring, "basin") : "";
      return { text: `${nodeType} #${selection.id}${extra}` };
    }
    const row = network.linkRow.get(selection.id)!;
    const extra = network.linkType[row] === "flow" ? value(this.flowColoring, "flow") : "";
    return {
      text: `${network.linkType[row]} link #${selection.id}\n${network.fromNodeId[row]} → ${network.toNodeId[row]}${extra}`,
    };
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

  private checkbox(key: string, label: string, symbol?: HTMLElement, count?: number): HTMLLabelElement {
    const input = el("input");
    input.type = "checkbox";
    input.checked = this.visible.get(key) ?? false;
    input.addEventListener("change", () => {
      this.visible.set(key, input.checked);
      this.render();
    });
    const row = el("label");
    row.append(input);
    if (symbol) row.append(symbol);
    row.append(count === undefined ? label : `${label} (${count.toLocaleString()})`);
    return row;
  }

  private nodeSymbol(nodeType: string): HTMLElement {
    const png = nodeIconPng(nodeType);
    if (png && nodeType in this.icons.mapping) {
      const image = el("img", "symbol");
      image.src = png;
      image.alt = "";
      return image;
    }
    const swatch = el("span", "swatch");
    swatch.style.background = `rgb(${(NODE_STYLES[nodeType] ?? DEFAULT_NODE_COLOR).join(",")})`;
    return swatch;
  }

  private layerControl(): HTMLDetailsElement {
    const { network } = this;
    const details = el("details", "layers");
    // On phones the list would cover most of the map
    details.open = !matchMedia("(max-width: 600px)").matches;
    details.append(el("summary", "", "Layers"));
    details.append(el("h4", "", "Nodes"));
    for (const group of network.nodeGroups) {
      details.append(this.checkbox(group.type, group.type, this.nodeSymbol(group.type), group.rows.length));
    }
    details.append(el("h4", "", "Links"));
    for (const group of network.linkGroups) {
      const style = LINK_STYLES[group.type] ?? DEFAULT_LINK_STYLE;
      const line = el("span", "line-swatch");
      line.style.borderTop = `2px ${style.dashed ? "dashed" : "solid"} rgb(${style.color.join(",")})`;
      details.append(this.checkbox(group.type, group.type, line, group.rows.length));
    }
    details.append(el("h4", "", "Areas"));
    details.append(
      this.checkbox("basin_area", "Basin / area", el("span", "area-swatch")),
      this.checkbox("waterboards", "Water boards"),
    );
    const basemap = el("select", "basemap");
    basemap.append(...Object.entries(BASEMAPS).map(([id, { label }]) => new Option(label, id, false, id === this.basemap)));
    basemap.addEventListener("change", () => {
      this.setBasemap(basemap.value).catch((error) => console.error("Failed to change the base map", error));
    });
    details.append(el("h4", "", "Base map"), basemap);
    return details;
  }

  private searchMatches(query: string): Selection[] {
    // Node and link ids overlap, so "node 12" / "n12" or "link 12" / "l12" restrict the search
    const match = /^(?:(n|node|l|link)\s*#?\s*)?#?(\d+)$/i.exec(query.trim());
    if (!match) return [];
    const id = Number(match[2]);
    const prefix = match[1]?.[0].toLowerCase();
    const matches: Selection[] = [];
    if (prefix !== "l" && this.network.nodeRow.has(id)) matches.push({ kind: "node", id });
    if (prefix !== "n" && this.network.linkRow.has(id)) matches.push({ kind: "link", id });
    return matches;
  }

  private searchControl(): HTMLElement[] {
    const input = el("input", "search");
    input.type = "search";
    input.placeholder = "Node or link id";
    input.title = "Prefix with n or l to search only nodes or links, e.g. l200001";
    const choices = el("div", "search-choices");
    choices.hidden = true;
    const go = (selection: Selection) => {
      choices.hidden = true;
      this.select(selection);
      this.zoomTo(selection);
    };
    input.addEventListener("keydown", (event) => {
      if (event.key !== "Enter") return;
      const matches = this.searchMatches(input.value);
      input.classList.toggle("not-found", matches.length === 0);
      choices.hidden = matches.length < 2;
      if (matches.length === 1) go(matches[0]);
      if (matches.length < 2) return;
      choices.replaceChildren(
        ...matches.map((selection) => {
          const { network } = this;
          const label =
            selection.kind === "node"
              ? `${network.nodeType[network.nodeRow.get(selection.id)!]} #${selection.id}`
              : `${network.linkType[network.linkRow.get(selection.id)!]} link #${selection.id}`;
          const button = el("button", "", label);
          button.type = "button";
          button.addEventListener("click", () => go(selection));
          return button;
        }),
      );
    });
    input.addEventListener("input", () => {
      choices.hidden = true;
    });
    return [input, choices];
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
    // MapLibre adds its view to the hash, so check for it before creating the map
    const hasView = hashParams().has("map");
    const map = new maplibregl.Map({
      container,
      hash: "map",
      bounds: hasView ? undefined : manifest.bounds,
      fitBoundsOptions: { padding: 20 },
      // The network is drawn flat, so rotation and pitch would only disorient
      dragRotate: false,
      pitchWithRotate: false,
      touchPitch: false,
      style: await basemapStyle(DEFAULT_BASEMAP),
      // The default control collapses into an "i" button on narrow maps and once the map is moved
      attributionControl: false,
    });
    map.touchZoomRotate.disableRotation();
    map.keyboard.disableRotation();
    // Zoom with the mouse wheel, pinch or keyboard; the buttons would sit under the panel
    map.addControl(new maplibregl.AttributionControl({ compact: false }), "bottom-right");
    map.addControl(new maplibregl.ScaleControl(), "bottom-left");
    // "load" also waits for basemap tiles and a rendered frame; the style is all we need to add layers
    const [network, icons] = await Promise.all([loadNetwork(manifest), loadIconAtlas(), map.once("style.load")]);
    const viewer = new Viewer(map, root, manifest, network, icons);
    status.remove();
    const selection = selectionFromHash();
    if (selection) {
      viewer.select(selection);
      if (!hasView) viewer.zoomTo(selection);
    }
    await viewer.initResults();
  } catch (error) {
    root.append(status);
    status.textContent = `Failed to load the model viewer: ${error}`;
    status.classList.add("error");
    throw error;
  }
}

await main();
