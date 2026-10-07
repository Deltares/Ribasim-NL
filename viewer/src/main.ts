import "maplibre-gl/dist/maplibre-gl.css";
import "uplot/dist/uPlot.min.css";
import "@fontsource/atkinson-hyperlegible-next/latin-400.css";
import "@fontsource/atkinson-hyperlegible-next/latin-600.css";
import "./style.css";

import type { PickingInfo } from "@deck.gl/core";
import { DataFilterExtension, PathStyleExtension } from "@deck.gl/extensions";
import { IconLayer, PathLayer, ScatterplotLayer, TextLayer } from "@deck.gl/layers";
import { MapboxOverlay } from "@deck.gl/mapbox";
import * as maplibregl from "maplibre-gl";
// maplibre resolves its worker at runtime, which bundlers cannot follow
import maplibreWorkerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url";
import { Protocol } from "pmtiles";
import { BASEMAPS, basemapStyle, DEFAULT_BASEMAP, withOverlays } from "./basemaps";
import { ColorScale, formatNumber, normalize } from "./colors";
import { fileUrl, loadComparison, loadManifest, type Comparison, type Manifest, type ModelDiff, type ResultKind } from "./data";
import {
  CONTEXT_COLOR,
  DIFF_CSS,
  DiffFilter,
  describeDiff,
  describeResult,
  diffSummary,
  ResultColors,
  statusColors,
} from "./diff";
import { linkLabel, loadNetwork, type Group, type LayerData, type Network } from "./network";
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
// Slate ink for the hovered and selected feature, which leaves the status colors to the differences
const HIGHLIGHT: [number, number, number, number] = [30, 42, 50, 255];
const HIGHLIGHT_CSS = "#1e2a32";
// A quiet base map when comparing models, so the status colors stand out
const COMPARISON_BASEMAP = "grey";
const NO_DATA: [number, number, number, number] = [170, 170, 170, 255];
const DASHED = [new PathStyleExtension({ dash: true })];
// Hides the differences that are filtered out in the Differences box
const FILTER = new DataFilterExtension({ filterSize: 1 });
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
const OVERLAY_SOURCES = new Set(["basin_area", "basin_area_base", "waterboards"]);
// The status ring around a differing node, relative to the icon size; when zoomed out the rings of dense
// differences would merge into blobs, so they are small dots on top of the icons instead
const DIFF_RING_SCALE = 0.75;
const DIFF_RING_MIN_ZOOM = 10;
const DIFF_DOT_PX = 3;
// The ring around the selected node, relative to the icon size
const SELECTION_RING_SCALE = 0.9;

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

/** Layer props of the filter of the Differences box. */
interface FilterProps {
  extensions: DataFilterExtension[];
  getFilterValue?: (object: unknown, info: { index: number }) => number;
  filterRange?: [number, number];
  updateTriggers: { getFilterValue?: number };
}

interface Label {
  position: [number, number];
  text: string;
}

/** A layer in the layer list. */
interface LayerRow {
  key: string;
  label: string;
  symbol?: HTMLElement;
  count?: number;
}

class Viewer {
  private readonly visible = new Map<string, boolean>();
  private basemap: string;
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
  /** Status colors per node or link group, only when comparing models */
  private readonly diffColors = new Map<Group, Uint8Array>();
  /** Status rings around the differing nodes, only when comparing models */
  private readonly diffRings: LayerData | null = null;
  /** The colors of the result differences, only when comparing models that both have results */
  private readonly resultColors: Record<ResultKind, ResultColors> | null = null;
  private readonly filter = new DiffFilter();
  /** Per node or link group, 1 for the features shown by the filter and 0 for the others */
  private readonly filterValues = new Map<Group, Float32Array>();
  private ringFilterValues = new Float32Array(0);

  constructor(
    private readonly map: maplibregl.Map,
    private readonly root: HTMLElement,
    private readonly manifest: Manifest,
    private readonly network: Network,
    private readonly icons: IconAtlas,
    private readonly comparison: Comparison | null,
  ) {
    this.basemap = comparison ? COMPARISON_BASEMAP : DEFAULT_BASEMAP;
    for (const group of [...network.nodeGroups, ...network.linkGroups]) {
      // When comparing, all differences are shown, also of the control nodes and links
      this.visible.set(group.type, comparison !== null || !HIDDEN_BY_DEFAULT.has(group.type));
    }
    this.visible.set("basin_area", true);
    this.visible.set("waterboards", true);
    this.visible.set("unchanged", false);
    if (comparison) {
      const { diff } = comparison;
      const results = diff.results && { basin: new ResultColors(diff.results.basin), flow: new ResultColors(diff.results.flow) };
      this.resultColors = results;
      for (const group of network.nodeGroups) {
        const basin = group.type === "Basin" ? results?.basin : undefined;
        this.diffColors.set(group, statusColors(group, network.nodeId, diff.nodes, basin));
      }
      for (const group of network.linkGroups) {
        const flow = group.type === "flow" ? results?.flow : undefined;
        this.diffColors.set(group, statusColors(group, network.linkId, diff.links, flow));
      }
      this.diffRings = this.ringData();
      this.updateFilterValues();
    }

    // Interleaved mode reads `map.transform`, which maplibre-gl 6 no longer exposes
    this.overlay = new MapboxOverlay({ interleaved: false, getTooltip: (info) => this.tooltip(info) });
    map.addControl(this.overlay);
    this.panel = new Panel(root, manifest, network, comparison, {
      select: (selection) => this.select(selection),
      zoomTo: (selection) => this.zoomTo(selection),
      close: () => this.select(null),
    });
    this.addMapLayers();
    // Search and layer list share a column, so the search choices push the list down
    const sidebar = el("div", "sidebar");
    sidebar.append(...this.searchControl());
    if (comparison) {
      sidebar.append(
        diffSummary(
          comparison.diff,
          this.resultColors,
          network,
          this.filter,
          () => {
            this.updateFilterValues();
            this.colorDiffAreas();
            this.render();
          },
          (selection) => {
            this.select(selection);
            this.zoomTo(selection);
          },
        ),
      );
    }
    sidebar.append(this.layerControl());
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

  /** Positions and colors of the status rings, of all node groups together. */
  private ringData(): LayerData {
    const groups = this.network.nodeGroups;
    const length = groups.reduce((sum, group) => sum + group.rows.length, 0);
    const positions = new Float32Array(length * 2);
    const colors = new Uint8Array(length * 4);
    let offset = 0;
    for (const group of groups) {
      positions.set(group.data.attributes.getPosition.value, offset * 2);
      colors.set(this.diffColors.get(group)!, offset * 4);
      offset += group.rows.length;
    }
    return {
      length,
      attributes: {
        getPosition: { value: positions, size: 2 },
        getLineColor: { value: colors, size: 4, normalized: true },
        getFillColor: { value: colors, size: 4, normalized: true },
      },
    };
  }

  /** Which features the filter of the Differences box shows, per group and for the status rings. */
  private updateFilterValues(): void {
    const { comparison, network, filter, resultColors } = this;
    if (!comparison) return;
    const { diff } = comparison;
    const values = (group: Group, ids: Int32Array, diffs: typeof diff.nodes, results?: ResultColors) =>
      Float32Array.from(group.rows, (row) =>
        filter.shows(diffs.get(ids[row]), results?.diff.differences.has(ids[row]) ?? false) ? 1 : 0,
      );
    for (const group of network.nodeGroups) {
      const basin = group.type === "Basin" ? resultColors?.basin : undefined;
      this.filterValues.set(group, values(group, network.nodeId, diff.nodes, basin));
    }
    for (const group of network.linkGroups) {
      const flow = group.type === "flow" ? resultColors?.flow : undefined;
      this.filterValues.set(group, values(group, network.linkId, diff.links, flow));
    }
    const rings = network.nodeGroups.map((group) => this.filterValues.get(group)!);
    this.ringFilterValues = new Float32Array(rings.reduce((sum, part) => sum + part.length, 0));
    let offset = 0;
    for (const part of rings) {
      this.ringFilterValues.set(part, offset);
      offset += part.length;
    }
  }

  /** Layer props that hide the features the filter of the Differences box leaves out, when comparing models. */
  private filterProps(values: Float32Array | undefined): FilterProps {
    if (!values) return { extensions: [], updateTriggers: {} };
    return {
      extensions: [FILTER],
      getFilterValue: (_: unknown, { index }: { index: number }) => values[index],
      filterRange: [0.5, 1],
      updateTriggers: { getFilterValue: this.filter.version },
    };
  }

  /**
   * Color the Basin / areas of the differing Basins in the head model by their status, or result difference,
   * if the filter of the Differences box shows them.
   */
  private colorDiffAreas(): void {
    if (!this.comparison) return;
    const { diff } = this.comparison;
    const target = { source: "basin_area", sourceLayer: "basin_area" };
    const results = this.resultColors?.basin;
    const ids = new Set([...(results?.diff.differences.keys() ?? []), ...diff.nodes.keys()]);
    for (const id of ids) {
      const row = this.network.nodeRow.get(id);
      const nodeDiff = diff.nodes.get(id);
      if (nodeDiff?.status === "removed" || row === undefined || this.network.nodeType[row] !== "Basin") continue;
      const resultDiffers = results?.diff.differences.has(id) ?? false;
      const color = !this.filter.shows(nodeDiff, resultDiffers)
        ? null
        : nodeDiff && !this.filter.hiddenStatuses.has(nodeDiff.status)
          ? DIFF_CSS[nodeDiff.status]
          : results!.css(id);
      this.map.setFeatureState({ ...target, id }, { diff: color });
    }
  }

  async initResults(): Promise<void> {
    // When comparing models the map shows the differences, and the panel compares the results
    if (!this.manifest.results || this.comparison) return;
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
      this.colorDiffAreas();
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
          HIGHLIGHT_CSS,
          ["to-color", ["coalesce", ["feature-state", "color"], ["feature-state", "diff"], "#f7f7f7"]],
        ],
        // Unfilled as in QGIS; transparent fills are still found by queryRenderedFeatures for clicks
        "fill-opacity": [
          "case",
          ["boolean", ["feature-state", "selected"], false],
          0.3,
          ["!=", ["feature-state", "color"], null],
          0.75,
          ["!=", ["feature-state", "diff"], null],
          0.35,
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
        // The selected area has a thick outline, its fill is light to keep the map below visible
        "line-width": [
          "interpolate",
          ["linear"],
          ["zoom"],
          BASIN_OUTLINE_MIN_ZOOM,
          ["case", ["boolean", ["feature-state", "selected"], false], 2, 0.3],
          12,
          ["case", ["boolean", ["feature-state", "selected"], false], 3, 1.36],
        ],
        "line-opacity": ["interpolate", ["linear"], ["zoom"], BASIN_OUTLINE_MIN_ZOOM, 0.4, 11, 1],
      },
    });
    this.addBaseAreas();
    this.colorDiffAreas();
    if (manifest.files.waterboards) {
      map.addSource("waterboards", { type: "geojson", data: fileUrl(manifest.files.waterboards) });
      map.addLayer({
        id: "waterboards",
        type: "line",
        source: "waterboards",
        paint: { "line-color": "#333", "line-width": 1.5, "line-dasharray": [4, 2] },
      });
    }
  }

  /** The outlines of the removed and changed Basin / areas in the base model. */
  private addBaseAreas(): void {
    if (!this.comparison) return;
    const ids = [...this.comparison.diff.nodes]
      .filter(([, diff]) => diff.status === "removed" || diff.changes.includes("Basin / area"))
      .map(([id]) => id);
    this.map.addSource("basin_area_base", {
      type: "vector",
      url: `pmtiles://${fileUrl(this.comparison.base.files.basin_area)}`,
    });
    this.map.addLayer({
      id: "basin_area_base",
      type: "line",
      source: "basin_area_base",
      "source-layer": "basin_area",
      filter: ["in", ["get", "node_id"], ["literal", ids]],
      paint: { "line-color": DIFF_CSS.removed, "line-width": 2, "line-dasharray": [2, 1] },
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
      ...this.filterProps(this.filterValues.get(group)),
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
    const diffColors = this.diffColors.get(group);
    // Colored links are wider, so the color is visible
    const byWidth = coloring !== null && flowStyle === "width";
    const filter = this.filterProps(this.filterValues.get(group));
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
          : diffColors
            ? (_: unknown, { index }: { index: number }) => diffColors.subarray(index * 4, index * 4 + 4) as unknown as Color
            : style.color,
      widthUnits: "pixels",
      getWidth: byWidth
        ? (_: unknown, { index }: { index: number }) => coloring.widths[index]
        : flowStyle === "color" || diffColors
          ? 2 * LINK_WIDTH_PX
          : LINK_WIDTH_PX,
      widthMinPixels: byWidth ? 0 : 1,
      ...filter,
      updateTriggers: {
        ...filter.updateTriggers,
        getColor: [coloring?.version, flowStyle],
        getWidth: [coloring?.version, flowStyle],
      },
      extensions: [...filter.extensions, ...(style.dashed ? DASHED : [])],
      ...(style.dashed ? { getDashArray: DASH_ARRAY, dashJustified: true } : {}),
      pickable: true,
      autoHighlight: true,
      highlightColor: HIGHLIGHT,
      highlightedObjectIndex: isSelected ? this.network.linkLocal[selected] : -1,
    });
  }

  /** Direction arrows halfway along each link, as the QGIS plugin draws them. */
  private arrowLayer(group: Group) {
    const diffColors = this.diffColors.get(group);
    return new IconLayer({
      id: `arrow-${group.type}`,
      data: group.arrows!,
      visible: this.visible.get(group.type) && this.map.getZoom() >= ARROW_MIN_ZOOM,
      iconAtlas: this.icons.atlas,
      iconMapping: this.icons.mapping,
      getIcon: () => ARROW_ICON,
      ...this.filterProps(this.filterValues.get(group)),
      getColor: diffColors
        ? (_: unknown, { index }: { index: number }) => diffColors.subarray(index * 4, index * 4 + 4) as unknown as Color
        : (LINK_STYLES[group.type] ?? DEFAULT_LINK_STYLE).color,
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

  /** The unchanged features when comparing models, faint for context; they can be clicked for details. */
  private contextLayers() {
    const context = this.network.context;
    if (!context) return [];
    const { selection } = this;
    const selected = (kind: "node" | "link") => {
      if (selection?.kind !== kind) return -1;
      const row = (kind === "node" ? this.network.nodeRow : this.network.linkRow).get(selection.id);
      const local = kind === "node" ? context.nodeLocal : context.linkLocal;
      return row === undefined ? -1 : (local.get(row) ?? -1);
    };
    const visible = this.visible.get("unchanged");
    return [
      new PathLayer({
        id: "context-link",
        data: context.links.data,
        _pathType: "open",
        positionFormat: "XY",
        visible,
        getColor: CONTEXT_COLOR,
        widthUnits: "pixels",
        getWidth: 1,
        pickable: true,
        autoHighlight: true,
        highlightColor: HIGHLIGHT,
        highlightedObjectIndex: selected("link"),
      }),
      new ScatterplotLayer({
        id: "context-node",
        data: context.nodes.data,
        visible,
        getFillColor: CONTEXT_COLOR,
        radiusUnits: "meters",
        getRadius: NODE_ICON_SIZE_M / 4,
        radiusMinPixels: 1.5,
        radiusMaxPixels: NODE_ICON_SIZE_PX / 4,
        pickable: true,
        autoHighlight: true,
        highlightColor: HIGHLIGHT,
        highlightedObjectIndex: selected("node"),
      }),
    ];
  }

  /** Whether the differing nodes have rings under their icons, or are zoomed out and have dots on top. */
  private showRings(): boolean {
    return this.map.getZoom() >= DIFF_RING_MIN_ZOOM;
  }

  /** Rings in the status color around the differing nodes, or dots when zoomed out. */
  private ringLayer() {
    if (!this.diffRings) return [];
    const rings = this.showRings();
    return [
      new ScatterplotLayer({
        id: "diff-rings",
        data: this.diffRings,
        stroked: rings,
        filled: !rings,
        lineWidthUnits: "pixels",
        getLineWidth: 3,
        radiusUnits: rings ? "meters" : "pixels",
        getRadius: rings ? NODE_ICON_SIZE_M * DIFF_RING_SCALE : DIFF_DOT_PX,
        radiusMinPixels: rings ? 6 : 0,
        radiusMaxPixels: rings ? NODE_ICON_SIZE_PX * DIFF_RING_SCALE : DIFF_DOT_PX,
        ...this.filterProps(this.ringFilterValues),
      }),
    ];
  }

  /** A ring in the highlight color around the selected node, so it stands out from the status rings. */
  private selectionLayer() {
    const row = this.selection?.kind === "node" ? this.network.nodeRow.get(this.selection.id) : undefined;
    if (row === undefined) return [];
    return [
      new ScatterplotLayer<number>({
        id: "selection",
        data: [row],
        getPosition: (nodeRow) => [this.network.nodeX[nodeRow], this.network.nodeY[nodeRow]],
        stroked: true,
        filled: false,
        lineWidthUnits: "pixels",
        getLineWidth: 2.5,
        getLineColor: HIGHLIGHT,
        radiusUnits: "meters",
        getRadius: NODE_ICON_SIZE_M * SELECTION_RING_SCALE,
        radiusMinPixels: 9,
        radiusMaxPixels: NODE_ICON_SIZE_PX * SELECTION_RING_SCALE + 3,
      }),
    ];
  }

  private zoomDetail(): string {
    const zoom = this.map.getZoom();
    return [ARROW_MIN_ZOOM, LABEL_MIN_ZOOM, DIFF_RING_MIN_ZOOM].map((min) => zoom >= min).join();
  }

  private render(): void {
    const { network } = this;
    this.detail = this.zoomDetail();
    const rings = this.showRings();
    this.overlay.setProps({
      layers: [
        ...this.contextLayers(),
        ...network.linkGroups.map((g) => this.linkLayer(g)),
        ...network.linkGroups.map((g) => this.arrowLayer(g)),
        ...(rings ? this.ringLayer() : []),
        ...network.nodeGroups.map((g) => this.nodeLayer(g)),
        ...(rings ? [] : this.ringLayer()),
        ...this.selectionLayer(),
        this.labelLayer(),
      ],
    });
    for (const id of ["basin_area", "basin_area_outline", "basin_area_base"]) {
      if (this.map.getLayer(id)) {
        this.map.setLayoutProperty(id, "visibility", this.visible.get("basin_area") ? "visible" : "none");
      }
    }
    if (this.map.getLayer("waterboards")) {
      this.map.setLayoutProperty("waterboards", "visibility", this.visible.get("waterboards") ? "visible" : "none");
    }
  }

  /** Map a deck.gl pick back to a node or link. */
  private picked(info: PickingInfo): Selection | null {
    if (!info.layer || info.index < 0) return null;
    const context = this.network.context;
    if (context && info.layer.id === "context-node") {
      return { kind: "node", id: this.network.nodeId[context.nodes.rows[info.index]] };
    }
    if (context && info.layer.id === "context-link") {
      return { kind: "link", id: this.network.linkId[context.links.rows[info.index]] };
    }
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
    const diffs = selection.kind === "node" ? this.comparison?.diff.nodes : this.comparison?.diff.links;
    const diff = diffs?.get(selection.id);
    const result = describeResult(this.comparison?.diff.results?.[selection.kind === "node" ? "basin" : "flow"], selection.id);
    const status = (diff ? `\n${describeDiff(diff)}` : "") + (result ? `\n${result}` : "");
    if (selection.kind === "node") {
      const nodeType = network.nodeType[network.nodeRow.get(selection.id)!];
      const extra = nodeType === "Basin" ? value(this.basinColoring, "basin") : "";
      return { text: `${nodeType} #${selection.id}${extra}${status}` };
    }
    const row = network.linkRow.get(selection.id)!;
    const extra = network.linkType[row] === "flow" ? value(this.flowColoring, "flow") : "";
    return {
      text: `${network.linkType[row]} link ${linkLabel(selection.id)}\n${network.fromNodeId[row]} → ${network.toNodeId[row]}${extra}${status}`,
    };
  }

  private onClick(event: maplibregl.MapMouseEvent): void {
    const info = this.overlay.pickObject({ x: event.point.x, y: event.point.y, radius: 4 });
    const picked = info ? this.picked(info) : null;
    if (picked) return this.select(picked);
    if (this.visible.get("basin_area") && this.map.getLayer("basin_area")) {
      const [area] = this.map.queryRenderedFeatures(event.point, { layers: ["basin_area"] });
      if (area?.id !== undefined) return this.select({ kind: "node", id: Number(area.id) });
    }
    this.select(null);
  }

  selected(): Selection | null {
    return this.selection;
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

  /** A header with a checkbox that toggles all layers below it, which follows their state. */
  private layerSection(title: string, layers: LayerRow[]): HTMLElement[] {
    const all = el("input");
    all.type = "checkbox";
    all.title = `Show or hide all ${title.toLowerCase()}`;
    const inputs = layers.map(() => el("input"));
    const sync = () => {
      const shown = layers.filter(({ key }) => this.visible.get(key)).length;
      all.checked = shown === layers.length;
      all.indeterminate = shown > 0 && shown < layers.length;
    };
    all.addEventListener("change", () => {
      layers.forEach(({ key }, i) => {
        this.visible.set(key, all.checked);
        inputs[i].checked = all.checked;
      });
      all.indeterminate = false;
      this.updateLabels();
      this.render();
    });
    const rows = layers.map(({ key, label, symbol, count }, i) => {
      const input = inputs[i];
      input.type = "checkbox";
      input.checked = this.visible.get(key) ?? false;
      input.addEventListener("change", () => {
        this.visible.set(key, input.checked);
        sync();
        this.updateLabels();
        this.render();
      });
      const row = el("label");
      row.append(input);
      if (symbol) row.append(symbol);
      row.append(count === undefined ? label : `${label} (${count.toLocaleString()})`);
      return row;
    });
    sync();
    const header = el("label", "section");
    header.append(all, el("h4", "", title));
    return [header, ...rows];
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
    const nodes = network.nodeGroups.map(({ type, rows }) => ({
      key: type,
      label: type,
      symbol: this.nodeSymbol(type),
      count: rows.length,
    }));
    const links = network.linkGroups.map(({ type, rows }) => {
      const style = LINK_STYLES[type] ?? DEFAULT_LINK_STYLE;
      const symbol = el("span", "line-swatch");
      symbol.style.borderTop = `2px ${style.dashed ? "dashed" : "solid"} rgb(${style.color.join(",")})`;
      return { key: type, label: type, symbol, count: rows.length };
    });
    details.append(
      ...this.layerSection("Nodes", nodes),
      ...this.layerSection("Links", links),
      ...this.layerSection("Areas", [
        { key: "basin_area", label: "Basin / area", symbol: el("span", "area-swatch") },
        ...(this.manifest.files.waterboards ? [{ key: "waterboards", label: "Water boards" }] : []),
      ]),
    );
    const context = this.network.context;
    if (context) {
      const symbol = el("span", "swatch");
      symbol.style.background = `rgb(${CONTEXT_COLOR.slice(0, 3).join(",")})`;
      const count = context.nodes.rows.length + context.links.rows.length;
      details.append(...this.layerSection("Comparison", [{ key: "unchanged", label: "Unchanged", symbol, count }]));
    }
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
              : `${network.linkType[network.linkRow.get(selection.id)!]} link ${linkLabel(selection.id)}`;
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

/**
 * The extent of the differing nodes, and of the nodes of differing links. Differences in results only spread
 * downstream of a change, so they are left out, unless there are no other differences.
 */
function diffBounds(network: Network, diff: ModelDiff): [number, number, number, number] | null {
  const bounds = new maplibregl.LngLatBounds();
  const extend = (nodeRow: number | undefined) => {
    if (nodeRow !== undefined) bounds.extend([network.nodeX[nodeRow], network.nodeY[nodeRow]]);
  };
  const networkChanged = diff.nodes.size > 0 || diff.links.size > 0;
  const nodeIds = networkChanged ? diff.nodes.keys() : (diff.results?.basin.differences.keys() ?? []);
  const linkIds = networkChanged ? diff.links.keys() : (diff.results?.flow.differences.keys() ?? []);
  for (const id of nodeIds) extend(network.nodeRow.get(id));
  for (const id of linkIds) {
    const row = network.linkRow.get(id);
    if (row === undefined) continue;
    extend(network.nodeRow.get(network.fromNodeId[row]));
    extend(network.nodeRow.get(network.toNodeId[row]));
  }
  return bounds.isEmpty() ? null : (bounds.toArray().flat() as [number, number, number, number]);
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
    const [manifest, comparison] = await Promise.all([loadManifest(), loadComparison()]);
    const networkPromise = loadNetwork(manifest, comparison);
    // MapLibre adds its view to the hash, so check for it before creating the map
    const hasView = hashParams().has("map");
    // When comparing models, start with a view of the differences
    const bounds = hasView ? undefined : comparison ? (diffBounds(await networkPromise, comparison.diff) ?? manifest.bounds) : manifest.bounds;
    const map = new maplibregl.Map({
      container,
      hash: "map",
      bounds,
      fitBoundsOptions: { padding: 20 },
      // The network is drawn flat, so rotation and pitch would only disorient
      dragRotate: false,
      pitchWithRotate: false,
      touchPitch: false,
      style: await basemapStyle(comparison ? COMPARISON_BASEMAP : DEFAULT_BASEMAP),
      // The default control collapses into an "i" button on narrow maps and once the map is moved
      attributionControl: false,
    });
    map.touchZoomRotate.disableRotation();
    map.keyboard.disableRotation();
    // Zoom with the mouse wheel, pinch or keyboard; the buttons would sit under the panel
    map.addControl(new maplibregl.AttributionControl({ compact: false }), "bottom-right");
    map.addControl(new maplibregl.ScaleControl(), "bottom-left");
    // "load" also waits for basemap tiles and a rendered frame; the style is all we need to add layers
    const [network, icons] = await Promise.all([networkPromise, loadIconAtlas(), map.once("style.load")]);
    status.textContent = "Drawing the model…";
    const viewer = new Viewer(map, root, manifest, network, icons, comparison);
    const selection = selectionFromHash();
    if (selection) {
      viewer.select(selection);
      if (!hasView) viewer.zoomTo(selection);
    }
    // A pasted link or going back in history only changes the hash, which selects another feature
    addEventListener("hashchange", () => {
      const next = selectionFromHash();
      if (next?.kind === viewer.selected()?.kind && next?.id === viewer.selected()?.id) return;
      viewer.select(next);
      if (next) viewer.zoomTo(next);
    });
    const results = viewer.initResults();
    // Drawing the network and the base map tiles takes a while for large models, so keep saying so
    await map.once("idle");
    status.remove();
    await results;
  } catch (error) {
    root.append(status);
    status.textContent = `Failed to load the model viewer: ${error}`;
    status.classList.add("error");
    throw error;
  }
}

await main();
