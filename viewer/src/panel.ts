import uPlot from "uplot";
import { CATEGORICAL, formatNumber, formatTicks, NEUTRAL } from "./colors";
import {
  readColumns,
  readRow,
  readRowsById,
  type Comparison,
  type FeatureDiff,
  type Manifest,
  type ResultSet,
  type Row,
  type TableEntry,
} from "./data";
import { describeResult, statusLabel } from "./diff";
import {
  basinFlowParts,
  basinFractions,
  CONNECTOR_TYPES,
  CONTINUITY,
  FLOW_NODE_TYPES,
  flowParts,
  INITIAL,
  layerFlows,
  layerNames,
  nodeFlow,
  OTHER,
  partsTotal,
  stackLayers,
  tracedWaters,
  visibleLayers,
  waterFractions,
  type FlowGraph,
  type FlowTerm,
  type Fractions,
  type TracerSet,
  type UsedWaters,
} from "./fractions";
import { LevelDiagram, type LevelSeries, type LevelSide } from "./levels";
import { flowGraph, linkLabel, type Network, pastJunctions } from "./network";
import { fitFacts, loadFits, loadObservedLinks, type Fits } from "./observations";
import { addSubstances } from "./timebar";

export type Selection = { kind: "node" | "link"; id: number };

const MAX_TABLE_ROWS = 2000;
const MAX_LINK_SERIES = 8;
const HIDDEN_COLUMNS = new Set(["x", "y", "coords"]);
const ID_COLUMNS = new Set(["node_id", "link_id"]);
/** Suffix of the series and tables of the base model, when comparing models */
const BASE = " (base)";
const BASIN_CHARTS: [string, string[]][] = [
  ["Level", ["level"]],
  ["Storage", ["storage"]],
  [
    "Fluxes",
    ["inflow_rate", "outflow_rate", "precipitation", "surface_runoff", "evaporation", "drainage", "infiltration"],
  ],
];

/** Create an element; children are added as text nodes so data is never parsed as HTML. */
function el<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  props: Partial<HTMLElementTagNameMap[K]> = {},
  ...children: (Node | string)[]
): HTMLElementTagNameMap[K] {
  const element = Object.assign(document.createElement(tag), props);
  element.append(...children);
  return element;
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (value instanceof Date) {
    const iso = value.toISOString();
    return iso.endsWith("T00:00:00.000Z") ? iso.slice(0, 10) : iso.slice(0, 19).replace("T", " ");
  }
  if (typeof value === "number" && !Number.isInteger(value)) return String(Number(value.toPrecision(6)));
  return String(value);
}

const isEmpty = (value: unknown) => value === null || value === undefined || value === "" || Number.isNaN(value);

/** The attributes of a feature; with the base row, values that differ show the base value struck through. */
function attributeTable(row: Row, base?: Row): HTMLTableElement {
  const body = el("tbody");
  const keys = [...new Set([...Object.keys(row), ...Object.keys(base ?? {})])];
  for (const key of keys) {
    const value = row[key];
    const baseValue = base?.[key];
    const differs = base !== undefined && formatValue(isEmpty(baseValue) ? null : baseValue) !== formatValue(isEmpty(value) ? null : value);
    if (HIDDEN_COLUMNS.has(key) || (isEmpty(value) && !differs)) continue;
    const cell = el("td", {}, formatValue(value));
    if (differs) cell.prepend(el("del", {}, formatValue(baseValue)), " → ");
    body.append(el("tr", { className: differs ? "changed" : "" }, el("th", {}, key), cell));
  }
  return el("table", { className: "attributes" }, body);
}

function dataTable(rows: Row[]): HTMLElement {
  const columns = Object.keys(rows[0]).filter((column) => column !== "node_id");
  const head = el("tr", {}, ...columns.map((column) => el("th", {}, column)));
  const body = el("tbody");
  for (const row of rows.slice(0, MAX_TABLE_ROWS)) {
    body.append(el("tr", {}, ...columns.map((column) => el("td", {}, formatValue(row[column])))));
  }
  const table = el("div", { className: "table-scroll" }, el("table", { className: "data" }, el("thead", {}, head), body));
  if (rows.length <= MAX_TABLE_ROWS) return table;
  return el("div", {}, table, el("p", { className: "note" }, `Showing ${MAX_TABLE_ROWS} of ${rows.length} rows.`));
}

const SERIES_COLORS = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2"];
// The base model is drawn dashed in a lighter tint of the same color, which also sets it apart in the legend
const BASE_ALPHA = "73";
/** The time axis of the charts, whose value in the legend is the date under the cursor */
const TIME_SERIES: uPlot.Series = {
  value: (_: uPlot, value: number | null) => (value === null ? "" : new Date(value * 1000).toISOString().slice(0, 10)),
};
/** The value axis of the charts, with ticks that tell neighbors apart */
const VALUE_AXIS: uPlot.Axis = { size: 60, values: (_, ticks, _axis, _space, increment) => formatTicks(ticks, increment) };

/** A time series chart of numeric columns, if the rows form one. */
function timeChart(rows: Row[], width: number, only?: string[]): HTMLElement | null {
  if (rows.length < 2 || !rows.every((row) => row.time instanceof Date)) return null;
  const columns = (only ?? Object.keys(rows[0])).filter(
    (column) => !ID_COLUMNS.has(column) && rows.some((row) => typeof row[column] === "number"),
  );
  if (columns.length === 0) return null;
  // A value that is missing or not finite, like the last day of the concentrations of Delwaq, is a gap
  const value = (row: Row, column: string) => {
    const v = row[column];
    return typeof v === "number" && Number.isFinite(v) ? v : null;
  };
  const data: uPlot.AlignedData = [
    rows.map((row) => (row.time as Date).getTime() / 1000),
    ...columns.map((column) => rows.map((row) => value(row, column))),
  ];
  const colors = columns.filter((column) => !column.endsWith(BASE));
  const container = el("div", { className: "chart" });
  // Values are only shown while hovering the chart
  const seriesValue = (_: uPlot, value: number | null) => (value === null ? "" : formatNumber(value));
  const plot = new uPlot(
    {
      width,
      height: 200,
      series: [
        TIME_SERIES,
        ...columns.map((label) => {
          const isBase = label.endsWith(BASE);
          const color = SERIES_COLORS[colors.indexOf(isBase ? label.slice(0, -BASE.length) : label) % SERIES_COLORS.length];
          return isBase
            ? { label, stroke: `${color}${BASE_ALPHA}`, dash: [4, 4], class: "base", value: seriesValue }
            : { label, stroke: color, value: seriesValue };
        }),
      ],
      scales: { x: { time: true } },
      axes: [{}, VALUE_AXIS],
    },
    data,
  );
  container.append(plot.root);
  return container;
}

/** A layer of a stacked chart. */
interface Layer {
  label: string;
  values: Float64Array;
  color: string;
  /** Hatched, to tell it apart from the layer with the same color */
  hatched: boolean;
}

// The hatching is a darker tone of the color it is drawn on
const HATCH_INK = "rgba(0, 0, 0, 0.35)";
const LINE_INK = "#1e2a32";

// A hatched layer has the color of the layer this many places below it in the set
const HATCH_PARTNER_DISTANCE = 5;

/**
 * The colors of the layers of a set, by their place in it, so a tracer keeps its color in all charts: grey for the
 * initial water and the water without a label, and the categorical colors in order for the others, with the derived
 * tracers last, so they do not change the colors of the others. More colors would be hard to tell apart, so the ninth
 * and later layers are hatched, in the color of a layer some places below them. Layers between them often have no
 * water, so they still touch, but rarely: in the LHM fractions of the coupled model, in 22 of the 20151 Basins.
 */
function layerStyles(set: TracerSet): Map<string, Pick<Layer, "color" | "hatched">> {
  const styles = new Map<string, Pick<Layer, "color" | "hatched">>([
    [INITIAL, { color: NEUTRAL.dark, hatched: false }],
    [OTHER, { color: NEUTRAL.light, hatched: false }],
  ]);
  const derived = (name: string) => set.derived?.[name] !== undefined;
  const named = layerNames(set).filter((name) => !styles.has(name));
  [...named.filter((name) => !derived(name)), ...named.filter(derived)].forEach((name, slot) => {
    const hatched = slot >= CATEGORICAL.length;
    const color = CATEGORICAL[(hatched ? slot - HATCH_PARTNER_DISTANCE : slot) % CATEGORICAL.length];
    styles.set(name, { color, hatched });
  });
  return styles;
}

/** The fill of a hatched layer: lines at 45° on its color, made once for the canvas of a chart. */
function hatchFill(color: string): uPlot.Series.Fill {
  let fill: CanvasPattern | string | null = null;
  return (plot) => {
    if (fill) return fill;
    const size = Math.round(6 * devicePixelRatio);
    const tile = Object.assign(document.createElement("canvas"), { width: size, height: size });
    const context = tile.getContext("2d");
    if (!context) return (fill = color);
    context.fillStyle = color;
    context.fillRect(0, 0, size, size);
    context.strokeStyle = HATCH_INK;
    context.lineWidth = devicePixelRatio;
    context.beginPath();
    // Lines from the lower left to the upper right, which continue in the next tile
    for (const offset of [-size, 0, size]) {
      context.moveTo(offset, size);
      context.lineTo(offset + size, 0);
    }
    context.stroke();
    return (fill = plot.ctx.createPattern(tile, "repeat") ?? color);
  };
}

/** The legend marker of a layer, hatched like its fill. */
const layerMarker = ({ color, hatched }: Layer) =>
  hatched ? `repeating-linear-gradient(-45deg, ${HATCH_INK} 0 1px, transparent 1px 4px), ${color}` : color;

/** A line on a stacked chart, like the total of its layers; dashed for measured values. */
interface ChartLine {
  label: string;
  values: Float64Array;
  dashed?: boolean;
}

// The legend marker of a line: a bar across the middle, dashed for a dashed line
const lineMarker = ({ dashed }: ChartLine) => {
  const bar = dashed ? `repeating-linear-gradient(90deg, ${LINE_INK} 0 4px, transparent 4px 6px)` : `linear-gradient(${LINE_INK}, ${LINE_INK})`;
  return `${bar} center / 100% 2px no-repeat`;
};

/**
 * A chart of layers stacked per time step: their positive parts up from zero and their negative parts down from zero,
 * with lines like their total. The legend shows the value of each layer under the cursor.
 */
function stackedChart(times: Date[], layers: Layer[], width: number, lines: ChartLine[] = []): HTMLElement {
  const { up, down } = stackLayers(layers.map((layer) => layer.values));
  const finite = (values: Float64Array) => Array.from(values, (value) => (Number.isFinite(value) ? value : null));
  const series: uPlot.Series[] = [TIME_SERIES];
  const data: (number | null)[][] = [times.map((time) => time.getTime() / 1000)];
  const markers: string[] = [""];
  const bands: uPlot.Band[] = [];
  const add = (layer: Layer, edges: Float64Array, options: uPlot.Series) => {
    const fill = layer.hatched ? hatchFill(layer.color) : layer.color;
    // An edge in the color of its layer, since a line of the panel color would hide the peaks of a single time step
    series.push({ label: layer.label, stroke: layer.color, width: 1, fill, ...options });
    data.push(finite(edges));
    markers.push(layerMarker(layer));
    return series.length - 1;
  };
  // Each layer fills the band between its edge and the edge below or above it; the first one fills to zero
  let below = 0;
  layers.forEach((layer, i) => {
    const value = (_: uPlot, _raw: number, _series: number, step: number | null) =>
      step === null ? "" : formatNumber(layer.values[step]);
    const edge = add(layer, up[i], { value });
    if (below) bands.push({ series: [edge, below] });
    below = edge;
  });
  let above = 0;
  layers.forEach((layer, i) => {
    if (!layer.values.some((value) => value < 0)) return;
    // The legend has a single entry per layer, with its net value
    const edge = add(layer, down[i], { class: "stack-down", value: () => "" });
    if (above) bands.push({ series: [edge, above], dir: 1 });
    above = edge;
  });
  for (const line of lines) {
    const value = (_: uPlot, raw: number | null) => (raw === null ? "" : formatNumber(raw));
    series.push({ label: line.label, stroke: LINE_INK, width: 1.5, value, ...(line.dashed ? { dash: [6, 4] } : {}) });
    data.push(finite(line.values));
    markers.push(lineMarker(line));
  }
  const plot = new uPlot(
    {
      width,
      height: 220,
      series,
      bands,
      scales: {
        x: { time: true },
        // The stack starts at zero
        y: { range: (_, min, max) => uPlot.rangeNum(Math.min(0, min), Math.max(0, max), 0.05, true) },
      },
      axes: [{}, VALUE_AXIS],
      legend: { markers: { show: true, width: 0, stroke: LINE_INK, fill: (_, i) => markers[i], dash: "solid" } },
    },
    data as uPlot.AlignedData,
  );
  return el("div", { className: "chart" }, plot.root);
}

/** The rows of a time table in effect at each time step: the last at or before it. */
function lastRowPerStep(rows: Row[], times: Date[]): (Row | undefined)[] {
  const sorted = [...rows].sort((a, b) => (a.time as Date).getTime() - (b.time as Date).getTime());
  let row = -1;
  return times.map((time) => {
    while (row + 1 < sorted.length && (sorted[row + 1].time as Date).getTime() <= time.getTime()) row++;
    return row >= 0 ? sorted[row] : undefined;
  });
}

/** Content that follows the time step of the map. */
interface Situation {
  element: HTMLElement;
  update: (step: number) => void;
}

function levelLegend(): HTMLElement {
  const item = (className: string, text: string) => el("span", {}, el("span", { className: `key ${className}` }), text);
  return el(
    "div",
    { className: "level-legend" },
    item("water", "water level"),
    item("limit", "min upstream / max downstream level of the control state"),
    item("bottom", "Basin bottom"),
  );
}

/** A collapsible section whose content is loaded when it is first opened. */
function lazySection(title: string, load: () => Promise<(Node | string)[]>, open = false): HTMLElement {
  const content = el("div", {}, "Loading…");
  const details = el("details", {}, el("summary", {}, title), content);
  let loaded = false;
  details.addEventListener("toggle", async () => {
    if (!details.open || loaded) return;
    loaded = true;
    try {
      content.replaceChildren(...(await load()));
    } catch (error) {
      content.replaceChildren(el("p", { className: "error" }, `Failed to load: ${error}`));
    }
  });
  details.open = open;
  return details;
}

function tableSection(entry: TableEntry, nodeId: number, width: number, title = entry.name): HTMLElement {
  return lazySection(title, async () => {
    const rows = await readRowsById(entry, "node_id", nodeId);
    if (rows.length === 0) return ["No rows for this node."];
    const chart = timeChart(rows, width);
    return chart ? [chart, dataTable(rows)] : [dataTable(rows)];
  });
}

/** Rows of both models merged on time, with the base columns suffixed. */
function mergeOnTime(head: Row[], base: Row[]): Row[] {
  const byTime = new Map<number, Row>();
  const add = (rows: Row[], suffix: string) => {
    for (const row of rows) {
      const time = (row.time as Date).getTime();
      let merged = byTime.get(time);
      if (!merged) byTime.set(time, (merged = { time: row.time }));
      for (const [key, value] of Object.entries(row)) if (key !== "time") merged[`${key}${suffix}`] = value;
    }
  };
  add(head, "");
  add(base, BASE);
  return [...byTime.values()].sort((a, b) => (a.time as Date).getTime() - (b.time as Date).getTime());
}

function charts(rows: Row[], width: number, specs: [string, string[]][]): (Node | string)[] {
  if (rows.length === 0) return ["No results for this feature."];
  return specs.flatMap(([title, columns]) => {
    const chart = timeChart(rows, width, columns.flatMap((column) => [column, `${column}${BASE}`]));
    return chart ? [el("h4", {}, title), chart] : [];
  });
}

const VOLUME_UNIT = 1e6;

/**
 * The cumulative volume of flow rate columns since the first row, reset to 0 on every 1 January.
 *
 * A flow rate holds until the next row, like the mean flow rate over a time step in Ribasim results.
 * To compare the columns, only intervals that start with a value in each column are summed, and only
 * the rows between the first and last with values in each column are returned.
 */
function cumulativeVolume(rows: Row[], columns: string[]): Row[] {
  const complete = (row: Row) => columns.every((column) => typeof row[column] === "number" && Number.isFinite(row[column]));
  const first = rows.findIndex(complete);
  let last = rows.length - 1;
  while (last > first && !complete(rows[last])) last--;
  if (first < 0 || last <= first) return [];
  const totals = columns.map(() => 0);
  const result: Row[] = [];
  for (let i = first; i <= last; i++) {
    const time = rows[i].time as Date;
    if (i > first) {
      const previous = rows[i - 1];
      const start = (previous.time as Date).getTime();
      const newYear = Date.UTC(time.getUTCFullYear(), 0, 1);
      // Reset on 1 January, then count only the part of the interval in the new year
      const resets = start < newYear;
      if (resets) totals.fill(0);
      const seconds = (time.getTime() - (resets ? newYear : start)) / 1000;
      if (complete(previous)) columns.forEach((column, j) => (totals[j] += ((previous[column] as number) * seconds) / VOLUME_UNIT));
    }
    result.push(Object.fromEntries([["time", time], ...columns.map((column, j) => [column, totals[j]])]));
  }
  return result;
}

/** A chart of the cumulative volume of flow rate columns, see `cumulativeVolume`. */
function cumulativeChart(rows: Row[], width: number, columns: string[]): (Node | string)[] {
  const present = columns.filter((column) => rows.some((row) => typeof row[column] === "number"));
  const chart = timeChart(cumulativeVolume(rows, present), width);
  return chart ? [el("h4", {}, "Cumulative volume since 1 January (10⁶ m³)"), chart] : [];
}

export interface PanelCallbacks {
  select: (selection: Selection) => void;
  zoomTo: (selection: Selection) => void;
  close: () => void;
  /** The user chose another substance in the panel, which the map shares */
  chooseSubstance: (substance: string) => void;
}

/** What a chart of the parts of a flow shows next to them, see `Panel.partsChart`. */
interface FlowChart {
  /** Like "flow rate" or "inflow rate", for the heading and the line of the total */
  variable?: string;
  /** What flows, for the heading, like "Pump #5" */
  subject?: string;
  /** The total flow, if it is not that of the charted links */
  total?: Float64Array;
  /** The observed flow of an Observation node, per time step */
  observed?: Float64Array;
  notes?: HTMLElement[];
}

/** A note below a chart. */
const note = (...text: string[]) => el("p", { className: "note" }, ...text);

export class Panel {
  private readonly element: HTMLElement;
  private token = 0;
  private step = 0;
  private readonly times: Date[];
  private situation: Situation | null = null;
  /** The time step of each time of the results, in milliseconds */
  private readonly steps: Map<number, number>;
  /** The substance to chart, the same as in the time bar */
  private substance: string | null;
  /** Shows the concentration chart of the selected Basin for another substance */
  private showSubstance: ((substance: string) => void) | null = null;
  /** The name of the tracer set of the stacked charts, which the panels of other features keep */
  private tracerSet: string | null = null;
  /** The Junctions in the Junction / static table of the shown model, once they are read */
  private staticJunctionIds: Promise<Set<number>> | null = null;

  constructor(
    parent: HTMLElement,
    private readonly manifest: Manifest,
    private readonly network: Network,
    private readonly comparison: Comparison | null,
    private readonly callbacks: PanelCallbacks,
  ) {
    this.element = el("div", { className: "panel", hidden: true });
    parent.append(this.element);
    this.times = (manifest.results?.times ?? []).map((time) => new Date(`${time}Z`));
    this.steps = new Map(this.times.map((time, step) => [time.getTime(), step]));
    this.substance = Object.keys(manifest.results?.concentration?.variables ?? {})[0] ?? null;
  }

  /** Follow the time step of the map, for the level diagram. */
  setStep(step: number): void {
    this.step = step;
    this.situation?.update(step);
  }

  /** Follow the substance of the map, for the concentration chart. */
  setSubstance(substance: string): void {
    if (substance === this.substance) return;
    this.substance = substance;
    this.showSubstance?.(substance);
  }

  private nodeLink(nodeId: number): HTMLElement {
    const link = el("a", { href: "#" }, this.nodeName(nodeId));
    link.addEventListener("click", (event) => {
      event.preventDefault();
      this.callbacks.select({ kind: "node", id: nodeId });
      this.callbacks.zoomTo({ kind: "node", id: nodeId });
    });
    return link;
  }

  private linkLink(row: number, other: "from" | "to"): HTMLElement {
    const { linkId, linkType, fromNodeId, toNodeId } = this.network;
    const id = linkId[row];
    const link = el("a", { href: "#" }, `${linkType[row]} link ${linkLabel(id)}`);
    link.addEventListener("click", (event) => {
      event.preventDefault();
      this.callbacks.select({ kind: "link", id });
    });
    const otherNode = other === "from" ? fromNodeId[row] : toNodeId[row];
    return el("li", {}, link, other === "from" ? " from " : " to ", this.nodeLink(otherNode));
  }

  private header(title: string, selection: Selection, diff?: FeatureDiff): HTMLElement {
    const zoom = el("button", { type: "button", title: "Zoom to" }, "Zoom to");
    zoom.addEventListener("click", () => this.callbacks.zoomTo(selection));
    const close = el("button", { type: "button", title: "Close", className: "close" }, "×");
    close.addEventListener("click", () => this.callbacks.close());
    const heading = el("h2", {}, title);
    if (diff) heading.append(" ", statusLabel(diff.status));
    return el("header", {}, heading, zoom, close);
  }

  /** The difference of a feature from the base model, when comparing models. */
  private featureDiff(selection: Selection): FeatureDiff | undefined {
    const diffs = selection.kind === "node" ? this.comparison?.diff.nodes : this.comparison?.diff.links;
    return diffs?.get(selection.id);
  }

  /** The model a feature is from: the base model for removed features. */
  private source(diff?: FeatureDiff): Manifest {
    return diff?.status === "removed" ? this.comparison!.base : this.manifest;
  }

  /** What differs from the base model, also in the results. */
  private changes(selection: Selection, diff: FeatureDiff | undefined): HTMLElement[] {
    if (!this.comparison) return [];
    const results = this.comparison.diff.results?.[selection.kind === "node" ? "basin" : "flow"];
    const result = describeResult(results, selection.id);
    const changes = [...(diff?.changes ?? []), ...(result ? [result] : [])];
    if (!changes.length) {
      const status = diff ? `${diff.status[0].toUpperCase()}${diff.status.slice(1)}.` : "Unchanged.";
      return [el("p", { className: "note" }, status)];
    }
    return [el("h3", {}, "Changes"), el("ul", {}, ...changes.map((change) => el("li", {}, change)))];
  }

  /** The base row of a feature in both models, to show the attributes that changed. */
  private baseRow(selection: Selection, diff: FeatureDiff | undefined): Promise<Row> | undefined {
    if (diff?.status !== "changed") return undefined;
    const base = this.comparison!.base;
    const baseId = diff.base_id ?? selection.id;
    const row = (selection.kind === "node" ? this.network.baseNodeRow : this.network.baseLinkRow).get(baseId);
    if (row === undefined) return undefined;
    return readRow(selection.kind === "node" ? base.files.nodes : base.files.links, row);
  }

  /** Result rows of a feature, merged with those of the base model when it is in both. */
  private async resultRows(kind: "basin" | "flow", id: number, diff: FeatureDiff | undefined): Promise<Row[]> {
    const idColumn = kind === "basin" ? "node_id" : "link_id";
    const source = this.source(diff).results;
    // Removed links have their negated base id
    const sourceId = diff?.status === "removed" && kind === "flow" ? -id : id;
    const head = source ? await readRowsById(source[kind].by_id, idColumn, sourceId) : [];
    const base = this.comparison?.base.results;
    if (!base || diff?.status === "removed" || diff?.status === "added") return head;
    // Links are renumbered, so also unchanged links can have another id in the base model
    const baseId = kind === "flow" ? this.network.baseLinkId.get(id) : id;
    if (baseId === undefined) return head;
    return mergeOnTime(head, await readRowsById(base[kind].by_id, idColumn, baseId));
  }

  /** The concentration of a substance in a Basin, merged with that of the base model when both have it. */
  private async concentrationRows(id: number, substance: string, diff: FeatureDiff | undefined): Promise<Row[]> {
    const read = async (set: ResultSet | undefined) =>
      set && substance in set.variables ? readRowsById(set.by_id, "node_id", id, ["time", substance]) : [];
    const head = await read(this.source(diff).results?.concentration);
    const base = this.comparison?.base.results?.concentration;
    if (!base || diff?.status === "removed" || diff?.status === "added") return head;
    return mergeOnTime(head, await read(base));
  }

  /**
   * The chart of the concentration of one substance in a Basin, also when the map shows something else, with the
   * list of substances. One at a time, since there can be many, of which some with other units.
   */
  private concentrationSection(id: number, diff: FeatureDiff | undefined, width: number, token: number): HTMLElement | null {
    const variables = this.source(diff).results?.concentration?.variables;
    if (!variables) return null;
    return lazySection(
      "Concentration",
      async () => {
        if (token !== this.token) return [];
        const select = el("select");
        addSubstances(select, variables);
        select.addEventListener("change", () => this.callbacks.chooseSubstance(select.value));
        const chart = el("div", {}, "Loading…");
        let latest = 0;
        const show = async (substance: string) => {
          if (!(substance in variables)) return;
          select.value = substance;
          const request = ++latest;
          const { label, units } = variables[substance];
          try {
            const rows = await this.concentrationRows(id, substance, diff);
            if (request === latest) chart.replaceChildren(...charts(rows, width, [[`${label} (${units})`, [substance]]]));
          } catch (error) {
            if (request === latest) chart.replaceChildren(el("p", { className: "error" }, `Failed to load: ${error}`));
          }
        };
        this.showSubstance = (substance) => void show(substance);
        void show(this.substance ?? Object.keys(variables)[0]);
        return [el("label", { className: "substance" }, "Substance ", select), chart];
      },
      true,
    );
  }

  /** The values of a column per time step of the results, NaN for a time step without a row. */
  private perStep(rows: Row[], column: string): Float64Array {
    const values = new Float64Array(this.times.length).fill(Number.NaN);
    for (const row of rows) {
      const step = this.steps.get((row.time as Date).getTime());
      const value = row[column];
      if (step !== undefined && typeof value === "number") values[step] = value;
    }
    return values;
  }

  /**
   * A section with a stacked chart of a tracer set of the shown model, see `TracerSet`, with the list of sets; the
   * panels of other features keep the chosen set.
   */
  private tracerSection(title: string, chart: (set: TracerSet) => Promise<(Node | string)[]>, token: number): HTMLElement | null {
    const sets = this.manifest.results?.concentration?.sets ?? [];
    if (sets.length === 0) return null;
    return lazySection(
      title,
      async () => {
        if (token !== this.token) return [];
        const select = el("select");
        select.append(...sets.map(({ name }) => new Option(name, name)));
        select.value = sets.find(({ name }) => name === this.tracerSet)?.name ?? sets[0].name;
        const content = el("div");
        let latest = 0;
        const show = async () => {
          const set = sets.find(({ name }) => name === select.value) ?? sets[0];
          this.tracerSet = set.name;
          const request = ++latest;
          content.replaceChildren("Loading…");
          try {
            const children = await chart(set);
            if (request === latest) content.replaceChildren(...children);
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            if (request === latest) content.replaceChildren(el("p", { className: "error" }, message));
          }
        };
        select.addEventListener("change", () => void show());
        void show();
        const note = this.comparison ? [el("p", { className: "note" }, "Of this model, not of the base model.")] : [];
        return [el("label", { className: "substance" }, "Tracer set ", select), content, ...note];
      },
      true,
    );
  }

  /** The fractions of the water of a Basin of the shown model per time step, from its concentrations. */
  private async basinFractions(set: TracerSet, id: number): Promise<Fractions> {
    const concentration = this.manifest.results?.concentration;
    if (!concentration) throw new Error("The model has no concentrations.");
    const columns = [CONTINUITY, ...set.tracers];
    const rows = await readRowsById(concentration.by_id, "node_id", id, ["time", ...columns]);
    if (rows.length === 0) throw new Error(`No concentrations of Basin #${id}.`);
    return basinFractions(set, Object.fromEntries(columns.map((column) => [column, this.perStep(rows, column)])));
  }

  /** The layers of a stacked chart of a set that are large enough to see, with their colors. */
  private layers(set: TracerSet, values: Map<string, Float64Array>, scale: number): Layer[] {
    const styles = layerStyles(set);
    return visibleLayers(values, scale).map((name) => ({ label: name, values: values.get(name)!, ...styles.get(name)! }));
  }

  /** What the layer of the water without a label is, if it is shown. */
  private static otherNote(layers: Layer[], untraced = ""): HTMLElement[] {
    if (!layers.some(({ label }) => label === OTHER)) return [];
    return [el("p", { className: "note" }, `${OTHER} is the water without any of these labels${untraced}.`)];
  }

  /** What the derived tracers of a set are, if they are shown, see `TracerSet`. */
  private static derivedNote(set: TracerSet, layers: Layer[]): HTMLElement[] {
    return layers.flatMap(({ label }) => {
      const derived = set.derived?.[label];
      if (!derived) return [];
      return [el("p", { className: "note" }, `${label} is the ${derived.tracer} tracer minus ${derived.minus.join(" and ")}.`)];
    });
  }

  /** The water of a Basin by its source, as stacked fractions; `subject` names the Basin in the heading. */
  private async basinFractionsChart(set: TracerSet, id: number, width: number, subject?: string): Promise<(Node | string)[]> {
    const layers = this.layers(set, await this.basinFractions(set, id), 1);
    if (layers.length === 0) return ["No water in this Basin."];
    return [
      el("h4", {}, `Fraction of the water${subject ? ` of ${subject}` : ""}`),
      stackedChart(this.times, layers, width),
      ...Panel.otherNote(layers),
      ...Panel.derivedNote(set, layers),
    ];
  }

  /** The label of a node of the shown model, like "Terminal #3". */
  private nodeName(id: number): string {
    const row = this.network.nodeRow.get(id);
    return row === undefined ? `#${id}` : `${this.network.nodeType[row]} #${id}`;
  }

  /** The Junctions in the Junction / static table of the shown model, which divide the flow they receive. */
  private staticJunctions(): Promise<Set<number>> {
    if (!this.staticJunctionIds) {
      const table = this.table("Junction", "static");
      const ids = table
        ? readColumns(table, ["node_id"]).then(({ node_id }) => new Set(node_id.map(Number)))
        : Promise.resolve(new Set<number>());
      ids.catch(() => (this.staticJunctionIds = null));
      this.staticJunctionIds = ids;
    }
    return this.staticJunctionIds;
  }

  /** The flow links of the shown model, with its Junctions in the Junction / static table. */
  private async graph(): Promise<FlowGraph> {
    return flowGraph(this.network, await this.staticJunctions());
  }

  /**
   * The flow over links by the source of its water, as stacked flows, with their total flow rate: that of a link, the
   * flow of a node, see `nodeFlow`, or the simulated flow of an Observation node, see `observedFractions`.
   */
  private async flowFractionsChart(set: TracerSet, links: number[], width: number, chart: FlowChart = {}): Promise<(Node | string)[]> {
    if (links.length === 0) return ["No flow links to trace."];
    const graph = await this.graph();
    const parts = links.map((link) => flowParts(graph, link));
    const notes: HTMLElement[] = [];
    if (links.length > 1) notes.push(note(`The total flow of links ${links.map((link) => `#${link}`).join(", ")}.`));
    parts.forEach(({ terms: behind, basin }, i) => {
      if (basin === null) return;
      notes.push(
        note(
          `${links.length > 1 ? `Link #${links[i]} is part` : "Part"} of the Junction tree of ${this.nodeName(basin)}: `,
          `the net flow of the ${behind.length} links of the tree behind it, which carry the water of where they come `,
          "from towards the Basin, and that of the Basin away from it.",
        ),
      );
    });
    const terms = parts.flatMap((part) => part.terms);
    return this.partsChart(set, graph, terms, width, { ...chart, notes: [...notes, ...(chart.notes ?? [])] }, links);
  }

  /** The inflow or outflow of a Basin by the source of its water, with that of the Basin results, see `basinFlowParts`. */
  private async basinFlowChart(
    set: TracerSet,
    basin: number,
    inflow: boolean,
    width: number,
    chart: FlowChart = {},
  ): Promise<(Node | string)[]> {
    const graph = await this.graph();
    const variable = inflow ? "inflow_rate" : "outflow_rate";
    const results = this.manifest.results;
    if (!results) return ["No simulation results."];
    const rows = await readRowsById(results.basin.by_id, "node_id", basin, ["time", variable]);
    const exchange =
      `The water that ${this.nodeName(basin)} exchanges with each neighbor, also with those of its Junction trees, ` +
      `where it flows ${inflow ? "in" : "out"}.`;
    return this.partsChart(set, graph, basinFlowParts(graph, basin, inflow), width, {
      variable: variable.replace("_", " "),
      ...chart,
      total: this.perStep(rows, variable),
      notes: [note(exchange), ...(chart.notes ?? [])],
    });
  }

  /** The flow of a node by the source of its water, see `nodeFlow`. */
  private async nodeFlowChart(set: TracerSet, node: number, width: number): Promise<(Node | string)[]> {
    const flow = nodeFlow(await this.graph(), node);
    if (!flow) return ["This node has no flow to trace."];
    return this.flowFractionsChart(set, flow.links, width, { variable: flow.variable.replace("_", " ") });
  }

  /**
   * The water of the node an Observation node observes, by its source: the simulated flow, with the observed values,
   * or the inflow or outflow of a Basin. The water in an observed Basin is charted for its other variables, like its
   * level.
   */
  private async observedFractions(set: TracerSet, nodeId: number, observedId: number, width: number): Promise<(Node | string)[]> {
    const subject = this.nodeName(observedId);
    const rows = await this.tableRows("Observation", "time", nodeId);
    const observed = (variable: string) => this.perStep(rows.filter((row) => row.variable === variable), "value");
    const row = this.network.nodeRow.get(observedId);
    if (row === undefined || this.network.nodeType[row] !== "Basin") {
      const links = await this.observedLinks(nodeId, observedId);
      return this.flowFractionsChart(set, links, width, { subject, observed: observed("flow_rate") });
    }
    const variables = new Set(rows.map((observation) => String(observation.variable)));
    const flows = ["inflow_rate", "outflow_rate"].filter((variable) => variables.has(variable));
    const charts = await Promise.all(
      flows.map((variable) =>
        this.basinFlowChart(set, observedId, variable === "inflow_rate", width, { subject, observed: observed(variable) }),
      ),
    );
    const others = flows.length < variables.size || flows.length === 0;
    return [...charts.flat(), ...(others ? await this.basinFractionsChart(set, observedId, width, subject) : [])];
  }

  /**
   * A stacked chart of the parts of a flow by the source of their water, with the total flow rate, which is that of the
   * `links` if given; the parts of the flow over links must add up to it.
   */
  private async partsChart(
    set: TracerSet,
    graph: FlowGraph,
    terms: FlowTerm[],
    width: number,
    chart: FlowChart,
    links: number[] = [],
  ): Promise<(Node | string)[]> {
    const traced = tracedWaters(graph, terms);
    const steps = this.times.length;
    const linkIds = [...new Set([...links, ...traced.links])];
    const basins = [...traced.basins];
    const [flows, fractions] = await Promise.all([
      Promise.all(linkIds.map((id) => this.flowSeries(id))),
      Promise.all(basins.map((id) => this.basinFractions(set, id))),
    ]);
    const flowOf = new Map(linkIds.map((id, i) => [id, flows[i] ?? new Float64Array(steps).fill(Number.NaN)]));
    const used: UsedWaters = { mixed: new Set(), untraced: new Set() };
    const fractionsOf = waterFractions(set, graph, flowOf, new Map(basins.map((id, i) => [id, fractions[i]])), steps, used);
    let total = chart.total;
    if (!total) {
      total = new Float64Array(steps);
      for (const link of links) flowOf.get(link)!.forEach((value, i) => (total![i] += value));
    }
    const scale = Math.max(0, ...Array.from(total, Math.abs).filter(Number.isFinite));
    const layers = this.layers(set, layerFlows(set, terms, flowOf, fractionsOf, steps), scale);

    const notes = [...(chart.notes ?? [])];
    if (used.mixed.size) {
      const names = [...used.mixed].map((id) => this.nodeName(id)).join(", ");
      const [verb, object] = used.mixed.size > 1 ? ["mix", "them"] : ["mixes", "it"];
      notes.push(note(`${names} ${verb} the water that flows into ${object} by the flows of each time step, like the core.`));
    }
    // The parts add up to the flow over the links, unless the Junction trees are read otherwise than the core does
    const differences = partsTotal(terms, flowOf, steps).map((value, i) => Math.abs(value - total[i]));
    const mismatch = Math.max(0, ...differences.filter(Number.isFinite));
    if (links.length && mismatch > 1e-6 + 1e-3 * scale) {
      const message = `The flows of the links behind differ up to ${formatNumber(mismatch)} m³ s⁻¹ from the flow rate.`;
      notes.push(el("p", { className: "error" }, message));
    }
    const untraced = [...used.untraced].map((id) => this.nodeName(id));
    const untracedNote = untraced.length ? `, and that of ${untraced.join(", ")}, which is not traced` : "";
    const variable = chart.variable ?? "flow rate";
    const lines: ChartLine[] = chart.observed
      ? [
          { label: "simulated", values: total },
          { label: "observed", values: chart.observed, dashed: true },
        ]
      : [{ label: variable, values: total }];
    const heading = `${variable[0].toUpperCase()}${variable.slice(1)}${chart.subject ? ` of ${chart.subject}` : ""}`;
    return [
      el("h4", {}, `${heading} by the source of the water (m³ s⁻¹)`),
      stackedChart(this.times, layers, width, lines),
      ...Panel.otherNote(layers, untracedNote),
      ...Panel.derivedNote(set, layers),
      ...notes,
    ];
  }

  /** Input tables of a node; changed tables are shown for both models. */
  private tableSections(nodeType: string, nodeId: number, diff: FeatureDiff | undefined, width: number): HTMLElement[] {
    const source = this.source(diff);
    const base = this.comparison?.base;
    const tables = source.tables.filter((table) => table.node_type === nodeType);
    const baseTables = diff?.status === "changed" && base ? base.tables.filter((table) => table.node_type === nodeType) : [];
    const names = [...new Set([...tables, ...baseTables].map((table) => table.name))].sort((a, b) => a.localeCompare(b));
    return names.flatMap((name) => {
      const changed = diff?.status === "changed" && diff.changes.includes(name);
      const head = tables.find((table) => table.name === name);
      const baseTable = changed ? baseTables.find((table) => table.name === name) : undefined;
      return [
        ...(head ? [tableSection(head, nodeId, width, changed ? `${name} (changed)` : name)] : []),
        ...(baseTable ? [tableSection(baseTable, nodeId, width, `${name}${BASE}`)] : []),
      ];
    });
  }

  show(selection: Selection): void {
    const token = ++this.token;
    this.situation = null;
    this.showSubstance = null;
    const { network } = this;
    const diff = this.featureDiff(selection);
    const manifest = this.source(diff);
    const width = this.contentWidth();
    const attributes = el("div", {}, "Loading…");
    const sections: HTMLElement[] = [];

    if (selection.kind === "node") {
      const row = network.nodeRow.get(selection.id);
      if (row === undefined) return this.hide();
      const nodeType = network.nodeType[row];
      sections.push(this.header(`${nodeType} #${selection.id}`, selection, diff), ...this.changes(selection, diff), attributes);

      const incoming: HTMLElement[] = [];
      const outgoing: HTMLElement[] = [];
      const flowLinks: [number, string][] = [];
      const upstream: number[] = [];
      const downstream: number[] = [];
      const controllers: number[] = [];
      const outflows: number[] = [];
      // Inflow and outflow of a connector are equal, so only the outflow is charted
      const isConnector = CONNECTOR_TYPES.has(nodeType);
      network.linkId.forEach((linkId, linkRow) => {
        const isFlow = network.linkType[linkRow] === "flow";
        if (network.toNodeId[linkRow] === selection.id) {
          incoming.push(this.linkLink(linkRow, "from"));
          if (isFlow && !isConnector) flowLinks.push([linkId, `in #${linkId}`]);
          if (isFlow) upstream.push(network.fromNodeId[linkRow]);
          if (network.linkType[linkRow] === "control") controllers.push(network.fromNodeId[linkRow]);
        }
        if (network.fromNodeId[linkRow] === selection.id) {
          outgoing.push(this.linkLink(linkRow, "to"));
          if (isFlow) flowLinks.push([linkId, `out #${linkId}`]);
          if (isFlow) downstream.push(network.toNodeId[linkRow]);
          if (isFlow) outflows.push(linkId);
        }
      });
      if (incoming.length) sections.push(el("h3", {}, "Incoming links"), el("ul", {}, ...incoming));
      if (outgoing.length) sections.push(el("h3", {}, "Outgoing links"), el("ul", {}, ...outgoing));

      const results = manifest.results;
      if (nodeType === "Observation" && diff?.status !== "removed") {
        const observed = network.linkId.findIndex(
          (_, linkRow) => network.linkType[linkRow] === "observation" && network.fromNodeId[linkRow] === selection.id,
        );
        if (observed >= 0) {
          const observedId = network.toNodeId[observed];
          sections.push(
            lazySection("Observed and simulated", () => this.observedSimulated(selection.id, observedId, width), true),
          );
          // The water of the observed node, like in its own panel
          const observedRow = network.nodeRow.get(observedId);
          const title = observedRow !== undefined && network.nodeType[observedRow] === "Basin" ? "Fractions" : "Fractional flow";
          const chart = (set: TracerSet) => this.observedFractions(set, selection.id, observedId, width);
          const fractions = results && this.tracerSection(title, chart, token);
          if (fractions) sections.push(fractions);
        }
      }
      // The stacked charts are of the shown model, which has no removed features
      const shown = diff?.status !== "removed";
      if (results && nodeType === "Basin") {
        sections.push(
          lazySection(
            "Results",
            async () => charts(await this.resultRows("basin", selection.id, diff), width, BASIN_CHARTS),
            true,
          ),
        );
        const fractions = shown && this.tracerSection("Fractions", (set) => this.basinFractionsChart(set, selection.id, width), token);
        if (fractions) sections.push(fractions);
        const concentration = this.concentrationSection(selection.id, diff, width, token);
        if (concentration) sections.push(concentration);
      } else if (results && flowLinks.length && shown) {
        sections.push(lazySection("Flow results", () => this.linkFlows(flowLinks, width), true));
        const chart = (set: TracerSet) => this.nodeFlowChart(set, selection.id, width);
        const fractions = FLOW_NODE_TYPES.has(nodeType) && this.tracerSection("Fractional flow", chart, token);
        if (fractions) sections.push(fractions);
      }
      // The situation combines head model inputs and results
      if (isConnector && diff?.status !== "removed") {
        const title = this.times.length ? "Situation at the current time" : "Situation";
        sections.push(
          lazySection(
            title,
            async () => {
              const situation = await this.connectorSituation(nodeType, selection.id, width, {
                upstream: upstream[0],
                downstream: downstream[0],
                controller: controllers[0],
                outflow: outflows[0],
              });
              if (token !== this.token) return [];
              this.situation = situation;
              situation.update(this.step);
              return [situation.element];
            },
            true,
          ),
        );
      }

      const tables = this.tableSections(nodeType, selection.id, diff, width);
      if (tables.length) sections.push(el("h3", {}, "Tables"), ...tables);
      this.render(sections);
      void this.fillAttributes(
        token,
        attributes,
        readRow(manifest.files.nodes, network.nodeFileRow[row]),
        this.baseRow(selection, diff),
      );
    } else {
      const row = network.linkRow.get(selection.id);
      if (row === undefined) return this.hide();
      sections.push(
        this.header(`${network.linkType[row]} link ${linkLabel(selection.id)}`, selection, diff),
        el("p", {}, "From ", this.nodeLink(network.fromNodeId[row]), " to ", this.nodeLink(network.toNodeId[row])),
        ...this.changes(selection, diff),
        attributes,
      );
      const results = manifest.results;
      if (results && network.linkType[row] === "flow") {
        sections.push(
          lazySection(
            "Results",
            async () => {
              const rows = await this.resultRows("flow", selection.id, diff);
              return [
                ...charts(rows, width, [["Flow", ["flow_rate"]]]),
                ...cumulativeChart(rows, width, ["flow_rate", `flow_rate${BASE}`]),
              ];
            },
            true,
          ),
        );
        const fractions =
          diff?.status !== "removed" &&
          this.tracerSection("Fractional flow", (set) => this.flowFractionsChart(set, [selection.id], width), token);
        if (fractions) sections.push(fractions);
      }
      this.render(sections);
      void this.fillAttributes(
        token,
        attributes,
        readRow(manifest.files.links, network.linkFileRow[row]),
        this.baseRow(selection, diff),
      );
    }
  }

  private table(nodeType: string, kind: string): TableEntry | undefined {
    return this.manifest.tables.find((table) => table.name === `${nodeType} / ${kind}`);
  }

  private async tableRows(nodeType: string, kind: string, nodeId: number): Promise<Row[]> {
    const table = this.table(nodeType, kind);
    return table ? readRowsById(table, "node_id", nodeId) : [];
  }

  /** Water level per time step: Basin results, or the LevelBoundary input. */
  private async levelSeries(nodeType: string, nodeId: number): Promise<Float64Array | null> {
    const results = this.manifest.results;
    const steps = Math.max(this.times.length, 1);
    if (nodeType === "Basin" && results) {
      return this.perStep(await readRowsById(results.basin.by_id, "node_id", nodeId, ["time", "level"]), "level");
    }
    if (nodeType !== "LevelBoundary") return null;
    const rows = await this.tableRows(nodeType, "time", nodeId);
    if (rows.length && this.times.length) {
      return Float64Array.from(lastRowPerStep(rows, this.times), (row) => (row?.level as number) ?? Number.NaN);
    }
    const [constant] = await this.tableRows(nodeType, "static", nodeId);
    return typeof constant?.level === "number" ? new Float64Array(steps).fill(constant.level) : null;
  }

  /** The levels on one side of a connector node, looking past Junctions to the Basin behind them. */
  private async levelSide(
    neighbor: number | undefined,
    direction: "upstream" | "downstream",
    limit: LevelSeries | null,
  ): Promise<LevelSide> {
    const nodeId = neighbor === undefined ? undefined : pastJunctions(this.network, neighbor, direction);
    const row = nodeId === undefined ? undefined : this.network.nodeRow.get(nodeId);
    if (nodeId === undefined || row === undefined) return { name: "none", levels: null, bottom: null, limit };
    const nodeType = this.network.nodeType[row];
    const via = nodeId === neighbor ? "" : ` (via Junction #${neighbor})`;
    const [levels, profile] = await Promise.all([
      this.levelSeries(nodeType, nodeId),
      nodeType === "Basin" ? this.tableRows(nodeType, "profile", nodeId) : [],
    ]);
    const bottoms = profile.map((r) => r.level).filter((level): level is number => typeof level === "number");
    return {
      name: `${nodeType} #${nodeId}${via}`,
      levels,
      bottom: bottoms.length ? Math.min(...bottoms) : null,
      limit,
    };
  }

  /** The control state per time step, from the results of a DiscreteControl node. */
  private async controlStates(controller: number | undefined): Promise<(string | null)[] | null> {
    const control = this.manifest.results?.control;
    const row = controller === undefined ? undefined : this.network.nodeRow.get(controller);
    if (!control || row === undefined || this.network.nodeType[row] !== "DiscreteControl") return null;
    const rows = await readRowsById(control, "control_node_id", controller!);
    return lastRowPerStep(rows, this.times).map((row) => (row ? String(row.control_state) : null));
  }

  /** Flow rate per time step of a link. */
  private async flowSeries(linkId: number | undefined): Promise<Float64Array | null> {
    const flow = this.manifest.results?.flow;
    if (!flow || linkId === undefined) return null;
    return this.perStep(await readRowsById(flow.by_id, "link_id", linkId, ["time", "flow_rate"]), "flow_rate");
  }

  /** Levels, control state and limits, and flow rate around a connector node, per time step. */
  private async connectorSituation(
    nodeType: string,
    nodeId: number,
    width: number,
    neighbors: { upstream?: number; downstream?: number; controller?: number; outflow?: number },
  ): Promise<Situation> {
    const steps = Math.max(this.times.length, 1);
    const [rows, states, flow] = await Promise.all([
      this.tableRows(nodeType, "static", nodeId),
      this.controlStates(neighbors.controller),
      this.flowSeries(neighbors.outflow),
    ]);
    // The static row in use: the one of the current control state, or the only one without control
    const uncontrolled = rows.length === 1 ? rows[0] : rows.find((row) => row.control_state == null);
    const activeRows = Array.from({ length: steps }, (_, step) => {
      const state = states?.[step];
      return state ? rows.find((row) => row.control_state === state) : uncontrolled;
    });
    const limit = (column: string, label: string): LevelSeries | null => {
      const values = Float64Array.from(activeRows, (row) =>
        typeof row?.[column] === "number" ? (row[column] as number) : Number.NaN,
      );
      return values.some(Number.isFinite) ? { label, values } : null;
    };
    const [up, down] = await Promise.all([
      this.levelSide(neighbors.upstream, "upstream", limit("min_upstream_level", "min upstream")),
      this.levelSide(neighbors.downstream, "downstream", limit("max_downstream_level", "max downstream")),
    ]);
    const diagram = new LevelDiagram(up, down, width);

    const controllerRow = neighbors.controller === undefined ? undefined : this.network.nodeRow.get(neighbors.controller);
    const controllerName =
      controllerRow === undefined ? "" : ` (${this.network.nodeType[controllerRow]} #${neighbors.controller})`;
    const stateValue = el("dd");
    const flowValue = el("dd");
    const facts = el("dl", { className: "facts" }, el("dt", {}, "Control state"), stateValue);
    if (flow) facts.append(el("dt", {}, "Flow rate"), flowValue);
    const element = el("div", {}, facts, diagram.element, levelLegend());
    return {
      element,
      update: (step) => {
        const state = states?.[step];
        stateValue.textContent =
          neighbors.controller === undefined ? "not controlled" : `${states ? (state ?? "unknown") : "–"}${controllerName}`;
        if (flow) flowValue.textContent = `${formatNumber(flow[step])} m³ s⁻¹`;
        diagram.update(step);
      },
    };
  }

  /**
   * The simulated values of a variable of an observed node in the results of a model. The simulated flow rate is the
   * total flow of `links`, see `ribasim_nl.goodness_of_fit`.
   */
  private async simulatedRows(
    results: Manifest["results"],
    observedType: string,
    observedId: number,
    variable: string,
    links: number[],
  ): Promise<Row[]> {
    if (results && observedType === "Basin") return readRowsById(results.basin.by_id, "node_id", observedId);
    if (!results || variable !== "flow_rate" || links.length === 0) return [];
    const series = await Promise.all(links.map((id) => readRowsById(results.flow.by_id, "link_id", id)));
    const total = new Map<number, Row>();
    for (const row of series.flat()) {
      const time = (row.time as Date).getTime();
      const sum = total.get(time);
      if (sum) sum.flow_rate = (sum.flow_rate as number) + (row.flow_rate as number);
      else total.set(time, { time: row.time, flow_rate: row.flow_rate });
    }
    return [...total.values()];
  }

  /**
   * The flow links of the shown model whose total flow is the simulated flow rate of an Observation node, see
   * `ribasim_nl.goodness_of_fit`; without the exported links, the outflows of the observed node.
   */
  private async observedLinks(nodeId: number, observedId: number): Promise<number[]> {
    const { network } = this;
    const observation = this.manifest.results?.observation;
    const links = observation ? await loadObservedLinks(observation) : null;
    return (
      links?.get(nodeId) ??
      Array.from(network.linkId.filter((_, row) => network.linkType[row] === "flow" && network.fromNodeId[row] === observedId))
    );
  }

  /**
   * Per observed variable, how well the results fit, and a chart of the observed values and the simulated values of
   * the observed node, also of the base model when comparing models.
   */
  private async observedSimulated(nodeId: number, observedId: number, width: number): Promise<(Node | string)[]> {
    const { network } = this;
    const row = network.nodeRow.get(observedId);
    if (row === undefined) return ["The observed node is not in the model."];
    const observedType = network.nodeType[row];
    const results = this.manifest.results;
    const base = this.comparison?.base.results;
    const [observations, fits, baseFits, outflows, baseLinks] = await Promise.all([
      this.tableRows("Observation", "time", nodeId),
      results?.observation ? loadFits(results.observation) : null,
      base?.observation ? loadFits(base.observation) : null,
      this.observedLinks(nodeId, observedId),
      base?.observation ? loadObservedLinks(base.observation) : null,
    ]);
    // Links are renumbered and can lead to renumbered nodes, so without the exported links of the base model, the
    // outflows of the observed node, which keeps its id, are those of the base model
    const baseOutflows = baseLinks?.get(nodeId) ?? network.baseOutflows.get(observedId) ?? [];
    const columns = ["observed", "simulated", ...(base ? [`simulated${BASE}`] : [])];
    const variables = [...new Set(observations.map((observation) => String(observation.variable)))].sort((a, b) => a.localeCompare(b));
    if (variables.length === 0) return ["No observed values."];
    const sections = await Promise.all(
      variables.map(async (variable) => {
        const [simulated, baseSimulated] = await Promise.all([
          this.simulatedRows(results, observedType, observedId, variable, outflows),
          this.simulatedRows(base, observedType, observedId, variable, baseOutflows),
        ]);
        const byTime = new Map<number, Row>();
        const add = (rows: Row[], column: string, value: (row: Row) => unknown) => {
          for (const row of rows) {
            const time = (row.time as Date).getTime();
            let merged = byTime.get(time);
            if (!merged) byTime.set(time, (merged = { time: row.time }));
            merged[column] = value(row);
          }
        };
        add(
          observations.filter((observation) => observation.variable === variable),
          "observed",
          (observation) => observation.value,
        );
        add(simulated, "simulated", (result) => result[variable]);
        add(baseSimulated, `simulated${BASE}`, (result) => result[variable]);
        const rows = [...byTime.values()].sort((a, b) => (a.time as Date).getTime() - (b.time as Date).getTime());
        const chart = timeChart(rows, width, columns);
        const title = `${variable} of ${observedType} #${observedId}`;
        const fit = (model: Fits | null) => model?.get(variable)?.get(nodeId);
        const facts = fits ? [fitFacts(fit(fits), fit(baseFits), results!.observation!.min_days, baseFits !== null)] : [];
        if (!chart) return [el("h4", {}, title), ...facts, "Too few values to chart."];
        return [
          el("h4", {}, title),
          ...facts,
          chart,
          ...(variable === "flow_rate" ? cumulativeChart(rows, width, columns) : []),
        ];
      }),
    );
    return [...sections.flat(), ...(results ? [] : [el("p", { className: "note" }, "No simulation results.")])];
  }

  /** One chart with the flow rate of each link, merged on time. */
  private async linkFlows(links: [number, string][], width: number): Promise<(Node | string)[]> {
    const flow = this.manifest.results!.flow;
    const shown = links.slice(0, MAX_LINK_SERIES);
    const series = await Promise.all(shown.map(([id]) => readRowsById(flow.by_id, "link_id", id)));
    const byTime = new Map<number, Row>();
    series.forEach((rows, i) => {
      for (const row of rows) {
        const time = (row.time as Date).getTime();
        let merged = byTime.get(time);
        if (!merged) byTime.set(time, (merged = { time: row.time }));
        merged[shown[i][1]] = row.flow_rate;
      }
    });
    const rows = [...byTime.values()].sort((a, b) => (a.time as Date).getTime() - (b.time as Date).getTime());
    const chart = timeChart(rows, width);
    const cumulative = cumulativeChart(rows, width, shown.map(([, label]) => label));
    const note = links.length > shown.length ? [el("p", { className: "note" }, `Showing ${shown.length} of ${links.length} links.`)] : [];
    return chart ? [el("h4", {}, "Flow rate (m³ s⁻¹)"), chart, ...cumulative, ...note] : ["No results for these links."];
  }

  /** Where the panel is on the page, or null if it is hidden. */
  rect(): DOMRect | null {
    return this.element.hidden ? null : this.element.getBoundingClientRect();
  }

  /** Width available for charts; the panel is shown first, since a hidden panel has no width. */
  private contentWidth(): number {
    this.element.hidden = false;
    const style = getComputedStyle(this.element);
    const width = this.element.clientWidth - Number.parseFloat(style.paddingLeft) - Number.parseFloat(style.paddingRight);
    return width > 0 ? Math.floor(width) : 360;
  }

  private render(sections: HTMLElement[]): void {
    this.element.replaceChildren(...sections);
    this.element.hidden = false;
    this.element.scrollTop = 0;
  }

  private async fillAttributes(
    token: number,
    container: HTMLElement,
    row: Promise<Row>,
    base?: Promise<Row>,
  ): Promise<void> {
    try {
      const attributes = attributeTable(await row, await base);
      if (token === this.token) container.replaceChildren(attributes);
    } catch (error) {
      if (token === this.token) container.replaceChildren(el("p", { className: "error" }, `Failed to load: ${error}`));
    }
  }

  hide(): void {
    this.token++;
    this.situation = null;
    this.element.hidden = true;
    this.element.replaceChildren();
  }
}
