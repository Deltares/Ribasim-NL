import uPlot from "uplot";
import { formatNumber } from "./colors";
import {
  readRow,
  readRowsById,
  type Comparison,
  type FeatureDiff,
  type Manifest,
  type Row,
  type TableEntry,
} from "./data";
import { describeResult, statusBadge } from "./diff";
import { LevelDiagram, type LevelSeries, type LevelSide } from "./levels";
import { linkLabel, type Network } from "./network";

export type Selection = { kind: "node" | "link"; id: number };

const MAX_TABLE_ROWS = 2000;
const MAX_LINK_SERIES = 8;
const HIDDEN_COLUMNS = new Set(["x", "y", "coords"]);
const ID_COLUMNS = new Set(["node_id", "link_id"]);
/** Suffix of the series and tables of the base model, when comparing models */
const BASE = " (base)";
/** Nodes with one flow in and out, see https://ribasim.org/reference/ */
const CONNECTOR_TYPES = new Set(["TabulatedRatingCurve", "Outlet", "Pump", "LinearResistance", "ManningResistance"]);
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

/** A time series chart of numeric columns, if the rows form one. */
function timeChart(rows: Row[], width: number, only?: string[]): HTMLElement | null {
  if (rows.length < 2 || !rows.every((row) => row.time instanceof Date)) return null;
  const columns = (only ?? Object.keys(rows[0])).filter(
    (column) => !ID_COLUMNS.has(column) && rows.some((row) => typeof row[column] === "number"),
  );
  if (columns.length === 0) return null;
  const data: uPlot.AlignedData = [
    rows.map((row) => (row.time as Date).getTime() / 1000),
    ...columns.map((column) => rows.map((row) => (typeof row[column] === "number" ? (row[column] as number) : null))),
  ];
  const colors = columns.filter((column) => !column.endsWith(BASE));
  const container = el("div", { className: "chart" });
  const seriesValue = (_: uPlot, value: number | null) => (value === null ? "–" : formatNumber(value));
  const plot = new uPlot(
    {
      width,
      height: 200,
      series: [
        { value: "{YYYY}-{MM}-{DD}" },
        ...columns.map((label) => {
          const isBase = label.endsWith(BASE);
          const color = SERIES_COLORS[colors.indexOf(isBase ? label.slice(0, -BASE.length) : label) % SERIES_COLORS.length];
          return { label, stroke: color, dash: isBase ? [4, 4] : undefined, value: seriesValue };
        }),
      ],
      scales: { x: { time: true } },
      axes: [{}, { size: 60, values: (_, ticks) => ticks.map(formatNumber) }],
    },
    data,
  );
  container.append(plot.root);
  return container;
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

export interface PanelCallbacks {
  select: (selection: Selection) => void;
  zoomTo: (selection: Selection) => void;
  close: () => void;
}

export class Panel {
  private readonly element: HTMLElement;
  private token = 0;
  private step = 0;
  private readonly times: Date[];
  private situation: Situation | null = null;

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
  }

  /** Follow the time step of the map, for the level diagram. */
  setStep(step: number): void {
    this.step = step;
    this.situation?.update(step);
  }

  private nodeLink(nodeId: number): HTMLElement {
    const row = this.network.nodeRow.get(nodeId);
    const label = row === undefined ? `#${nodeId}` : `${this.network.nodeType[row]} #${nodeId}`;
    const link = el("a", { href: "#" }, label);
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
    if (diff) heading.append(" ", statusBadge(diff.status));
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

  /** Input tables of a node; changed tables are shown for both models. */
  private tableSections(nodeType: string, nodeId: number, diff: FeatureDiff | undefined, width: number): HTMLElement[] {
    const source = this.source(diff);
    const base = this.comparison?.base;
    const tables = source.tables.filter((table) => table.node_type === nodeType);
    const baseTables = diff?.status === "changed" && base ? base.tables.filter((table) => table.node_type === nodeType) : [];
    const names = [...new Set([...tables, ...baseTables].map((table) => table.name))].sort();
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
      if (results && nodeType === "Basin") {
        sections.push(
          lazySection(
            "Results",
            async () => charts(await this.resultRows("basin", selection.id, diff), width, BASIN_CHARTS),
            true,
          ),
        );
      } else if (results && flowLinks.length && diff?.status !== "removed") {
        sections.push(lazySection("Flow results", () => this.linkFlows(flowLinks, width), true));
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
            async () => charts(await this.resultRows("flow", selection.id, diff), width, [["Flow", ["flow_rate"]]]),
            true,
          ),
        );
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
      const byTime = new Map<number, number>();
      for (const row of await readRowsById(results.basin.by_id, "node_id", nodeId)) {
        byTime.set((row.time as Date).getTime(), row.level as number);
      }
      return Float64Array.from(this.times, (time) => byTime.get(time.getTime()) ?? Number.NaN);
    }
    if (nodeType !== "LevelBoundary") return null;
    const rows = await this.tableRows(nodeType, "time", nodeId);
    if (rows.length && this.times.length) {
      return Float64Array.from(lastRowPerStep(rows, this.times), (row) => (row?.level as number) ?? Number.NaN);
    }
    const [constant] = await this.tableRows(nodeType, "static", nodeId);
    return typeof constant?.level === "number" ? new Float64Array(steps).fill(constant.level) : null;
  }

  private async levelSide(nodeId: number | undefined, limit: LevelSeries | null): Promise<LevelSide> {
    const row = nodeId === undefined ? undefined : this.network.nodeRow.get(nodeId);
    if (nodeId === undefined || row === undefined) return { name: "none", levels: null, bottom: null, limit };
    const nodeType = this.network.nodeType[row];
    const [levels, profile] = await Promise.all([
      this.levelSeries(nodeType, nodeId),
      nodeType === "Basin" ? this.tableRows(nodeType, "profile", nodeId) : [],
    ]);
    const bottoms = profile.map((r) => r.level).filter((level): level is number => typeof level === "number");
    return {
      name: `${nodeType} #${nodeId}`,
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
    const byTime = new Map<number, number>();
    for (const row of await readRowsById(flow.by_id, "link_id", linkId)) {
      byTime.set((row.time as Date).getTime(), row.flow_rate as number);
    }
    return Float64Array.from(this.times, (time) => byTime.get(time.getTime()) ?? Number.NaN);
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
      this.levelSide(neighbors.upstream, limit("min_upstream_level", "min upstream")),
      this.levelSide(neighbors.downstream, limit("max_downstream_level", "max downstream")),
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
    const note = links.length > shown.length ? [el("p", { className: "note" }, `Showing ${shown.length} of ${links.length} links.`)] : [];
    return chart ? [el("h4", {}, "Flow rate (m³ s⁻¹)"), chart, ...note] : ["No results for these links."];
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
