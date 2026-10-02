import uPlot from "uplot";
import { formatNumber } from "./colors";
import { readRow, readRowsById, type Manifest, type Row, type TableEntry } from "./data";
import type { Network } from "./network";

export type Selection = { kind: "node" | "link"; id: number };

const MAX_TABLE_ROWS = 2000;
const MAX_LINK_SERIES = 8;
const HIDDEN_COLUMNS = new Set(["x", "y", "coords"]);
const ID_COLUMNS = new Set(["node_id", "link_id"]);
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

function attributeTable(row: Row): HTMLTableElement {
  const body = el("tbody");
  for (const [key, value] of Object.entries(row)) {
    if (HIDDEN_COLUMNS.has(key) || isEmpty(value)) continue;
    body.append(el("tr", {}, el("th", {}, key), el("td", {}, formatValue(value))));
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
  const container = el("div", { className: "chart" });
  const seriesValue = (_: uPlot, value: number | null) => (value === null ? "–" : formatNumber(value));
  const plot = new uPlot(
    {
      width,
      height: 200,
      series: [
        { value: "{YYYY}-{MM}-{DD}" },
        ...columns.map((label, i) => ({ label, stroke: SERIES_COLORS[i % SERIES_COLORS.length], value: seriesValue })),
      ],
      scales: { x: { time: true } },
      axes: [{}, { size: 60, values: (_, ticks) => ticks.map(formatNumber) }],
    },
    data,
  );
  container.append(plot.root);
  return container;
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

function tableSection(entry: TableEntry, nodeId: number, width: number): HTMLElement {
  return lazySection(entry.name, async () => {
    const rows = await readRowsById(entry, "node_id", nodeId);
    if (rows.length === 0) return ["No rows for this node."];
    const chart = timeChart(rows, width);
    return chart ? [chart, dataTable(rows)] : [dataTable(rows)];
  });
}

function charts(rows: Row[], width: number, specs: [string, string[]][]): (Node | string)[] {
  if (rows.length === 0) return ["No results for this feature."];
  return specs.flatMap(([title, columns]) => {
    const chart = timeChart(rows, width, columns);
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

  constructor(
    parent: HTMLElement,
    private readonly manifest: Manifest,
    private readonly network: Network,
    private readonly callbacks: PanelCallbacks,
  ) {
    this.element = el("div", { className: "panel", hidden: true });
    parent.append(this.element);
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
    const link = el("a", { href: "#" }, `${linkType[row]} link #${id}`);
    link.addEventListener("click", (event) => {
      event.preventDefault();
      this.callbacks.select({ kind: "link", id });
    });
    const otherNode = other === "from" ? fromNodeId[row] : toNodeId[row];
    return el("li", {}, link, other === "from" ? " from " : " to ", this.nodeLink(otherNode));
  }

  private header(title: string, selection: Selection): HTMLElement {
    const zoom = el("button", { type: "button", title: "Zoom to" }, "Zoom to");
    zoom.addEventListener("click", () => this.callbacks.zoomTo(selection));
    const close = el("button", { type: "button", title: "Close", className: "close" }, "×");
    close.addEventListener("click", () => this.callbacks.close());
    return el("header", {}, el("h2", {}, title), zoom, close);
  }

  show(selection: Selection): void {
    const token = ++this.token;
    const { network, manifest } = this;
    const width = this.element.clientWidth || 360;
    const attributes = el("div", {}, "Loading…");
    const sections: HTMLElement[] = [];

    if (selection.kind === "node") {
      const row = network.nodeRow.get(selection.id);
      if (row === undefined) return this.hide();
      const nodeType = network.nodeType[row];
      sections.push(this.header(`${nodeType} #${selection.id}`, selection), attributes);

      const incoming: HTMLElement[] = [];
      const outgoing: HTMLElement[] = [];
      const flowLinks: [number, string][] = [];
      network.linkId.forEach((linkId, linkRow) => {
        const isFlow = network.linkType[linkRow] === "flow";
        if (network.toNodeId[linkRow] === selection.id) {
          incoming.push(this.linkLink(linkRow, "from"));
          if (isFlow) flowLinks.push([linkId, `in #${linkId}`]);
        }
        if (network.fromNodeId[linkRow] === selection.id) {
          outgoing.push(this.linkLink(linkRow, "to"));
          if (isFlow) flowLinks.push([linkId, `out #${linkId}`]);
        }
      });
      if (incoming.length) sections.push(el("h3", {}, "Incoming links"), el("ul", {}, ...incoming));
      if (outgoing.length) sections.push(el("h3", {}, "Outgoing links"), el("ul", {}, ...outgoing));

      const results = manifest.results;
      if (results && nodeType === "Basin") {
        sections.push(
          lazySection(
            "Results",
            async () => charts(await readRowsById(results.basin.by_id, "node_id", selection.id), width, BASIN_CHARTS),
            true,
          ),
        );
      } else if (results && flowLinks.length) {
        sections.push(lazySection("Flow results", () => this.linkFlows(flowLinks, width), true));
      }

      const tables = manifest.tables.filter((table) => table.node_type === nodeType);
      if (tables.length) {
        sections.push(el("h3", {}, "Tables"), ...tables.map((table) => tableSection(table, selection.id, width)));
      }
      this.render(sections);
      void this.fillAttributes(token, attributes, readRow(manifest.files.nodes, row));
    } else {
      const row = network.linkRow.get(selection.id);
      if (row === undefined) return this.hide();
      sections.push(
        this.header(`${network.linkType[row]} link #${selection.id}`, selection),
        el("p", {}, "From ", this.nodeLink(network.fromNodeId[row]), " to ", this.nodeLink(network.toNodeId[row])),
        attributes,
      );
      const results = manifest.results;
      if (results && network.linkType[row] === "flow") {
        sections.push(
          lazySection(
            "Results",
            async () =>
              charts(await readRowsById(results.flow.by_id, "link_id", selection.id), width, [["Flow", ["flow_rate"]]]),
            true,
          ),
        );
      }
      this.render(sections);
      void this.fillAttributes(token, attributes, readRow(manifest.files.links, row));
    }
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
    return chart ? [el("h4", {}, "Flow rate (m3 s-1)"), chart, ...note] : ["No results for these links."];
  }

  private render(sections: HTMLElement[]): void {
    this.element.replaceChildren(...sections);
    this.element.hidden = false;
    this.element.scrollTop = 0;
  }

  private async fillAttributes(token: number, container: HTMLElement, row: Promise<Row>): Promise<void> {
    try {
      const attributes = attributeTable(await row);
      if (token === this.token) container.replaceChildren(attributes);
    } catch (error) {
      if (token === this.token) container.replaceChildren(el("p", { className: "error" }, `Failed to load: ${error}`));
    }
  }

  hide(): void {
    this.token++;
    this.element.hidden = true;
    this.element.replaceChildren();
  }
}
