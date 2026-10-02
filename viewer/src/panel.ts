import uPlot from "uplot";
import { readNodeRows, readRow, type Manifest, type Row, type TableEntry } from "./data";
import type { Network } from "./network";

export type Selection = { kind: "node" | "link"; id: number };

const MAX_TABLE_ROWS = 2000;
const HIDDEN_COLUMNS = new Set(["x", "y", "coords"]);

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

/** A time series chart of all numeric columns, if the rows form one. */
function timeChart(rows: Row[], width: number): HTMLElement | null {
  if (rows.length < 2 || !rows.every((row) => row.time instanceof Date)) return null;
  const columns = Object.keys(rows[0]).filter(
    (column) => column !== "node_id" && rows.some((row) => typeof row[column] === "number"),
  );
  if (columns.length === 0) return null;
  const data: uPlot.AlignedData = [
    rows.map((row) => (row.time as Date).getTime() / 1000),
    ...columns.map((column) => rows.map((row) => (typeof row[column] === "number" ? (row[column] as number) : null))),
  ];
  const container = el("div", { className: "chart" });
  new uPlot(
    {
      width,
      height: 200,
      series: [{}, ...columns.map((label, i) => ({ label, stroke: SERIES_COLORS[i % SERIES_COLORS.length] }))],
      scales: { x: { time: true } },
      legend: { live: false },
    },
    data,
    container,
  );
  return container;
}

function tableSection(entry: TableEntry, nodeId: number, width: number): HTMLElement {
  const content = el("div", {}, "Loading…");
  const details = el("details", {}, el("summary", {}, entry.name), content);
  let loaded = false;
  details.addEventListener("toggle", async () => {
    if (!details.open || loaded) return;
    loaded = true;
    try {
      const rows = await readNodeRows(entry, nodeId);
      const chart = rows.length > 0 ? timeChart(rows, width) : null;
      content.replaceChildren(
        ...(rows.length === 0 ? ["No rows for this node."] : chart ? [chart, dataTable(rows)] : [dataTable(rows)]),
      );
    } catch (error) {
      content.replaceChildren(el("p", { className: "error" }, `Failed to load: ${error}`));
    }
  });
  return details;
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

  async show(selection: Selection): Promise<void> {
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
      network.linkId.forEach((_, linkRow) => {
        if (network.toNodeId[linkRow] === selection.id) incoming.push(this.linkLink(linkRow, "from"));
        if (network.fromNodeId[linkRow] === selection.id) outgoing.push(this.linkLink(linkRow, "to"));
      });
      if (incoming.length) sections.push(el("h3", {}, "Incoming links"), el("ul", {}, ...incoming));
      if (outgoing.length) sections.push(el("h3", {}, "Outgoing links"), el("ul", {}, ...outgoing));

      const tables = manifest.tables.filter((table) => table.node_type === nodeType);
      if (tables.length) {
        sections.push(el("h3", {}, "Tables"), ...tables.map((table) => tableSection(table, selection.id, width)));
      }
      this.render(sections);
      this.fillAttributes(token, attributes, readRow(manifest.files.nodes, row));
    } else {
      const row = network.linkRow.get(selection.id);
      if (row === undefined) return this.hide();
      sections.push(
        this.header(`${network.linkType[row]} link #${selection.id}`, selection),
        el("p", {}, "From ", this.nodeLink(network.fromNodeId[row]), " to ", this.nodeLink(network.toNodeId[row])),
        attributes,
      );
      this.render(sections);
      this.fillAttributes(token, attributes, readRow(manifest.files.links, row));
    }
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
