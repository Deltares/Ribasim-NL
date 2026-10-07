import { ColorScale, formatNumber, GREYS } from "./colors";
import { DIFF_STATUSES, type DiffStatus, type FeatureDiff, type ModelDiff, type ResultDiff, type ResultKind } from "./data";
import { linkLabel, type Group, type Network } from "./network";
import type { Selection } from "./panel";

export type Rgba = [number, number, number, number];

/**
 * Colorblind-safe colors of the Okabe-Ito palette. Changed is reddish purple rather than sky blue, which would
 * be lost among the water on the base map and the blue Basins and flow links.
 */
export const DIFF_COLORS: Record<DiffStatus, Rgba> = {
  added: [0, 158, 115, 255],
  removed: [213, 94, 0, 255],
  changed: [204, 121, 167, 255],
};
export const DIFF_CSS: Record<DiffStatus, string> = Object.fromEntries(
  DIFF_STATUSES.map((status) => [status, `rgb(${DIFF_COLORS[status].slice(0, 3).join(",")})`]),
) as Record<DiffStatus, string>;
export const CONTEXT_COLOR: Rgba = [140, 140, 140, 120];
// Features listed in the summary per status, more would make the list slow and useless
const MAX_LISTED = 500;

function el<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  props: Partial<HTMLElementTagNameMap[K]> = {},
  ...children: (Node | string)[]
): HTMLElementTagNameMap[K] {
  const element = Object.assign(document.createElement(tag), props);
  element.append(...children);
  return element;
}

/** Which differences are shown on the map, set with the checkboxes of the Differences box. */
export class DiffFilter {
  readonly hiddenStatuses = new Set<DiffStatus>();
  /** Changes like "Basin / state" or "geometry" */
  readonly hiddenChanges = new Set<string>();
  hideResults = false;
  /** Increased on every change, to update the map */
  version = 0;

  /** Whether a feature is shown: by its status and what changed, or by a difference in its results. */
  shows(diff: FeatureDiff | undefined, resultDiffers: boolean): boolean {
    const byDiff =
      diff !== undefined &&
      !this.hiddenStatuses.has(diff.status) &&
      (diff.status !== "changed" || diff.changes.some((change) => !this.hiddenChanges.has(change)));
    return byDiff || (resultDiffers && !this.hideResults);
  }
}

/** Result differences of one kind with the color scale of their size. */
export class ResultColors {
  readonly scale: ColorScale;

  constructor(readonly diff: ResultDiff) {
    let [low, high] = [Infinity, 0];
    for (const value of diff.differences.values()) {
      const magnitude = Math.abs(value ?? 0);
      if (magnitude > 0) [low, high] = [Math.min(low, magnitude), Math.max(high, magnitude)];
    }
    if (!(high > 0)) [low, high] = [diff.atol || 1, 10 * (diff.atol || 1)];
    if (low === high) low = high / 10;
    this.scale = new ColorScale({ label: diff.variable, units: diff.units, scale: "log", domain: [low, high] }, GREYS);
  }

  /** Write the color of a feature with a result difference, returns false if it has none. */
  write(id: number, target: Uint8Array, offset: number): boolean {
    if (!this.diff.differences.has(id)) return false;
    // A value missing in one model is a difference of unknown size, drawn as the largest
    const value = this.diff.differences.get(id) ?? this.scale.variable.domain[1];
    return this.scale.write(value, target, offset);
  }

  css(id: number): string | null {
    const rgba = new Uint8Array(4);
    return this.write(id, rgba, 0) ? `rgb(${rgba[0]},${rgba[1]},${rgba[2]})` : null;
  }
}

/** A signed result difference with units, or a note that a value is missing. */
function formatDifference(value: number | null, units: string): string {
  return value === null ? "missing in one model" : `${value > 0 ? "+" : ""}${formatNumber(value)} ${units}`;
}

/** A short description of the result difference of a feature, or null if it has none. */
export function describeResult(diff: ResultDiff | undefined, id: number): string | null {
  if (!diff?.differences.has(id)) return null;
  const value = diff.differences.get(id) ?? null;
  return `${diff.variable} ${value === null ? "" : "differs up to "}${formatDifference(value, diff.units)}`;
}

/**
 * The status color of each feature in a group, as RGBA bytes. Features whose only change is in their results
 * have the color of the size of the difference.
 */
export function statusColors(
  group: Group,
  ids: Int32Array,
  diffs: Map<number, FeatureDiff>,
  results?: ResultColors,
): Uint8Array {
  const colors = new Uint8Array(group.rows.length * 4);
  group.rows.forEach((row, i) => {
    const status = diffs.get(ids[row])?.status;
    if (status) colors.set(DIFF_COLORS[status], i * 4);
    else if (!results?.write(ids[row], colors, i * 4)) colors.set(CONTEXT_COLOR, i * 4);
  });
  return colors;
}

/** A short description of what differs, for tooltips. */
export function describeDiff(diff: FeatureDiff, max = 4): string {
  if (diff.status !== "changed") return diff.status;
  const shown = diff.changes.slice(0, max).join(", ");
  return `changed: ${shown}${diff.changes.length > max ? `, … (${diff.changes.length} in total)` : ""}`;
}

/** A dot in the color of a status. */
export function statusDot(status: DiffStatus): HTMLElement {
  const dot = el("span", { className: "dot" });
  dot.style.background = DIFF_CSS[status];
  return dot;
}

/** The status of a feature as a colored dot and its name, for headings. */
export function statusLabel(status: DiffStatus): HTMLElement {
  return el("span", { className: "diff-status" }, statusDot(status), status);
}

function counts(diffs: Map<number, FeatureDiff>): Record<DiffStatus, number> {
  const result = { added: 0, removed: 0, changed: 0 };
  for (const { status } of diffs.values()) result[status]++;
  return result;
}

/** The number of changed nodes and links per change, most frequent first. */
function changeCounts(diff: ModelDiff): [string, number][] {
  const result = new Map<string, number>();
  for (const diffs of [diff.nodes, diff.links]) {
    for (const { changes } of diffs.values()) {
      for (const change of changes) result.set(change, (result.get(change) ?? 0) + 1);
    }
  }
  return [...result].sort(([a, m], [b, n]) => n - m || a.localeCompare(b));
}

/** A checkbox that shows or hides a kind of difference on the map. */
function filterBox(checked: boolean, title: string, onChange: (checked: boolean) => void): HTMLInputElement {
  const input = el("input", { type: "checkbox", checked, title });
  input.addEventListener("change", () => onChange(input.checked));
  return input;
}

/** A list of differing features of one kind, by status, that selects a feature when clicked. */
function featureList(
  kind: "node" | "link",
  diffs: Map<number, FeatureDiff>,
  network: Network,
  go: (selection: Selection) => void,
): HTMLElement[] {
  return DIFF_STATUSES.flatMap((status) => {
    const ids = [...diffs].filter(([, diff]) => diff.status === status).map(([id]) => id);
    if (ids.length === 0) return [];
    const list = el("ul", { className: "diff-list" });
    for (const id of ids.slice(0, MAX_LISTED)) {
      const row = kind === "node" ? network.nodeRow.get(id) : network.linkRow.get(id);
      const label =
        row === undefined
          ? `#${id}`
          : kind === "node"
            ? `${network.nodeType[row]} #${id}`
            : `${network.linkType[row]} link ${linkLabel(id)}`;
      const link = el("a", { href: "#" }, label);
      link.addEventListener("click", (event) => {
        event.preventDefault();
        go({ kind, id });
      });
      const changes = diffs.get(id)!.changes;
      list.append(el("li", { title: changes.join("\n") }, link, changes.length ? ` ${changes.join(", ")}` : ""));
    }
    const note = ids.length > MAX_LISTED ? [el("p", { className: "note" }, `Showing ${MAX_LISTED} of ${ids.length}.`)] : [];
    const title = `${status[0].toUpperCase()}${status.slice(1)} ${kind}s (${ids.length.toLocaleString()})`;
    return [el("details", {}, el("summary", {}, statusDot(status), title), list, ...note)];
  });
}

const RESULT_TITLES: Record<ResultKind, string> = { basin: "Basin levels", flow: "Flow rates" };

/** The features whose results differ, largest first, with the legend of their colors. */
function resultList(kind: ResultKind, results: ResultColors, go: (selection: Selection) => void): HTMLElement {
  const { diff, scale } = results;
  const magnitude = (value: number | null) => (value === null ? Infinity : Math.abs(value));
  const ids = [...diff.differences].sort(([, a], [, b]) => magnitude(b) - magnitude(a)).map(([id]) => id);
  const list = el("ul", { className: "diff-list" });
  for (const id of ids.slice(0, MAX_LISTED)) {
    const selection: Selection = kind === "basin" ? { kind: "node", id } : { kind: "link", id };
    const link = el("a", { href: "#" }, kind === "basin" ? `Basin #${id}` : `flow link ${linkLabel(id)}`);
    link.addEventListener("click", (event) => {
      event.preventDefault();
      go(selection);
    });
    const swatch = el("span", { className: "swatch" });
    swatch.style.background = results.css(id) ?? "";
    list.append(el("li", {}, swatch, link, ` ${formatDifference(diff.differences.get(id) ?? null, diff.units)}`));
  }
  const notes = ids.length > MAX_LISTED ? [el("p", { className: "note" }, `Showing ${MAX_LISTED} of ${ids.length}.`)] : [];
  const [low, high] = scale.variable.domain;
  const gradient = el("div", { className: "gradient" });
  gradient.style.background = scale.gradient();
  const legend = el(
    "div",
    { className: "legend" },
    gradient,
    el("div", { className: "legend-labels" }, formatNumber(low), `|Δ ${diff.variable}| ${diff.units}`, formatNumber(high)),
  );
  const relative = diff.rtol ? ` plus ${100 * diff.rtol}% of the largest magnitude` : "";
  const tolerance = `Differences up to ${diff.atol} ${diff.units}${relative} are ignored`;
  const title = `${RESULT_TITLES[kind]} (${ids.length.toLocaleString()} of ${diff.compared.toLocaleString()} differ)`;
  return el("details", {}, el("summary", { title: tolerance }, title), ...(ids.length ? [legend, list] : []), ...notes);
}

/** The compared models: the model name once if both have it, and their labels. */
function modelLine(diff: ModelDiff): HTMLElement {
  const { base, head } = diff;
  const line = el("p", { className: "models" });
  if (base.model === head.model && base.label !== base.model && head.label !== head.model) {
    line.append(el("strong", {}, head.model), " ");
  }
  const label = (model: ModelDiff["base"]) => el("span", { title: model.toml }, model.label);
  line.append(label(base), " → ", label(head));
  return line;
}

/**
 * A box with the compared models and the number of differences by status and by what changed, with checkboxes
 * to filter the map, and lists of the differing settings, features and results.
 */
export function diffSummary(
  diff: ModelDiff,
  results: Record<ResultKind, ResultColors> | null,
  network: Network,
  filter: DiffFilter,
  onFilter: () => void,
  go: (selection: Selection) => void,
): HTMLElement {
  const details = el("details", { className: "diff-summary" });
  details.open = !matchMedia("(max-width: 600px)").matches;
  const update = () => {
    filter.version++;
    onFilter();
  };
  const nodes = counts(diff.nodes);
  const links = counts(diff.links);
  const table = el(
    "table",
    { className: "diff-counts" },
    el("tr", {}, el("th"), el("th", {}, "Nodes"), el("th", {}, "Links")),
    ...DIFF_STATUSES.map((status) => {
      const box = filterBox(true, `Show the ${status} features`, (checked) => {
        if (checked) filter.hiddenStatuses.delete(status);
        else filter.hiddenStatuses.add(status);
        update();
      });
      return el(
        "tr",
        {},
        el("th", {}, el("label", {}, box, statusDot(status), status)),
        el("td", {}, nodes[status].toLocaleString()),
        el("td", {}, links[status].toLocaleString()),
      );
    }),
  );
  details.append(el("summary", {}, "Differences"), modelLine(diff), table);

  const changes = changeCounts(diff);
  if (changes.length) {
    const list = el(
      "ul",
      { className: "diff-changes" },
      ...changes.map(([change, count]) => {
        const box = filterBox(true, `Show the features with a changed ${change}`, (checked) => {
          if (checked) filter.hiddenChanges.delete(change);
          else filter.hiddenChanges.add(change);
          update();
        });
        return el("li", {}, el("label", {}, box, el("span", { className: "name" }, change), el("span", { className: "count" }, count.toLocaleString())));
      }),
    );
    const section = el("details", { className: "diff-changes-section", open: true }, el("summary", {}, `What changed (${changes.length})`), list);
    details.append(section);
  }
  if (diff.config.length) {
    const list = el(
      "ul",
      { className: "diff-list" },
      ...diff.config.map(({ key, base, head }) => el("li", {}, el("code", {}, key), `: ${base ?? "unset"} → ${head ?? "unset"}`)),
    );
    details.append(el("details", {}, el("summary", {}, `Settings (${diff.config.length})`), list));
  }
  const features = [...featureList("node", diff.nodes, network, go), ...featureList("link", diff.links, network, go)];
  if (features.length) details.append(el("h4", {}, "Features"), ...features);
  else details.append(el("p", { className: "note" }, "No differences in the network."));

  if (results) {
    const box = filterBox(true, "Show the features whose results differ", (checked) => {
      filter.hideResults = !checked;
      update();
    });
    details.append(
      el("h4", {}, el("label", {}, box, "Results")),
      resultList("basin", results.basin, go),
      resultList("flow", results.flow, go),
    );
  } else if (diff.results_note) {
    details.append(el("h4", {}, "Results"), el("p", { className: "note" }, diff.results_note));
  }
  return details;
}
