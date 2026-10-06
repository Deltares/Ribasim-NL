import { DIFF_STATUSES, type DiffStatus, type FeatureDiff, type ModelDiff } from "./data";
import { linkLabel, type Group, type Network } from "./network";
import type { Selection } from "./panel";

export type Rgba = [number, number, number, number];

/** Colorblind-safe colors of the Okabe-Ito palette, also used for the node types */
export const DIFF_COLORS: Record<DiffStatus, Rgba> = {
  added: [0, 158, 115, 255],
  removed: [213, 94, 0, 255],
  changed: [86, 180, 233, 255],
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

/** The status color of each feature in a group, as RGBA bytes. */
export function statusColors(group: Group, ids: Int32Array, diffs: Map<number, FeatureDiff>): Uint8Array {
  const colors = new Uint8Array(group.rows.length * 4);
  group.rows.forEach((row, i) => {
    const status = diffs.get(ids[row])?.status;
    colors.set(status ? DIFF_COLORS[status] : CONTEXT_COLOR, i * 4);
  });
  return colors;
}

/** A short description of what differs, for tooltips. */
export function describeDiff(diff: FeatureDiff, max = 4): string {
  if (diff.status !== "changed") return diff.status;
  const shown = diff.changes.slice(0, max).join(", ");
  return `changed: ${shown}${diff.changes.length > max ? `, … (${diff.changes.length} in total)` : ""}`;
}

export function statusBadge(status: DiffStatus): HTMLElement {
  const badge = el("span", { className: "diff-badge" }, status);
  badge.style.background = DIFF_CSS[status];
  return badge;
}

function counts(diffs: Map<number, FeatureDiff>): Record<DiffStatus, number> {
  const result = { added: 0, removed: 0, changed: 0 };
  for (const { status } of diffs.values()) result[status]++;
  return result;
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
    return [el("details", {}, el("summary", {}, statusBadge(status), " ", title), list, ...note)];
  });
}

/** A box with the compared models, the number of differences and the differing settings and features. */
export function diffSummary(diff: ModelDiff, network: Network, go: (selection: Selection) => void): HTMLElement {
  const details = el("details", { className: "diff-summary" });
  details.open = !matchMedia("(max-width: 600px)").matches;
  const nodes = counts(diff.nodes);
  const links = counts(diff.links);
  const table = el(
    "table",
    {},
    el("tr", {}, el("th"), el("th", {}, "Nodes"), el("th", {}, "Links")),
    ...DIFF_STATUSES.map((status) =>
      el(
        "tr",
        {},
        el("th", {}, statusBadge(status)),
        el("td", {}, nodes[status].toLocaleString()),
        el("td", {}, links[status].toLocaleString()),
      ),
    ),
  );
  details.append(
    el("summary", {}, "Differences"),
    el(
      "p",
      { className: "models" },
      el("span", { title: diff.base.toml }, diff.base.model),
      " → ",
      el("span", { title: diff.head.toml }, diff.head.model),
    ),
    table,
  );
  if (diff.config.length) {
    const list = el(
      "ul",
      { className: "diff-list" },
      ...diff.config.map(({ key, base, head }) => el("li", {}, el("code", {}, key), `: ${base ?? "unset"} → ${head ?? "unset"}`)),
    );
    details.append(el("details", {}, el("summary", {}, `Settings (${diff.config.length})`), list));
  }
  details.append(...featureList("node", diff.nodes, network, go), ...featureList("link", diff.links, network, go));
  if (diff.nodes.size === 0 && diff.links.size === 0) details.append(el("p", { className: "note" }, "No differences in the network."));
  return details;
}
