import { formatNumber } from "./colors";

/** A level that can change per time step, like a control limit that depends on the control state. */
export interface LevelSeries {
  label: string;
  /** Value per time step, NaN where it does not apply */
  values: Float64Array;
}

/** One side of a connector node: the node it connects to and the levels to draw there. */
export interface LevelSide {
  /** Like "Basin #12" */
  name: string;
  /** Water level per time step, or null if unknown */
  levels: Float64Array | null;
  /** Lowest level of the Basin profile */
  bottom: number | null;
  /** min_upstream_level or max_downstream_level of the current control state */
  limit: LevelSeries | null;
}

const SVG = "http://www.w3.org/2000/svg";
const HEIGHT = 230;
const MARGIN = { top: 10, right: 8, bottom: 22, left: 44 };
// Space between the two sides, where the connector is drawn
const GAP = 28;
const WATER = "#2c7fb8";
const BOTTOM = "#8c510a";
const LIMIT = "#000";

function svg<K extends keyof SVGElementTagNameMap>(
  tag: K,
  attributes: Record<string, string | number>,
  text?: string,
): SVGElementTagNameMap[K] {
  const element = document.createElementNS(SVG, tag);
  for (const [key, value] of Object.entries(attributes)) element.setAttribute(key, String(value));
  if (text !== undefined) element.textContent = text;
  return element;
}

/** Round tick values covering [low, high]. */
function ticks(low: number, high: number, count = 5): number[] {
  const raw = (high - low) / count;
  const magnitude = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 5, 10].map((f) => f * magnitude).find((s) => s >= raw) ?? raw;
  const result: number[] = [];
  for (let value = Math.ceil(low / step) * step; value <= high + step * 1e-9; value += step) {
    result.push(Number(value.toPrecision(12)));
  }
  return result;
}

/** The y range covering all levels and lines of both sides over all time steps, so the lines move as time passes. */
function domain(sides: LevelSide[]): [number, number] | null {
  let low = Infinity;
  let high = -Infinity;
  const add = (value: number) => {
    if (!Number.isFinite(value)) return;
    low = Math.min(low, value);
    high = Math.max(high, value);
  };
  for (const side of sides) {
    side.levels?.forEach(add);
    side.limit?.values.forEach(add);
    if (side.bottom !== null) add(side.bottom);
  }
  if (low > high) return null;
  const padding = Math.max((high - low) * 0.05, 0.1);
  return [low - padding, high + padding];
}

/** Show an SVG element at a level, or hide it if the level is unknown. */
function placeLine(line: SVGLineElement, y: number | null): void {
  line.style.display = y === null ? "none" : "";
  if (y === null) return;
  line.setAttribute("y1", String(y));
  line.setAttribute("y2", String(y));
}

interface DrawnSide {
  side: LevelSide;
  water: SVGRectElement;
  level: SVGLineElement;
  value: SVGTextElement;
  limit: SVGLineElement;
  limitLabel: SVGTextElement;
}

/**
 * Water levels on both sides of a connector node at one time step, with the control limits and Basin bottoms.
 * The y axis is fixed over time, so the levels move up and down when the time step changes.
 */
export class LevelDiagram {
  readonly element: HTMLElement;
  private readonly sides: DrawnSide[] = [];
  private readonly y: (value: number) => number;
  private readonly bottomY = HEIGHT - MARGIN.bottom;

  constructor(upstream: LevelSide, downstream: LevelSide, width: number) {
    const range = domain([upstream, downstream]);
    this.element = document.createElement("div");
    this.element.className = "levels";
    if (!range) {
      this.element.textContent = "No levels known on either side.";
      this.y = () => 0;
      return;
    }
    const [low, high] = range;
    this.y = (value) => MARGIN.top + ((high - value) / (high - low)) * (this.bottomY - MARGIN.top);

    const root = svg("svg", { width, height: HEIGHT, viewBox: `0 0 ${width} ${HEIGHT}` });
    const left = MARGIN.left;
    const right = width - MARGIN.right;
    const middle = (left + right) / 2;
    for (const tick of ticks(low, high)) {
      const y = this.y(tick);
      root.append(
        svg("line", { x1: left, x2: right, y1: y, y2: y, class: "grid" }),
        svg("text", { x: left - 4, y: y + 3, "text-anchor": "end", class: "tick" }, formatNumber(tick)),
      );
    }
    const axisMiddle = (MARGIN.top + this.bottomY) / 2;
    root.append(
      svg(
        "text",
        { x: 10, y: axisMiddle, class: "tick", transform: `rotate(-90 10 ${axisMiddle})`, "text-anchor": "middle" },
        "level (m)",
      ),
      // The connector between the two sides
      svg("rect", { x: middle - 3, y: MARGIN.top, width: 6, height: this.bottomY - MARGIN.top, class: "connector" }),
    );

    const halves: [LevelSide, number, number][] = [
      [upstream, left, middle - GAP / 2],
      [downstream, middle + GAP / 2, right],
    ];
    for (const [side, x1, x2] of halves) {
      const dashed = { x1, x2, "stroke-width": 1.5, "stroke-dasharray": "3 3" };
      const water = svg("rect", { x: x1, width: x2 - x1, fill: WATER, "fill-opacity": 0.15 });
      const level = svg("line", { x1, x2, stroke: WATER, "stroke-width": 2.5 });
      const limit = svg("line", { ...dashed, stroke: LIMIT });
      // Water level values on the left, line labels right-aligned below their line, so they rarely overlap
      const value = svg("text", { x: x1 + 4, class: "value" });
      const limitLabel = svg("text", { x: x2 - 2, "text-anchor": "end", fill: LIMIT }, side.limit?.label ?? "");
      root.append(water, level);
      if (side.bottom !== null) {
        const y = this.y(side.bottom);
        root.append(
          svg("line", { ...dashed, y1: y, y2: y, stroke: BOTTOM }),
          svg("text", { x: x2 - 2, y: y + 11, "text-anchor": "end", fill: BOTTOM }, "bottom"),
        );
      }
      root.append(
        limit,
        limitLabel,
        value,
        svg("text", { x: (x1 + x2) / 2, y: HEIGHT - 6, "text-anchor": "middle", class: "side-name" }, side.name),
      );
      this.sides.push({ side, water, level, value, limit, limitLabel });
    }
    this.element.append(root);
  }

  /** Move the levels to a time step. */
  update(step: number): void {
    for (const { side, water, level, value, limit, limitLabel } of this.sides) {
      const limitValue = side.limit?.values[step] ?? Number.NaN;
      const limitY = Number.isFinite(limitValue) ? this.y(limitValue) : null;
      placeLine(limit, limitY);
      limitLabel.style.display = limitY === null ? "none" : "";
      if (limitY !== null) limitLabel.setAttribute("y", String(limitY + 11));

      const levelValue = side.levels?.[step] ?? Number.NaN;
      const levelY = Number.isFinite(levelValue) ? this.y(levelValue) : null;
      placeLine(level, levelY);
      for (const element of [water, value]) element.style.display = levelY === null ? "none" : "";
      if (levelY === null) continue;
      const floor = side.bottom !== null ? Math.max(this.y(side.bottom), levelY) : this.bottomY;
      water.setAttribute("y", String(levelY));
      water.setAttribute("height", String(Math.max(floor - levelY, 0)));
      value.setAttribute("y", String(levelY - 4));
      value.textContent = `${levelValue.toFixed(2)} m`;
    }
  }
}
