import type { ResultVariable } from "./data";

export type RGBA = [number, number, number, number];

const VIRIDIS = ["#440154", "#482878", "#3e4989", "#31688e", "#26828e", "#1f9e89", "#35b779", "#6ece58", "#b5de2b", "#fde725"];
// Light grey to slate ink, for the size of result differences: quieter than the colors of the network changes
// that cause them
export const GREYS = ["#c9d1d6", "#a3afb7", "#7e8d97", "#5a6b77", "#3a4a55", "#1e2a32"];
// Red for falling, blue for rising values
const RED_BLUE = ["#b2182b", "#d6604d", "#f4a582", "#fddbc7", "#f7f7f7", "#d1e5f0", "#92c5de", "#4393c3", "#2166ac"];

const parseHex = (hex: string): [number, number, number] => [1, 3, 5].map((i) => Number.parseInt(hex.slice(i, i + 2), 16)) as [number, number, number];

export function palette(variable: ResultVariable): string[] {
  return variable.scale === "diverging" ? RED_BLUE : VIRIDIS;
}

/** Map a value to [0, 1] on the color scale of a variable, or NaN if it has no color. */
export function normalize(variable: ResultVariable, value: number): number {
  const [low, high] = variable.domain;
  let t: number;
  if (variable.scale === "log") {
    const magnitude = Math.abs(value);
    if (magnitude <= 0 || Number.isNaN(magnitude)) return Number.NaN;
    t = (Math.log10(magnitude) - Math.log10(low)) / (Math.log10(high) - Math.log10(low));
  } else {
    t = (value - low) / (high - low);
  }
  return Number.isFinite(t) ? Math.min(1, Math.max(0, t)) : Number.NaN;
}

export class ColorScale {
  private readonly stops: [number, number, number][];

  constructor(
    readonly variable: ResultVariable,
    private readonly colors = palette(variable),
  ) {
    this.stops = colors.map(parseHex);
  }

  /** Write the color of a value into `target` at `offset`, returns false if the value has no color. */
  write(value: number, target: Uint8Array, offset: number, alpha = 255): boolean {
    const t = normalize(this.variable, value);
    if (Number.isNaN(t)) return false;
    const position = t * (this.stops.length - 1);
    const i = Math.min(Math.floor(position), this.stops.length - 2);
    const f = position - i;
    const [a, b] = [this.stops[i], this.stops[i + 1]];
    for (let c = 0; c < 3; c++) target[offset + c] = Math.round(a[c] + (b[c] - a[c]) * f);
    target[offset + 3] = alpha;
    return true;
  }

  css(value: number): string | null {
    const rgba = new Uint8Array(4);
    return this.write(value, rgba, 0) ? `rgb(${rgba[0]},${rgba[1]},${rgba[2]})` : null;
  }

  gradient(): string {
    return `linear-gradient(to right, ${this.colors.join(", ")})`;
  }
}

const SUPERSCRIPTS: Record<string, string> = Object.fromEntries(
  [..."-0123456789"].map((char, i) => [char, "⁻⁰¹²³⁴⁵⁶⁷⁸⁹"[i]]),
);

/** Write exponents in units as superscripts, like "m3 s-1" as "m³ s⁻¹". */
export function formatUnits(units: string): string {
  return units.replace(/([a-zA-Z])(-?\d+)/g, (_, unit: string, exponent: string) =>
    unit + [...exponent].map((char) => SUPERSCRIPTS[char]).join(""),
  );
}

export function formatNumber(value: number): string {
  if (value === 0 || !Number.isFinite(value)) return String(value);
  const magnitude = Math.abs(value);
  return magnitude >= 1e4 || magnitude < 1e-2 ? value.toExponential(1) : String(Number(value.toPrecision(3)));
}

/** Tick labels with the decimals the tick spacing needs, so neighboring ticks never read the same. */
export function formatTicks(ticks: number[], increment: number): string[] {
  const largest = Math.max(0, ...ticks.map(Math.abs));
  const step = Math.floor(Math.log10(increment) + 1e-9);
  if (largest >= 1e5 || (largest > 0 && largest < 1e-3)) {
    const digits = Math.max(1, Math.floor(Math.log10(largest)) - step);
    return ticks.map((tick) => (tick === 0 ? "0" : tick.toExponential(digits)));
  }
  return ticks.map((tick) => tick.toFixed(Math.max(0, -step)));
}
