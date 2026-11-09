import { FIT, FIT_AROUND_ZERO, formatNumber } from "./colors";
import { readColumns, type ObservationResults, type ResultVariable } from "./data";
import type { Selection } from "./panel";

/**
 * How well the results fit the measurements of one variable of an Observation node, see
 * `ribasim_nl.goodness_of_fit.compare`. The efficiencies and bias are NaN with too few days.
 */
export interface Fit {
  name: string | null;
  /** The number of days with both a measured and a simulated value */
  days: number;
  nse: number;
  kge: number;
  /** Relative to the mean absolute measurement */
  bias: number;
}

/** The fits by observed variable, then by Observation node id. */
export type Fits = Map<string, Map<number, Fit>>;

export type Metric = "kge" | "nse" | "bias" | "difference";

/** A way to color the Observation nodes: a metric of an observed variable, or its change from the base model. */
export interface ObservationChoice {
  /** Like "flow_rate/kge", or "flow_rate/kge/change" */
  key: string;
  variable: string;
  metric: Metric;
  /** The change head - base, of the magnitude for the bias */
  change: boolean;
  /** The color scale, whose label is that of the option */
  scale: ResultVariable;
}

/** The choices of an observed variable, under its label. */
export interface ChoiceGroup {
  label: string;
  choices: ObservationChoice[];
}

// The efficiencies are colored from -1 to 1: 0 is as good as the mean of the measurements
const EFFICIENCY: [number, number] = [-1, 1];
// Green within a bias of 25%, the limit of a satisfactory bias of discharges of Moriasi et al. (2007), grey at 50%;
// most discharges are biased more, so a narrower scale would show many of them alike
const BIAS_PERCENT: [number, number] = [-100, 100];
const EFFICIENCY_CHANGE: [number, number] = [-0.25, 0.25];
const BIAS_CHANGE_PERCENT: [number, number] = [-25, 25];
const METRIC_LABELS: Record<Exclude<Metric, "difference">, string> = { kge: "KGE", nse: "NSE", bias: "bias" };

const number = (value: unknown) => (typeof value === "number" ? value : Number.NaN);
const loaded = new WeakMap<ObservationResults, Promise<Fits>>();

/** The fits of the Observation nodes of a model, loaded once. */
export function loadFits(results: ObservationResults): Promise<Fits> {
  let fits = loaded.get(results);
  if (!fits) {
    const columns = ["node_id", "name", "variable", "days", "nse", "kge", "bias"] as const;
    fits = readColumns(results.metrics, [...columns]).then((table) => {
      const byVariable: Fits = new Map();
      table.node_id.forEach((id, i) => {
        const variable = String(table.variable[i]);
        let byNode = byVariable.get(variable);
        if (!byNode) byVariable.set(variable, (byNode = new Map()));
        const name = table.name[i];
        byNode.set(Number(id), {
          // Missing values are read as null
          name: name === null || name === undefined ? null : String(name),
          days: number(table.days[i]),
          nse: number(table.nse[i]),
          kge: number(table.kge[i]),
          bias: number(table.bias[i]),
        });
      });
      return byVariable;
    });
    fits.catch(() => loaded.delete(results));
    loaded.set(results, fits);
  }
  return fits;
}

/** A metric of a fit as it is colored, with the bias in percent. */
function metricValue(fit: Fit | undefined, metric: Exclude<Metric, "difference">): number {
  if (!fit) return Number.NaN;
  return metric === "bias" ? 100 * fit.bias : fit[metric];
}

/** The fits of the Observation nodes of a model and its base model, and the ways to color them. */
export class Observations {
  readonly groups: ChoiceGroup[];
  private readonly byKey: Map<string, ObservationChoice>;

  private constructor(
    readonly results: ObservationResults,
    readonly head: Fits,
    /** Only when comparing with a base model that has them */
    readonly base: Fits | null,
  ) {
    this.groups = Object.entries(results.variables).map(([variable, difference]) => ({
      label: difference.label,
      choices: this.choices(variable, difference),
    }));
    this.byKey = new Map(this.groups.flatMap((group) => group.choices.map((choice) => [choice.key, choice])));
  }

  static async load(results: ObservationResults, base?: ObservationResults): Promise<Observations> {
    const [head, baseFits] = await Promise.all([loadFits(results), base ? loadFits(base) : null]);
    return new Observations(results, head, baseFits);
  }

  private choices(variable: string, difference: ResultVariable): ObservationChoice[] {
    const choice = (metric: Metric, change: boolean, scale: ResultVariable): ObservationChoice => ({
      key: `${variable}/${metric}${change ? "/change" : ""}`,
      variable,
      metric,
      change,
      scale,
    });
    const linear = (label: string, units: string, domain: [number, number], palette: string[]): ResultVariable => ({
      label,
      units,
      scale: "linear",
      domain,
      palette,
    });
    const choices = [
      choice("kge", false, linear("KGE", "", EFFICIENCY, FIT)),
      choice("nse", false, linear("NSE", "", EFFICIENCY, FIT)),
      choice("bias", false, linear("bias", "%", BIAS_PERCENT, FIT_AROUND_ZERO)),
      choice("difference", false, { ...difference, label: "simulated − observed" }),
    ];
    if (this.base) {
      choices.push(
        choice("kge", true, linear("KGE change", "", EFFICIENCY_CHANGE, FIT)),
        choice("nse", true, linear("NSE change", "", EFFICIENCY_CHANGE, FIT)),
        // A smaller bias is better
        choice("bias", true, linear("|bias| change", "%", BIAS_CHANGE_PERCENT, [...FIT].reverse())),
      );
    }
    return choices;
  }

  choice(key: string | null): ObservationChoice | null {
    return key === null ? null : (this.byKey.get(key) ?? null);
  }

  /** The value of a choice other than the difference for an Observation node, NaN if it has none. */
  value(choice: ObservationChoice, nodeId: number): number {
    if (choice.metric === "difference") throw new Error("The difference changes with the time step");
    const head = metricValue(this.head.get(choice.variable)?.get(nodeId), choice.metric);
    if (!choice.change) return head;
    const base = metricValue(this.base?.get(choice.variable)?.get(nodeId), choice.metric);
    return choice.metric === "bias" ? Math.abs(head) - Math.abs(base) : head - base;
  }

  /** The name of an Observation node, if it has one. */
  name(nodeId: number): string | null {
    for (const fits of this.head.values()) {
      const fit = fits.get(nodeId);
      if (fit) return fit.name;
    }
    return null;
  }

  /** A value of a choice with its units. */
  static format(choice: ObservationChoice, value: number): string {
    const { units } = choice.scale;
    const signed = choice.metric === "bias" || choice.metric === "difference" || choice.change;
    const text = `${signed && value > 0 ? "+" : ""}${formatNumber(value)}`;
    return units === "%" ? `${text}%` : units ? `${text} ${units}` : text;
  }

  /** The value of a choice for an Observation node, or why it has none. */
  describe(choice: ObservationChoice, nodeId: number, value: number): string {
    if (Number.isFinite(value)) return Observations.format(choice, value);
    if (choice.metric === "difference") return "not measured";
    const minDays = this.results.min_days;
    const fit = this.head.get(choice.variable)?.get(nodeId);
    if (!fit) return "not compared";
    if (fit.days < minDays) return `${fit.days} of the ${minDays} days needed`;
    if (choice.change) {
      const base = this.base?.get(choice.variable)?.get(nodeId);
      if (!base) return "not compared in the base model";
      if (base.days < minDays) return `${base.days} of the ${minDays} days needed in the base model`;
    }
    // Like the efficiencies of constant measurements, see `ribasim_nl.goodness_of_fit.compare`
    return "undefined for these measurements";
  }

  /** A score of a value, which is higher for a better fit, to sort by. */
  static score(choice: ObservationChoice, value: number): number {
    if (choice.metric === "kge" || choice.metric === "nse") return value;
    // A smaller bias and difference are better, also a decreasing magnitude of the bias
    return choice.change ? -value : -Math.abs(value);
  }

  /** The option label of a choice with the observed variable, like "flow rate KGE". */
  label(choice: ObservationChoice): string {
    return `${this.results.variables[choice.variable].label} ${choice.scale.label}`;
  }
}

/** An Observation node in the list, with its value of the shown choice and its color. */
export interface ListedObservation {
  id: number;
  name: string | null;
  value: number;
  color: string | null;
}

function el<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  props: Partial<HTMLElementTagNameMap[K]> = {},
  ...children: (Node | string)[]
): HTMLElementTagNameMap[K] {
  const element = Object.assign(document.createElement(tag), props);
  element.append(...children);
  return element;
}

/** A box with the Observation nodes sorted by the shown choice, best or worst first, that selects one when clicked. */
export class ObservationList {
  readonly element: HTMLDetailsElement;
  private readonly title: HTMLElement;
  private readonly summary: HTMLElement;
  private readonly order: HTMLSelectElement;
  private readonly list: HTMLUListElement;
  private shown: { choice: ObservationChoice; entries: ListedObservation[] } | null = null;

  constructor(private readonly go: (selection: Selection) => void) {
    this.title = el("summary", {}, "Observations");
    this.summary = el("p", { className: "note" });
    this.order = el("select", { title: "The order of the list" });
    this.order.append(new Option("Best fit first", "best"), new Option("Worst fit first", "worst"));
    this.order.addEventListener("change", () => this.render());
    this.list = el("ul", { className: "diff-list" });
    this.element = el("details", { className: "observation-list", hidden: true }, this.title, this.summary, this.order, this.list);
    this.element.open = !matchMedia("(max-width: 600px)").matches;
  }

  /** Show the Observation nodes with their values of a choice, or hide the box without a choice. */
  update(observations: Observations, choice: ObservationChoice | null, entries: ListedObservation[], when = ""): void {
    this.element.hidden = choice === null;
    this.shown = choice ? { choice, entries } : null;
    if (!choice) return;
    const valued = entries.filter(({ value }) => Number.isFinite(value));
    const values = valued.map(({ value }) => value).sort((a, b) => a - b);
    const middle = values.length / 2;
    const median = values.length % 2 ? values[Math.floor(middle)] : (values[middle - 1] + values[middle]) / 2;
    this.title.textContent = `Observations: ${observations.label(choice)}`;
    const [best, worst] = this.order.options;
    best.text = choice.change ? "Most improved first" : "Best fit first";
    worst.text = choice.change ? "Most worsened first" : "Worst fit first";
    this.summary.textContent = values.length
      ? `Median ${Observations.format(choice, median)} of ${values.length.toLocaleString()} Observations${when}` +
        (valued.length < entries.length ? `; ${(entries.length - valued.length).toLocaleString()} without a value.` : ".")
      : `No Observation has a value${when}.`;
    this.render();
  }

  private render(): void {
    if (!this.shown) return;
    const { choice, entries } = this.shown;
    const sign = this.order.value === "best" ? -1 : 1;
    const valued = entries.filter(({ value }) => Number.isFinite(value));
    valued.sort((a, b) => sign * (Observations.score(choice, a.value) - Observations.score(choice, b.value)));
    this.list.replaceChildren(
      ...valued.map(({ id, name, value, color }) => {
        const link = el("a", { href: "#" }, name ? `${name} #${id}` : `Observation #${id}`);
        link.addEventListener("click", (event) => {
          event.preventDefault();
          this.go({ kind: "node", id });
        });
        const swatch = el("span", { className: "swatch" });
        swatch.style.background = color ?? "";
        return el("li", { title: name ?? "" }, swatch, link, ` ${Observations.format(choice, value)}`);
      }),
    );
  }
}

/** The fit of an observed variable, compared with that of the base model when it has one, for the panel. */
export function fitFacts(head: Fit | undefined, base: Fit | undefined, minDays: number, comparing: boolean): HTMLElement {
  const facts = el("dl", { className: "facts" });
  const value = (fit: Fit | undefined, metric: Exclude<Metric, "difference">) => {
    const v = metricValue(fit, metric);
    if (!fit || !Number.isFinite(v)) return "–";
    return metric === "bias" ? `${v > 0 ? "+" : ""}${formatNumber(v)}%` : formatNumber(v);
  };
  for (const metric of ["kge", "nse", "bias"] as const) {
    const text = comparing ? `${value(base, metric)} → ${value(head, metric)}` : value(head, metric);
    facts.append(el("dt", {}, METRIC_LABELS[metric]), el("dd", {}, text));
  }
  const days = (fit: Fit | undefined) => (fit ? fit.days.toLocaleString() : "–");
  facts.append(
    el("dt", { title: `The efficiencies and bias need ${minDays} days with both values` }, "Days compared"),
    el("dd", {}, comparing ? `${days(base)} → ${days(head)}` : days(head)),
  );
  return facts;
}
