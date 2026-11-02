import { ColorScale } from "./colors";
import type { ResultVariable, Results } from "./data";
import type { FrameKind } from "./results";

/** Flow is shown as a color ramp, or as blue links whose width grows with the magnitude */
export type FlowStyle = "color" | "width";
export type Kind = "basin" | "flow" | "observation";

export interface TimeState {
  step: number;
  basin: string | null;
  /** The substance to color the Basins by, if `basin` is `CONCENTRATION` */
  substance: string | null;
  flow: string | null;
  flowStyle: FlowStyle;
  /** The key of an `ObservationChoice` */
  observation: string | null;
}

/** Variables to choose from, under a label if a kind has several groups. */
export interface VariableGroup {
  label: string;
  variables: Record<string, ResultVariable>;
}

const SELECTOR_LABELS: Record<Kind, string> = { flow: "Links", basin: "Basins", observation: "Observations" };

/** The choice of Basin variable that colors the Basins by the concentration of a substance, chosen separately. */
const CONCENTRATION = "concentration";

const FRAME_INTERVAL_MS = 150;
// Default limits that replace those of the export, by "<kind>/<variable>"; the exported upper limit of the
// flow rate is the 98th percentile, which makes all larger flows look the same
const DEFAULT_LIMITS: Record<string, { lower?: number; upper?: number }> = { "flow/flow_rate": { upper: 100 } };

function el<K extends keyof HTMLElementTagNameMap>(tag: K, className = "", text = ""): HTMLElementTagNameMap[K] {
  const element = document.createElement(tag);
  element.className = className;
  element.textContent = text;
  return element;
}

/**
 * Add the substances to a list, under their group headings. A long list stays usable under those, and by typing the
 * first letters of a substance.
 */
export function addSubstances(select: HTMLSelectElement, substances: Record<string, ResultVariable>): void {
  const groups = new Map<string, HTMLOptGroupElement>();
  for (const [substance, { label, group = "" }] of Object.entries(substances)) {
    let optgroup = groups.get(group);
    if (!optgroup) {
      optgroup = el("optgroup");
      optgroup.label = group;
      groups.set(group, optgroup);
      select.append(optgroup);
    }
    optgroup.append(new Option(label, substance));
  }
}

/** Time slider, play button and result variable selection, with legends whose limits can be edited. */
export class TimeBar {
  readonly state: TimeState;
  private playing = false;
  private readonly slider: HTMLInputElement;
  private readonly date: HTMLSpanElement;
  private readonly play: HTMLButtonElement;
  private readonly legends = {} as Record<Kind, HTMLDivElement>;
  /** Color scale or width limits set by the user, by "<kind>/<variable>" */
  private readonly limits = new Map<string, [number, number]>();
  /** The variables to choose from, per kind */
  private readonly groups: Record<Kind, VariableGroup[]>;
  /** The substances to choose from, in a second list since there can be many */
  private readonly substances: Record<string, ResultVariable>;
  private substanceSelect: HTMLSelectElement | null = null;

  /** With a selection of each of `kinds`; the observation variables are in `observations`. */
  constructor(
    parent: HTMLElement,
    results: Results,
    private readonly times: Date[],
    initial: Partial<TimeState>,
    private readonly onChange: () => Promise<void>,
    kinds: Kind[],
    observations: VariableGroup[] = [],
  ) {
    this.groups = {
      flow: [{ label: "", variables: results.flow.variables }],
      basin: [{ label: "", variables: results.basin.variables }],
      observation: observations,
    };
    this.substances = results.concentration?.variables ?? {};
    const flow = kinds.includes("flow") ? "flow_rate" : null;
    const substance = Object.keys(this.substances)[0] ?? null;
    this.state = { step: 0, basin: null, substance, flow, flowStyle: "width", observation: null, ...initial };
    const bar = el("div", "timebar");
    this.play = el("button", "play", "▶");
    this.play.type = "button";
    this.play.title = "Play";
    this.play.addEventListener("click", () => this.togglePlay());
    this.slider = el("input");
    this.slider.type = "range";
    this.slider.min = "0";
    this.slider.max = String(times.length - 1);
    this.slider.value = String(this.state.step);
    this.slider.addEventListener("input", () => this.setStep(Number(this.slider.value)));
    this.date = el("span", "date");
    const time = el("div", "time");
    time.append(this.play, this.slider, this.date);
    const variables = el("div", "variables");
    variables.append(...kinds.map((kind) => this.selector(kind)));
    bar.append(time, variables);
    parent.append(bar);
    this.update();
    for (const kind of kinds) this.renderLegend(kind);
  }

  /** Which values to show of a kind: those of its own result set, or the concentration of the chosen substance. */
  frames(kind: Kind): { kind: FrameKind; variable: string } | null {
    const name = this.state[kind];
    if (!name) return null;
    if (kind === "basin" && name === CONCENTRATION) return { kind: "concentration", variable: this.state.substance! };
    return { kind, variable: name };
  }

  /** The selected variable of a kind, with the limits set by the user. */
  variable(kind: Kind): ResultVariable | null {
    const frames = this.frames(kind);
    if (!frames) return null;
    const { kind: set, variable: name } = frames;
    const variable =
      set === "concentration"
        ? this.substances[name]
        : this.groups[kind].find((group) => name in group.variables)?.variables[name];
    if (!variable) return null;
    const key = `${set}/${name}`;
    const defaults = DEFAULT_LIMITS[key];
    const domain = this.limits.get(key) ?? [
      defaults?.lower ?? variable.domain[0],
      defaults?.upper ?? variable.domain[1],
    ];
    return { ...variable, domain };
  }

  /**
   * Variable selection with its legend below; flow variables can be shown by color or by width, and the variables
   * of a kind with several groups are grouped under their label.
   */
  private selector(kind: Kind): HTMLDivElement {
    const select = el("select");
    select.append(new Option("none", ""));
    const groups = this.groups[kind];
    for (const group of groups) {
      const options = Object.entries(group.variables).flatMap(([variable, { label }]) =>
        kind === "flow"
          ? [new Option(`${label} (color)`, `${variable}:color`), new Option(`${label} (width)`, `${variable}:width`)]
          : // The label of the only group is in each option
            [new Option(groups.length === 1 && group.label ? `${group.label} ${label}` : label, variable)],
      );
      if (groups.length === 1) {
        select.append(...options);
        continue;
      }
      const optgroup = el("optgroup");
      optgroup.label = group.label;
      optgroup.append(...options);
      select.append(optgroup);
    }
    const { flow, flowStyle } = this.state;
    const substances = kind === "basin" && Object.keys(this.substances).length > 0 ? this.substanceSelector() : null;
    if (substances) select.append(new Option(CONCENTRATION, CONCENTRATION));
    select.value = kind === "flow" ? (flow ? `${flow}:${flowStyle}` : "") : (this.state[kind] ?? "");
    select.addEventListener("change", () => {
      if (substances) substances.hidden = select.value !== CONCENTRATION;
      if (kind === "flow") {
        const [variable, style] = select.value.split(":");
        this.state.flow = variable || null;
        if (style) this.state.flowStyle = style as FlowStyle;
      } else {
        this.state[kind] = select.value || null;
      }
      this.renderLegend(kind);
      void this.onChange();
    });
    const wrapper = el("label", "", SELECTOR_LABELS[kind]);
    wrapper.append(select);
    this.legends[kind] = el("div", "legend");
    const column = el("div", "variable");
    column.append(wrapper, ...(substances ? [substances] : []), this.legends[kind]);
    return column;
  }

  /** The list of substances, shown when the Basins are colored by concentration. */
  private substanceSelector(): HTMLLabelElement {
    const select = el("select");
    addSubstances(select, this.substances);
    select.value = this.state.substance ?? "";
    select.addEventListener("change", () => this.setSubstance(select.value));
    this.substanceSelect = select;
    const wrapper = el("label", "", "Substance");
    wrapper.hidden = this.state.basin !== CONCENTRATION;
    wrapper.append(select);
    return wrapper;
  }

  /** Choose a substance, also from elsewhere, which only changes the map if that shows the concentrations. */
  setSubstance(substance: string): void {
    this.state.substance = substance;
    if (this.substanceSelect) this.substanceSelect.value = substance;
    if (this.state.basin !== CONCENTRATION) return;
    this.renderLegend("basin");
    void this.onChange();
  }

  /** The key of the limits of the selected variable of a kind in `limits`. */
  private limitKey(kind: Kind): string {
    const { kind: set, variable } = this.frames(kind)!;
    return `${set}/${variable}`;
  }

  /** The color ramp or width wedge, with editable lower and upper limits. */
  private renderLegend(kind: Kind): void {
    const legend = this.legends[kind];
    const variable = this.variable(kind);
    legend.hidden = !variable;
    if (!variable) return;
    const byWidth = kind === "flow" && this.state.flowStyle === "width";
    const bar = el("div", byWidth ? "wedge" : "gradient");
    if (!byWidth) bar.style.background = new ColorScale(variable).gradient();
    const key = this.limitKey(kind);
    const limit = (i: 0 | 1) => {
      const input = el("input", "limit");
      input.type = "number";
      input.step = "any";
      input.value = String(Number(variable.domain[i].toPrecision(3)));
      input.title = i === 0 ? "Lower limit" : "Upper limit";
      input.addEventListener("change", () => this.setLimit(kind, i, Number(input.value), input));
      return input;
    };
    const units = el("span", "units", variable.units);
    if (variable.scale === "log") units.title = "Colored by the absolute value, on a log scale";
    if (variable.scale === "symlog") units.title = "Colored by the sign, and by the absolute value on a log scale";
    const labels = el("div", "legend-labels");
    labels.append(limit(0), units);
    if (this.limits.has(key)) {
      const reset = el("button", "reset", "reset");
      reset.type = "button";
      reset.title = "Reset the limits";
      reset.addEventListener("click", () => {
        this.limits.delete(key);
        this.renderLegend(kind);
        void this.onChange();
      });
      labels.append(reset);
    }
    labels.append(limit(1));
    legend.replaceChildren(bar, labels);
  }

  private setLimit(kind: Kind, i: 0 | 1, value: number, input: HTMLInputElement): void {
    const variable = this.variable(kind);
    if (!variable) return;
    const domain: [number, number] = [...variable.domain];
    domain[i] = value;
    const valid =
      Number.isFinite(value) &&
      domain[0] < domain[1] &&
      (variable.scale !== "log" || domain[0] > 0) &&
      (variable.scale !== "symlog" || (domain[0] < 0 && domain[1] > 0));
    input.classList.toggle("invalid", !valid);
    if (!valid) return;
    this.limits.set(this.limitKey(kind), domain);
    this.renderLegend(kind);
    void this.onChange();
  }

  private update(): void {
    this.slider.value = String(this.state.step);
    this.date.textContent = this.times[this.state.step].toISOString().slice(0, 10);
  }

  async setStep(step: number): Promise<void> {
    this.state.step = Math.min(Math.max(step, 0), this.times.length - 1);
    this.update();
    await this.onChange();
  }

  private async togglePlay(): Promise<void> {
    this.playing = !this.playing;
    this.play.textContent = this.playing ? "⏸" : "▶";
    this.play.title = this.playing ? "Pause" : "Play";
    if (this.playing && this.state.step === this.times.length - 1) await this.setStep(0);
    while (this.playing) {
      const started = performance.now();
      if (this.state.step >= this.times.length - 1) {
        void this.togglePlay();
        break;
      }
      await this.setStep(this.state.step + 1);
      await new Promise((resolve) => setTimeout(resolve, Math.max(0, FRAME_INTERVAL_MS - (performance.now() - started))));
    }
  }
}
