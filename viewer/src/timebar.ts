import { ColorScale } from "./colors";
import type { ResultVariable, Results } from "./data";

/** Flow is shown as a color ramp, or as blue links whose width grows with the magnitude */
export type FlowStyle = "color" | "width";
type Kind = "basin" | "flow";

export interface TimeState {
  step: number;
  basin: string | null;
  flow: string | null;
  flowStyle: FlowStyle;
}

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

  constructor(
    parent: HTMLElement,
    private readonly results: Results,
    private readonly times: Date[],
    initial: Partial<TimeState>,
    private readonly onChange: () => Promise<void>,
  ) {
    this.state = { step: 0, basin: null, flow: "flow_rate", flowStyle: "width", ...initial };
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
    variables.append(this.selector("Links", "flow"), this.selector("Basins", "basin"));
    bar.append(time, variables);
    parent.append(bar);
    this.update();
    this.renderLegend("flow");
    this.renderLegend("basin");
  }

  /** The selected variable of a kind, with the limits set by the user. */
  variable(kind: Kind): ResultVariable | null {
    const name = this.state[kind];
    if (!name) return null;
    const variable = this.results[kind].variables[name];
    const key = `${kind}/${name}`;
    const defaults = DEFAULT_LIMITS[key];
    const domain = this.limits.get(key) ?? [
      defaults?.lower ?? variable.domain[0],
      defaults?.upper ?? variable.domain[1],
    ];
    return { ...variable, domain };
  }

  /** Variable selection with its legend below; flow variables can be shown by color or by width. */
  private selector(label: string, kind: Kind): HTMLDivElement {
    const select = el("select");
    select.append(new Option("none", ""));
    for (const [variable, { label }] of Object.entries(this.results[kind].variables)) {
      if (kind === "basin") {
        select.append(new Option(label, variable));
        continue;
      }
      select.append(new Option(`${label} (color)`, `${variable}:color`), new Option(`${label} (width)`, `${variable}:width`));
    }
    const { flow, flowStyle } = this.state;
    select.value = kind === "basin" ? (this.state.basin ?? "") : flow ? `${flow}:${flowStyle}` : "";
    select.addEventListener("change", () => {
      if (kind === "basin") {
        this.state.basin = select.value || null;
      } else {
        const [variable, style] = select.value.split(":");
        this.state.flow = variable || null;
        if (style) this.state.flowStyle = style as FlowStyle;
      }
      this.renderLegend(kind);
      void this.onChange();
    });
    const wrapper = el("label", "", label);
    wrapper.append(select);
    this.legends[kind] = el("div", "legend");
    const column = el("div", "variable");
    column.append(wrapper, this.legends[kind]);
    return column;
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
    const key = `${kind}/${this.state[kind]}`;
    const limit = (i: 0 | 1) => {
      const input = el("input", "limit");
      input.type = "number";
      input.step = "any";
      input.value = String(Number(variable.domain[i].toPrecision(3)));
      input.title = i === 0 ? "Lower limit" : "Upper limit";
      input.addEventListener("change", () => this.setLimit(kind, i, Number(input.value), input));
      return input;
    };
    const units = el("span", "units", `${variable.scale === "log" ? "|x| " : ""}${variable.units}`);
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
    const valid = Number.isFinite(value) && domain[0] < domain[1] && (variable.scale !== "log" || domain[0] > 0);
    input.classList.toggle("invalid", !valid);
    if (!valid) return;
    this.limits.set(`${kind}/${this.state[kind]}`, domain);
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
