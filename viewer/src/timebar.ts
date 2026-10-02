import { ColorScale, formatNumber } from "./colors";
import type { Results } from "./data";

export interface TimeState {
  step: number;
  basin: string | null;
  flow: string | null;
}

const FRAME_INTERVAL_MS = 150;

function el<K extends keyof HTMLElementTagNameMap>(tag: K, className = "", text = ""): HTMLElementTagNameMap[K] {
  const element = document.createElement(tag);
  element.className = className;
  element.textContent = text;
  return element;
}

/** Time slider, play button and result variable selection, with color legends. */
export class TimeBar {
  readonly state: TimeState;
  private playing = false;
  private readonly slider: HTMLInputElement;
  private readonly date: HTMLSpanElement;
  private readonly play: HTMLButtonElement;
  private readonly legends = {} as Record<"basin" | "flow", HTMLDivElement>;

  constructor(
    parent: HTMLElement,
    private readonly results: Results,
    private readonly times: Date[],
    initial: Partial<TimeState>,
    private readonly onChange: () => Promise<void>,
  ) {
    this.state = { step: 0, basin: null, flow: "flow_rate", ...initial };
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
    variables.append(this.variable("Links", "flow"), this.variable("Basins", "basin"));
    bar.append(time, variables);
    parent.append(bar);
    this.update();
  }

  /** Variable selection with its color legend below. */
  private variable(label: string, kind: "basin" | "flow"): HTMLDivElement {
    const select = el("select");
    select.append(new Option("none", ""));
    for (const variable of Object.keys(this.results[kind].variables)) {
      select.append(new Option(this.results[kind].variables[variable].label, variable));
    }
    select.value = this.state[kind] ?? "";
    select.addEventListener("change", () => {
      this.state[kind] = select.value || null;
      this.update();
      void this.onChange();
    });
    const wrapper = el("label", "", label);
    wrapper.append(select);
    this.legends[kind] = el("div", "legend");
    const column = el("div", "variable");
    column.append(wrapper, this.legends[kind]);
    return column;
  }

  private update(): void {
    this.slider.value = String(this.state.step);
    this.date.textContent = this.times[this.state.step].toISOString().slice(0, 10);
    for (const kind of ["flow", "basin"] as const) {
      const legend = this.legends[kind];
      const variable = this.state[kind];
      legend.hidden = !variable;
      if (!variable) continue;
      const scale = new ColorScale(this.results[kind].variables[variable]);
      const { units, domain, scale: type } = scale.variable;
      const bar = el("div", "gradient");
      bar.style.background = scale.gradient();
      const labels = el("div", "legend-labels");
      labels.append(
        el("span", "", `${type === "log" ? "|x| " : ""}${formatNumber(domain[0])}`),
        el("span", "", units),
        el("span", "", formatNumber(domain[1])),
      );
      legend.replaceChildren(bar, labels);
    }
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
