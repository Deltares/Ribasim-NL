import { readNumbers, type ResultSet, type Results } from "./data";

const MAX_CACHED_GROUPS = 24;

/** Loads per-time-step values of result variables, cached per Parquet row group. */
export class ResultFrames {
  readonly times: Date[];
  private readonly ids = new Map<"basin" | "flow", Promise<Int32Array>>();
  private readonly cache = new Map<string, Promise<Float32Array>>();

  constructor(readonly results: Results) {
    this.times = results.times.map((time) => new Date(`${time}Z`));
  }

  private set(kind: "basin" | "flow"): ResultSet {
    return this.results[kind];
  }

  /** Feature ids in the order of the values in a frame. */
  featureIds(kind: "basin" | "flow"): Promise<Int32Array> {
    let ids = this.ids.get(kind);
    if (!ids) {
      const set = this.set(kind);
      ids = readNumbers(set.by_time, set.id, 0, set.count, Int32Array);
      this.ids.set(kind, ids);
    }
    return ids;
  }

  private group(kind: "basin" | "flow", variable: string, group: number): Promise<Float32Array> {
    const key = `${kind}/${variable}/${group}`;
    let values = this.cache.get(key);
    if (values) {
      // Move to the end, so the least recently used group is evicted first
      this.cache.delete(key);
    } else {
      const set = this.set(kind);
      const steps = this.results.steps_per_row_group;
      const start = group * steps * set.count;
      const end = Math.min((group + 1) * steps, this.times.length) * set.count;
      values = readNumbers(set.by_time, variable, start, end, Float32Array);
      values.catch(() => this.cache.delete(key));
    }
    this.cache.set(key, values);
    while (this.cache.size > MAX_CACHED_GROUPS) this.cache.delete(this.cache.keys().next().value!);
    return values;
  }

  private async raw(kind: "basin" | "flow", variable: string, step: number): Promise<Float32Array> {
    const { count } = this.set(kind);
    const steps = this.results.steps_per_row_group;
    const group = Math.floor(step / steps);
    const values = await this.group(kind, variable, group);
    const offset = (step - group * steps) * count;
    return values.subarray(offset, offset + count);
  }

  /** Values of a variable at a time step, in the order of `featureIds(kind)`. */
  async frame(kind: "basin" | "flow", variable: string, step: number): Promise<Float32Array> {
    const source = this.set(kind).variables[variable].source;
    if (!source) return this.raw(kind, variable, step);
    const [values, initial] = await Promise.all([this.raw(kind, source, step), this.raw(kind, source, 0)]);
    return values.map((value, i) => value - initial[i]);
  }

  /** Start loading the row group of a later time step, to play smoothly. */
  prefetch(kind: "basin" | "flow", variable: string, step: number): void {
    const source = this.set(kind).variables[variable].source ?? variable;
    if (step < this.times.length) this.group(kind, source, Math.floor(step / this.results.steps_per_row_group));
  }
}
