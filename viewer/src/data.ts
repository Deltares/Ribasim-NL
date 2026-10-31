import {
  asyncBufferFromUrl,
  cachedAsyncBuffer,
  parquetMetadataAsync,
  parquetRead,
  parquetReadObjects,
} from "hyparquet";
import type { AsyncBuffer, FileMetaData } from "hyparquet";
import { compressors } from "hyparquet-compressors";
import { formatUnits } from "./colors";

export interface FileEntry {
  path: string;
  hash: string;
  bytes: number;
  /** Set when the manifest is loaded, since the base model of a comparison has another location */
  url?: string;
}

export interface TableEntry extends FileEntry {
  name: string;
  node_type: string;
  rows: number;
}

export interface ResultVariable {
  label: string;
  units: string;
  /** "symlog" colors signed values by their magnitude on a log scale, see `normalize` */
  scale: "linear" | "log" | "diverging" | "symlog";
  domain: [number, number];
  /** Derived variables are differences from the first time step of this variable */
  source?: string;
  /** Colors from the lower to the upper limit, instead of those of the scale */
  palette?: string[];
}

/** Values per time step of a set of features, ordered by time step then id, for map frames. */
export interface FrameSet {
  id: "node_id" | "link_id";
  count: number;
  variables: Record<string, ResultVariable>;
  by_time: FileEntry;
}

export interface ResultSet extends FrameSet {
  /** Sorted by id, for time series of one feature */
  by_id: FileEntry;
}

/**
 * How well the results fit the measurements of the Observation nodes, see `ribasim_nl.webmap.export_observations`.
 * The frames are the differences simulated - observed of each observed variable.
 */
export interface ObservationResults extends FrameSet {
  /** The number of days with both values needed for the efficiencies and bias */
  min_days: number;
  /** The fit over the whole run: node_id, name, variable, days, nse, kge and bias */
  metrics: FileEntry;
}

export interface Results {
  times: string[];
  steps_per_row_group: number;
  basin: ResultSet;
  flow: ResultSet;
  /** Only if the model has Observation nodes with measurements */
  observation?: ObservationResults;
  /** Control state changes, sorted by control_node_id and time */
  control?: FileEntry;
}

export interface Manifest {
  model: string;
  ribasim_version: string | null;
  starttime: string;
  endtime: string;
  bounds: [number, number, number, number];
  node_types: Record<string, number>;
  files: Record<"nodes" | "links" | "basin_area", FileEntry> & { waterboards?: FileEntry };
  tables: TableEntry[];
  results?: Results;
  run?: RunInfo;
}

/** When the model was run, in local time, and from which commit, see `ribasim_nl.webmap.run_info`. */
export interface RunInfo {
  start?: string;
  end?: string;
  commit?: string;
  /** Whether the code had uncommitted changes */
  dirty?: boolean;
  /** The Ribasim core, from the `README.md` of its installation; commit and branch are GitHub URLs */
  core?: { version: string; commit: string; branch: string };
}

export type Row = Record<string, unknown>;

export type DiffStatus = "added" | "removed" | "changed";
export const DIFF_STATUSES: DiffStatus[] = ["added", "removed", "changed"];

export interface FeatureDiff {
  status: DiffStatus;
  /** Attribute columns, tables, "geometry" or "Basin / area" that differ */
  changes: string[];
  /** The id in the base model of a changed link that was renumbered */
  base_id?: number;
}

/** The features whose results differ, for one result set, see `ribasim_nl.webmap_diff.compare_results`. */
export interface ResultDiff {
  variable: string;
  units: string;
  /** Results are equal if they differ less than `atol` plus `rtol` times the largest magnitude of the feature */
  atol: number;
  rtol: number;
  /** The number of features in both models */
  compared: number;
  /** The difference head - base with the largest magnitude by head id, null if a value is missing in one model */
  differences: Map<number, number | null>;
}

export type ResultKind = "basin" | "flow";

/** The differences between two models, written by `ribasim_nl.webmap_diff`. */
export interface ModelDiff {
  base: { model: string; toml: string; label: string };
  head: { model: string; toml: string; label: string };
  config: { key: string; base: string | null; head: string | null }[];
  nodes: Map<number, FeatureDiff>;
  /** Removed links are keyed by their negated base id, which may be in use in the head model */
  links: Map<number, FeatureDiff>;
  /** Only if both models have results that are not outdated */
  results: Record<ResultKind, ResultDiff> | null;
  /** Why the results are not compared, if they are outdated */
  results_note: string | null;
}

/** The model to compare with, and the differences; the viewer shows the newer "head" model. */
export interface Comparison {
  base: Manifest;
  diff: ModelDiff;
}

/** A warning about the shown model, with a link to its latest version if it is outdated. */
export interface Notice {
  text: string;
  /** The link text, and the query string of the latest version */
  link?: { text: string; query: string };
}

/** What the viewer shows: a model, optionally compared with a base model, and a warning about it, if any. */
export interface Source {
  manifest: Manifest;
  comparison: Comparison | null;
  notice: Notice | null;
  /** The query string that shows this model again once it is outdated, pinned to its commits, if it is not already */
  pinned: string | null;
}

// The published models and their files, see `ribasim_nl.webmap_publish`
const STORE_URL = "https://s3.deltares.nl/ribasim-nl/doc-image/webmap/";
const MODEL_URL = new URL("lhm_coupled/", STORE_URL);
const FILES_URL = new URL("files/", STORE_URL);
// The model of a commit is the snapshot of the `export_webmap` output in its `dvc.lock`
const RAW_URL = "https://raw.githubusercontent.com/Deltares/Ribasim-NL/";
// The commit of main the docs are published for, or empty if the viewer was built otherwise, see vite.config.ts
declare const __MAIN_COMMIT__: string;
const MAIN = __MAIN_COMMIT__ || "main";
const COMMIT = /^[0-9a-f]{7,40}$/;
// Commits in pinned links are shortened to this length, which stays unambiguous as the repository grows
const PINNED_COMMIT_LENGTH = 10;
const EXPORT_STAGE = "export_webmap";
const EXPORT_PATH = "data/Rijkswaterstaat/webmap/lhm_coupled";
const SNAPSHOT = /^[0-9a-f]{32}$/;
const DIFF_PATH = /^diffs\/[0-9a-f]{32}-[0-9a-f]{32}\.json$/;
const TRUSTED_DATA_ORIGINS = new Set([new URL(STORE_URL).origin, location.origin]);
const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);
// Manifest paths are relative paths of the export, hashes are hex digests
const SAFE_PATH = /^[\w-]+(\/[\w-]+)*\.\w+$/;
const SAFE_HASH = /^[0-9a-f]+$/;

/** A data location from the URL; it may only point to the default host, this site or localhost. */
function locationParam(name: "data" | "base_data" | "diff"): URL | null {
  const requested = new URLSearchParams(location.search).get(name);
  if (requested === null) return null;
  const url = new URL(requested, location.href);
  const trusted = TRUSTED_DATA_ORIGINS.has(url.origin) || LOCAL_HOSTS.has(url.hostname);
  if (!trusted || !["http:", "https:"].includes(url.protocol)) {
    throw new Error(`Untrusted data location: ${url.origin}`);
  }
  return url;
}

/** The location of a local model to show, `?data=<url>`; the development server serves one at `/`. */
const dataUrl = () => locationParam("data") ?? new URL("/", location.href);

/** URL of an exported file; the content hash makes it safe to cache indefinitely. */
function resolveFile(base: URL, entry: FileEntry): string {
  if (!SAFE_PATH.test(entry.path) || !SAFE_HASH.test(entry.hash)) {
    throw new Error(`Invalid file entry in manifest: ${entry.path}`);
  }
  const path = entry.path.split("/").map(encodeURIComponent).join("/");
  const url = new URL(`${path}?v=${encodeURIComponent(entry.hash)}`, base);
  if (url.origin !== base.origin || !url.pathname.startsWith(base.pathname)) {
    throw new Error(`File outside the data location: ${entry.path}`);
  }
  return url.href;
}

export function fileUrl(entry: FileEntry): string {
  if (!entry.url) throw new Error(`File entry without URL: ${entry.path}`);
  return entry.url;
}

function fileEntries(manifest: Manifest): FileEntry[] {
  const entries: FileEntry[] = [...Object.values(manifest.files), ...manifest.tables];
  const results = manifest.results;
  if (results) {
    entries.push(results.basin.by_id, results.basin.by_time, results.flow.by_id, results.flow.by_time);
    if (results.observation) entries.push(results.observation.by_time, results.observation.metrics);
    if (results.control) entries.push(results.control);
  }
  return entries;
}

async function fetchJson<T>(url: URL): Promise<T> {
  const response = await fetch(url, { cache: "no-cache" });
  if (!response.ok) throw new Error(`Failed to load ${url}: HTTP ${response.status}`);
  return response.json();
}

/** The manifest in a directory, with file paths relative to `files`, by default that directory. */
async function loadManifest(base: URL, files: URL = base): Promise<Manifest> {
  const manifest = await fetchJson<Manifest>(new URL("manifest.json", base));
  for (const entry of fileEntries(manifest)) entry.url = resolveFile(files, entry);
  const results = manifest.results;
  const sets = results ? [results.basin, results.flow, ...(results.observation ? [results.observation] : [])] : [];
  for (const set of sets) {
    for (const variable of Object.values(set.variables)) {
      // NetCDF long names like "water flow rate" are verbose in the variable dropdowns
      variable.label = variable.label.replace(/^water /, "");
      variable.units = formatUnits(variable.units);
    }
  }
  return manifest;
}

function featureDiffs(record: Record<string, FeatureDiff>): Map<number, FeatureDiff> {
  const diffs = new Map<number, FeatureDiff>();
  for (const [id, diff] of Object.entries(record)) {
    if (!Number.isInteger(Number(id)) || !DIFF_STATUSES.includes(diff.status) || !Array.isArray(diff.changes)) {
      throw new Error(`Invalid difference for id ${id}`);
    }
    diffs.set(Number(id), diff);
  }
  return diffs;
}

type RawResultDiff = Omit<ResultDiff, "differences"> & { differences: Record<string, number | null> };

function resultDiff(raw: RawResultDiff): ResultDiff {
  const differences = new Map<number, number | null>();
  for (const [id, value] of Object.entries(raw.differences)) {
    if (!Number.isInteger(Number(id)) || !(value === null || typeof value === "number")) {
      throw new Error(`Invalid result difference for id ${id}`);
    }
    differences.set(Number(id), value);
  }
  return { ...raw, units: formatUnits(raw.units), differences };
}

async function loadDiff(url: URL): Promise<ModelDiff> {
  type RawDiff = Omit<ModelDiff, "nodes" | "links" | "results"> &
    Record<"nodes" | "links", Record<string, FeatureDiff>> & { results: Record<ResultKind, RawResultDiff> | null };
  const raw = await fetchJson<RawDiff>(url);
  const results = raw.results && { basin: resultDiff(raw.results.basin), flow: resultDiff(raw.results.flow) };
  return { ...raw, nodes: featureDiffs(raw.nodes), links: featureDiffs(raw.links), results };
}

/** The local model to compare with, if `?base_data=<url>&diff=<url>` are given. */
async function loadComparison(): Promise<Comparison | null> {
  const base = locationParam("base_data");
  const diff = locationParam("diff");
  if (!base && !diff) return null;
  if (!base || !diff) throw new Error("Comparing models needs both ?base_data= and ?diff=");
  const [manifest, modelDiff] = await Promise.all([loadManifest(base), loadDiff(new URL("diff.json", diff))]);
  return { base: manifest, diff: modelDiff };
}

function snapshot(md5: unknown): string {
  if (typeof md5 !== "string" || !SNAPSHOT.test(md5)) throw new Error(`Invalid snapshot ${md5}`);
  return md5;
}

/** A published model, named by the DVC hash of its `export_webmap` output. */
const loadSnapshot = (md5: string) => loadManifest(new URL(`snapshots/${snapshot(md5)}/`, MODEL_URL), FILES_URL);

/** The DVC hash of the `export_webmap` output in the text of a `dvc.lock`, or null if it is not there. */
export function exportHash(lock: string): string | null {
  const lines = lock.split(/\r?\n/);
  const start = lines.indexOf(`  ${EXPORT_STAGE}:`);
  if (start < 0) return null;
  let inOuts = false;
  let inExport = false;
  for (const line of lines.slice(start + 1)) {
    // The next stage
    if (/^ {0,2}\S/.test(line)) break;
    if (/^ {4}[^\s-]/.test(line)) inOuts = line === "    outs:";
    const path = /^ {4}- path: (.+)$/.exec(line);
    if (path) inExport = inOuts && path[1] === EXPORT_PATH;
    const md5 = /^ {6}md5: ([0-9a-f]{32})\.dir$/.exec(line);
    if (inExport && md5) return md5[1];
  }
  return null;
}

/** The snapshot in `dvc.lock` of main or a commit on GitHub, or null if it cannot be read from GitHub. */
async function snapshotAt(ref: string): Promise<string | null> {
  if (ref !== "main" && !COMMIT.test(ref)) throw new Error(`Invalid commit ${ref}`);
  try {
    // A commit does not change, main does
    const response = await fetch(new URL(`${ref}/dvc.lock`, RAW_URL), { cache: ref === "main" ? "no-cache" : "default" });
    return response.ok ? exportHash(await response.text()) : null;
  } catch {
    return null;
  }
}

/** The snapshot of the current main, or null if it cannot be read from GitHub. */
const currentMain = () => snapshotAt(MAIN);

/** The query string that shows the model of a commit. */
const commitQuery = (commit: string) => `commit=${commit.slice(0, PINNED_COMMIT_LENGTH)}`;

interface Published {
  snapshot: string;
  commit: string;
  published: string;
}

const shortCommit = (commit: unknown) => String(commit).slice(0, 7);

/** The model of main; the latest published main if that of the current main is not published. */
async function loadMain(): Promise<Source> {
  const md5 = await currentMain();
  if (md5) {
    try {
      const manifest = await loadSnapshot(md5);
      return { manifest, comparison: null, notice: null, pinned: MAIN === "main" ? null : commitQuery(MAIN) };
    } catch (error) {
      console.warn(`The model of main ${md5} is not published`, error);
    }
  }
  const latest = await fetchJson<Published>(new URL("main.json", MODEL_URL));
  const text = md5
    ? `The model of the latest main is not published yet, this is main at ${shortCommit(latest.commit)}.`
    : `Could not find the latest main on GitHub, this is main at ${shortCommit(latest.commit)}.`;
  // The snapshot of the latest published main is that of its commit
  const manifest = await loadSnapshot(latest.snapshot);
  return { manifest, comparison: null, notice: { text }, pinned: commitQuery(latest.commit) };
}

interface PullRequest {
  pr: number;
  head: { snapshot: string; commit: string };
  /** The pull request below this one in a stack, or main if `pr` is absent */
  base: { snapshot: string; commit: string; pr?: number };
  diff: string;
  published: string;
}

/** The snapshot the base of a pull request has now: the latest main, or the head of the pull request below it. */
async function currentBase(base: PullRequest["base"]): Promise<string | null> {
  if (base.pr === undefined) return currentMain();
  const record = await fetchJson<PullRequest>(new URL(`prs/${base.pr}.json`, MODEL_URL)).catch(() => null);
  return record?.head.snapshot ?? null;
}

/**
 * The model of a pull request, compared with the main or the pull request below it that it was published with.
 * Its links are not pinned, since it is usually published before its `dvc.lock` is committed.
 */
async function loadPullRequest(number: number, compare: boolean): Promise<Source> {
  const record = await fetchJson<PullRequest>(new URL(`prs/${number}.json`, MODEL_URL)).catch((error) => {
    throw new Error(`No model is published for pull request #${number} (${error})`);
  });
  if (!compare) return { manifest: await loadSnapshot(record.head.snapshot), comparison: null, notice: null, pinned: null };
  if (!DIFF_PATH.test(record.diff)) throw new Error(`Invalid differences ${record.diff}`);
  const [manifest, base, diff, current] = await Promise.all([
    loadSnapshot(record.head.snapshot),
    loadSnapshot(record.base.snapshot),
    loadDiff(new URL(record.diff, MODEL_URL)),
    currentBase(record.base),
  ]);
  const baseName = record.base.pr === undefined ? "main" : `pull request #${record.base.pr}`;
  const notice =
    current && current !== record.base.snapshot
      ? {
          text: `The base ${baseName} has changed since pull request #${number} was compared with it, at ${shortCommit(record.base.commit)}. Publish it again to compare with the latest ${baseName}.`,
        }
      : null;
  return { manifest, comparison: { base, diff }, notice, pinned: null };
}

/** A notice with a link to the latest main, if the model of a commit is not that of the latest main. */
async function outdatedNotice(md5: string, commit: string): Promise<Notice | null> {
  const latest = await currentMain();
  if (latest === null || latest === md5) return null;
  return {
    text: `This is the model at ${shortCommit(commit)}, not that of the latest main.`,
    link: { text: "Show the latest main", query: "" },
  };
}

/** The model of a commit, optionally compared with that of a base commit. */
async function loadCommit(commit: string, base: string | null): Promise<Source> {
  const [md5, baseMd5] = await Promise.all([snapshotAt(commit), base === null ? null : snapshotAt(base)]);
  const notFound = (name: string) =>
    new Error(`No model found for commit ${name}: it is not on GitHub, or ${EXPORT_STAGE} is not in its dvc.lock`);
  if (md5 === null) throw notFound(commit);
  const command = `pixi run publish-webmap --commit ${commit}${base === null ? "" : ` --base ${base}`}`;
  const unpublished = (what: string) => (error: unknown) => {
    throw new Error(`${what} is not published, publish it with \`${command}\` (${error})`);
  };
  let comparison: Promise<Comparison | null> = Promise.resolve(null);
  if (base !== null) {
    if (baseMd5 === null) throw notFound(base);
    if (baseMd5 === md5) throw new Error(`Commits ${base} and ${commit} have the same model`);
    comparison = Promise.all([
      loadSnapshot(baseMd5).catch(unpublished(`The model of ${base}`)),
      loadDiff(new URL(`diffs/${baseMd5}-${md5}.json`, MODEL_URL)).catch(unpublished(`The comparison of ${commit} with ${base}`)),
    ]).then(([baseManifest, diff]) => ({
      base: baseManifest,
      // The differences may have been published with labels for other commits with the same models
      diff: { ...diff, base: { ...diff.base, label: shortCommit(base) }, head: { ...diff.head, label: shortCommit(commit) } },
    }));
  }
  const [manifest, loaded, notice] = await Promise.all([
    loadSnapshot(md5).catch(unpublished(`The model of ${commit}`)),
    comparison,
    outdatedNotice(md5, commit),
  ]);
  return { manifest, comparison: loaded, notice, pinned: null };
}

// The query parameter that shows a model without its comparison
const COMPARE = "compare";

/** The query string of the shown model without its comparison, see `loadSource`. */
export function withoutComparison(search: string): string {
  const params = new URLSearchParams(search);
  params.set(COMPARE, "0");
  return params.toString();
}

/** The value of a commit in the query string, or null if it is not given. */
function commitParam(params: URLSearchParams, name: "commit" | "base"): string | null {
  const commit = params.get(name);
  if (commit !== null && !COMMIT.test(commit)) throw new Error(`Invalid commit ${name}=${commit}`);
  return commit;
}

/**
 * The model to show, by the query string of the page:
 * - by default the model of main, then pinned to `?commit=<commit>`
 * - `?commit=<commit>`: the model of a commit, compared with that of another commit with `&base=<commit>`
 * - `?pr=<number>`: the latest published model of a pull request, compared with main or the pull request below it
 * - `?data=<url>`, optionally with `&base_data=<url>&diff=<url>`: a local model
 *
 * With `&compare=0` the model is shown without its comparison, see `withoutComparison`.
 */
export async function loadSource(): Promise<Source> {
  const params = new URLSearchParams(location.search);
  const pr = params.get("pr");
  const commit = commitParam(params, "commit");
  const base = commitParam(params, "base");
  const compareParam = params.get(COMPARE);
  if (compareParam !== null && compareParam !== "0") throw new Error(`Invalid ${COMPARE}=${compareParam}, only ${COMPARE}=0 is valid`);
  const compare = compareParam === null;
  if (pr !== null) {
    if (!/^[1-9]\d{0,5}$/.test(pr)) throw new Error(`Invalid pull request number ${pr}`);
    if (commit !== null || base !== null) throw new Error("pr= cannot be combined with commit= or base=");
    return loadPullRequest(Number(pr), compare);
  }
  if (commit !== null) return loadCommit(commit, compare ? base : null);
  if (base !== null) throw new Error("Comparing with base= needs the commit= to compare");
  if (["data", "base_data", "diff"].some((name) => params.has(name)) || import.meta.env.DEV) {
    const [manifest, comparison] = await Promise.all([loadManifest(dataUrl()), compare ? loadComparison() : null]);
    return { manifest, comparison, notice: null, pinned: null };
  }
  return loadMain();
}

interface ParquetFile {
  file: AsyncBuffer;
  metadata: FileMetaData;
}

const parquetFiles = new Map<string, Promise<ParquetFile>>();
// Smaller files are fetched in one request instead of many concurrent range requests
const WHOLE_FILE_BYTES = 32e6;

async function fetchWhole(url: string): Promise<AsyncBuffer> {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch ${url}: HTTP ${response.status}`);
  const buffer = await response.arrayBuffer();
  return { byteLength: buffer.byteLength, slice: (start, end) => buffer.slice(start, end) };
}

function openParquet(entry: FileEntry): Promise<ParquetFile> {
  const url = fileUrl(entry);
  let parquet = parquetFiles.get(url);
  if (!parquet) {
    parquet = (async () => {
      const file =
        entry.bytes < WHOLE_FILE_BYTES
          ? await fetchWhole(url)
          : cachedAsyncBuffer(await asyncBufferFromUrl({ url, byteLength: entry.bytes }));
      return { file, metadata: await parquetMetadataAsync(file) };
    })();
    parquet.catch(() => parquetFiles.delete(url));
    parquetFiles.set(url, parquet);
  }
  return parquet;
}

/** Read complete columns as arrays. */
export async function readColumns<K extends string>(
  entry: FileEntry,
  columns: K[],
): Promise<Record<K, unknown[]>> {
  const { file, metadata } = await openParquet(entry);
  const rows = await new Promise<unknown[][]>((resolve, reject) => {
    parquetRead({ file, metadata, columns, compressors, onComplete: (rows) => resolve(rows as unknown[][]) }).catch(
      reject,
    );
  });
  if (rows.length !== Number(metadata.num_rows)) throw new Error(`Incomplete read of ${entry.path}`);
  const result = {} as Record<K, unknown[]>;
  columns.forEach((column, i) => {
    result[column] = rows.map((row) => row[i]);
  });
  return result;
}

export async function readRow(entry: FileEntry, row: number): Promise<Row> {
  const { file, metadata } = await openParquet(entry);
  const rows = await parquetReadObjects({ file, metadata, rowStart: row, rowEnd: row + 1, compressors });
  if (rows.length !== 1) throw new Error(`Row ${row} not found in ${entry.path}`);
  return rows[0];
}

/** Read a range of a numeric column into a typed array. */
export async function readNumbers<T extends Float32Array | Int32Array>(
  entry: FileEntry,
  column: string,
  rowStart: number,
  rowEnd: number,
  ArrayType: { new (length: number): T },
): Promise<T> {
  const { file, metadata } = await openParquet(entry);
  const values = new ArrayType(rowEnd - rowStart);
  let filled = 0;
  await parquetRead({
    file,
    metadata,
    columns: [column],
    rowStart,
    rowEnd,
    compressors,
    // Chunks may extend beyond the requested rows
    onChunk: ({ columnData, rowStart: chunkStart }) => {
      const from = Math.max(chunkStart, rowStart);
      const to = Math.min(chunkStart + columnData.length, rowEnd);
      for (let row = from; row < to; row++) values[row - rowStart] = Number(columnData[row - chunkStart]);
      filled += Math.max(0, to - from);
    },
  });
  if (filled !== values.length) throw new Error(`Incomplete read of ${column} in ${entry.path}`);
  return values;
}

/** Rows for one feature; files are sorted by the id column so statistics skip other row groups. */
export async function readRowsById(entry: FileEntry, idColumn: string, id: number): Promise<Row[]> {
  const { file, metadata } = await openParquet(entry);
  return parquetReadObjects({ file, metadata, filter: { [idColumn]: { $eq: id } }, compressors });
}
