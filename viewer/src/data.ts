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
  scale: "linear" | "log" | "diverging";
  domain: [number, number];
  /** Derived variables are differences from the first time step of this variable */
  source?: string;
}

export interface ResultSet {
  id: "node_id" | "link_id";
  count: number;
  variables: Record<string, ResultVariable>;
  /** Sorted by id, for time series of one feature */
  by_id: FileEntry;
  /** Ordered by time step then id, for map frames */
  by_time: FileEntry;
}

export interface Results {
  times: string[];
  steps_per_row_group: number;
  basin: ResultSet;
  flow: ResultSet;
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

const DEFAULT_DATA_URL = "https://s3.deltares.nl/ribasim-nl/doc-image/webmap/lhm_coupled/";
const TRUSTED_DATA_ORIGINS = new Set([new URL(DEFAULT_DATA_URL).origin, location.origin]);
const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);
// Manifest paths are relative paths of the export, hashes are hex digests
const SAFE_PATH = /^[\w-]+(\/[\w-]+)*\.\w+$/;
const SAFE_HASH = /^[0-9a-f]+$/;

/** A data location from the URL; it may only point to the default host, this site or localhost. */
function locationParam(name: "data" | "base" | "diff"): URL | null {
  const requested = new URLSearchParams(location.search).get(name);
  if (requested === null) return null;
  const url = new URL(requested, location.href);
  const trusted = TRUSTED_DATA_ORIGINS.has(url.origin) || LOCAL_HOSTS.has(url.hostname);
  if (!trusted || !["http:", "https:"].includes(url.protocol)) {
    throw new Error(`Untrusted data location: ${url.origin}`);
  }
  return url;
}

/** The location of the model to show, `?data=<url>`. */
const dataUrl = () => locationParam("data") ?? new URL(import.meta.env.DEV ? "/" : DEFAULT_DATA_URL, location.href);

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
    if (results.control) entries.push(results.control);
  }
  return entries;
}

async function fetchJson<T>(url: URL): Promise<T> {
  const response = await fetch(url, { cache: "no-cache" });
  if (!response.ok) throw new Error(`Failed to load ${url}: HTTP ${response.status}`);
  return response.json();
}

export async function loadManifest(base: URL = dataUrl()): Promise<Manifest> {
  const manifest = await fetchJson<Manifest>(new URL("manifest.json", base));
  for (const entry of fileEntries(manifest)) entry.url = resolveFile(base, entry);
  for (const set of manifest.results ? [manifest.results.basin, manifest.results.flow] : []) {
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

/** The model to compare with, if `?base=<url>&diff=<url>` are given. */
export async function loadComparison(): Promise<Comparison | null> {
  const base = locationParam("base");
  const diff = locationParam("diff");
  if (!base && !diff) return null;
  if (!base || !diff) throw new Error("Comparing models needs both ?base= and ?diff=");
  type RawDiff = Omit<ModelDiff, "nodes" | "links" | "results"> &
    Record<"nodes" | "links", Record<string, FeatureDiff>> & { results: Record<ResultKind, RawResultDiff> | null };
  const [manifest, raw] = await Promise.all([loadManifest(base), fetchJson<RawDiff>(new URL("diff.json", diff))]);
  const results = raw.results && { basin: resultDiff(raw.results.basin), flow: resultDiff(raw.results.flow) };
  return { base: manifest, diff: { ...raw, nodes: featureDiffs(raw.nodes), links: featureDiffs(raw.links), results } };
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
