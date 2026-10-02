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
  files: Record<"nodes" | "links" | "basin_area" | "waterboards", FileEntry>;
  tables: TableEntry[];
  results?: Results;
}

export type Row = Record<string, unknown>;

const DEFAULT_DATA_URL = "https://s3.deltares.nl/ribasim-nl/doc-image/webmap/lhm_coupled/";
const TRUSTED_DATA_ORIGINS = new Set([new URL(DEFAULT_DATA_URL).origin, location.origin]);
const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);
// Manifest paths are relative paths of the export, hashes are hex digests
const SAFE_PATH = /^[\w-]+(\/[\w-]+)*\.\w+$/;
const SAFE_HASH = /^[0-9a-f]+$/;

/** The data location; `?data=<url>` may only point to the default host, this site or localhost. */
function resolveDataUrl(): URL {
  const requested = new URLSearchParams(location.search).get("data");
  const url = new URL(requested ?? (import.meta.env.DEV ? "/" : DEFAULT_DATA_URL), location.href);
  const trusted = TRUSTED_DATA_ORIGINS.has(url.origin) || LOCAL_HOSTS.has(url.hostname);
  if (!trusted || !["http:", "https:"].includes(url.protocol)) {
    throw new Error(`Untrusted data location: ${url.origin}`);
  }
  return url;
}

let dataLocation: URL | undefined;
const dataUrl = () => (dataLocation ??= resolveDataUrl());

/** URL of an exported file; the content hash makes it safe to cache indefinitely. */
export function fileUrl(entry: FileEntry): string {
  if (!SAFE_PATH.test(entry.path) || !SAFE_HASH.test(entry.hash)) {
    throw new Error(`Invalid file entry in manifest: ${entry.path}`);
  }
  const base = dataUrl();
  const path = entry.path.split("/").map(encodeURIComponent).join("/");
  const url = new URL(`${path}?v=${encodeURIComponent(entry.hash)}`, base);
  if (url.origin !== base.origin || !url.pathname.startsWith(base.pathname)) {
    throw new Error(`File outside the data location: ${entry.path}`);
  }
  return url.href;
}

export async function loadManifest(): Promise<Manifest> {
  const response = await fetch(new URL("manifest.json", dataUrl()), { cache: "no-cache" });
  if (!response.ok) throw new Error(`Failed to load manifest.json: HTTP ${response.status}`);
  const manifest: Manifest = await response.json();
  for (const set of manifest.results ? [manifest.results.basin, manifest.results.flow] : []) {
    for (const variable of Object.values(set.variables)) {
      // NetCDF long names like "water flow rate" are verbose in the variable dropdowns
      variable.label = variable.label.replace(/^water /, "");
      variable.units = formatUnits(variable.units);
    }
  }
  return manifest;
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
