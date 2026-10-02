import {
  asyncBufferFromUrl,
  cachedAsyncBuffer,
  parquetMetadataAsync,
  parquetRead,
  parquetReadObjects,
} from "hyparquet";
import type { AsyncBuffer, ColumnData, DecodedArray, FileMetaData } from "hyparquet";
import { compressors } from "hyparquet-compressors";

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

export interface Manifest {
  model: string;
  ribasim_version: string | null;
  starttime: string;
  endtime: string;
  bounds: [number, number, number, number];
  node_types: Record<string, number>;
  files: Record<"nodes" | "links" | "basin_area" | "waterboards", FileEntry>;
  tables: TableEntry[];
}

export type Row = Record<string, unknown>;

const DEFAULT_DATA_URL = "https://s3.deltares.nl/ribasim-nl/doc-image/webmap/lhm_coupled/";
const dataUrl = new URL(
  new URLSearchParams(location.search).get("data") ?? (import.meta.env.DEV ? "/" : DEFAULT_DATA_URL),
  location.href,
);

/** URL of an exported file; the content hash makes it safe to cache indefinitely. */
export function fileUrl(entry: FileEntry): string {
  return new URL(`${entry.path}?v=${entry.hash}`, dataUrl).href;
}

export async function loadManifest(): Promise<Manifest> {
  const response = await fetch(new URL("manifest.json", dataUrl), { cache: "no-cache" });
  if (!response.ok) throw new Error(`Failed to load manifest.json: HTTP ${response.status}`);
  return response.json();
}

interface ParquetFile {
  file: AsyncBuffer;
  metadata: FileMetaData;
}

const parquetFiles = new Map<string, Promise<ParquetFile>>();

function openParquet(entry: FileEntry): Promise<ParquetFile> {
  const url = fileUrl(entry);
  let parquet = parquetFiles.get(url);
  if (!parquet) {
    parquet = (async () => {
      const file = cachedAsyncBuffer(await asyncBufferFromUrl({ url, byteLength: entry.bytes }));
      return { file, metadata: await parquetMetadataAsync(file) };
    })();
    parquetFiles.set(url, parquet);
  }
  return parquet;
}

/** Read complete columns as arrays. */
export async function readColumns<K extends string>(
  entry: FileEntry,
  columns: K[],
): Promise<Record<K, DecodedArray>> {
  const { file, metadata } = await openParquet(entry);
  const chunks: ColumnData[] = [];
  await parquetRead({ file, metadata, columns, compressors, onChunk: (chunk) => chunks.push(chunk) });
  const numRows = Number(metadata.num_rows);
  const result = {} as Record<K, DecodedArray>;
  for (const column of columns) {
    const parts = chunks.filter((c) => c.columnName === column).sort((a, b) => a.rowStart - b.rowStart);
    const values: unknown[] = [];
    for (const part of parts) {
      if (part.rowStart !== values.length) throw new Error(`Gap in column ${column} of ${entry.path}`);
      for (let i = 0; i < part.columnData.length; i++) values.push(part.columnData[i]);
    }
    if (values.length !== numRows) throw new Error(`Column ${column} of ${entry.path} is incomplete`);
    result[column] = values as DecodedArray;
  }
  return result;
}

export async function readRow(entry: FileEntry, row: number): Promise<Row> {
  const { file, metadata } = await openParquet(entry);
  const rows = await parquetReadObjects({ file, metadata, rowStart: row, rowEnd: row + 1, compressors });
  if (rows.length !== 1) throw new Error(`Row ${row} not found in ${entry.path}`);
  return rows[0];
}

/** Rows of a table for one node; tables are sorted by node_id so statistics skip other row groups. */
export async function readNodeRows(entry: FileEntry, nodeId: number): Promise<Row[]> {
  const { file, metadata } = await openParquet(entry);
  return parquetReadObjects({ file, metadata, filter: { node_id: { $eq: nodeId } }, compressors });
}
