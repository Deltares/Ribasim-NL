/**
 * Search the nodes and links of a model by their id, name and codes, and the Netherlands for places. Only type
 * imports, so Node runs its tests as they are, see `search.test.ts`.
 */

export type FeatureKind = "node" | "link";

/** A feature whose text matches a query, by its best matching column. */
export interface Match {
  kind: FeatureKind;
  id: number;
  column: string;
  value: string;
  /** How well the value matches, see `matchRank` */
  rank: number;
}

/** Text searches need this many characters, since shorter ones match about everything */
const MIN_QUERY_LENGTH = 2;
/** Columns whose values this many features share on average are categories, like `meta_categorie`, not codes */
const CATEGORY_SHARE = 3;
const WORD_CHARACTER = /[\p{L}\p{N}]/u;

/** Text in lowercase, without accents and with single spaces, so "Fryslan" finds "Fryslân" and "ij" finds "Ĳ". */
export function normalize(text: string): string {
  return text.normalize("NFKD").replace(/\p{M}/gu, "").toLowerCase().replace(/\s+/g, " ").trim();
}

/** The id in a query like "12" or "#12", or "n12" and "link 12" to search only nodes or links, whose ids overlap. */
export function parseId(query: string): { id: number; kind: FeatureKind | null } | null {
  const match = /^(?:(n|node|l|link)\s*#?\s*)?#?(\d+)$/i.exec(query.trim());
  if (!match) return null;
  const prefix = match[1]?.[0].toLowerCase();
  return { id: Number(match[2]), kind: prefix === "n" ? "node" : prefix === "l" ? "link" : null };
}

/**
 * How well a normalized value matches a normalized query, lower is better: 0 if equal, 1 if it starts with the query,
 * 2 if a word in it does, 3 if it contains the query, 4 if it contains all its words in another order, or -1 if not.
 */
export function matchRank(value: string, query: string): number {
  if (value === query) return 0;
  if (value.startsWith(query)) return 1;
  let at = value.indexOf(query);
  if (at >= 0) {
    for (; at >= 0; at = value.indexOf(query, at + 1)) {
      if (!WORD_CHARACTER.test(value[at - 1])) return 2;
    }
    return 3;
  }
  const words = query.split(" ");
  return words.length > 1 && words.every((word) => value.includes(word)) ? 4 : -1;
}

/** Better matches first, by `name` before other columns, shorter values, nodes before links, then id. */
function compareMatches(a: Match, b: Match): number {
  return (
    a.rank - b.rank ||
    Number(a.column !== "name") - Number(b.column !== "name") ||
    a.value.length - b.value.length ||
    Number(a.kind === "link") - Number(b.kind === "link") ||
    a.id - b.id ||
    (a.column < b.column ? -1 : a.column > b.column ? 1 : 0)
  );
}

/**
 * The text columns to search: `name`, and the `meta_` columns that identify features, like `meta_code_waterbeheerder`.
 * Categories that many features share, like `meta_categorie`, would bury the names under thousands of equal matches.
 */
export function searchedColumns(table: Record<string, unknown[]>): string[] {
  return Object.entries(table)
    .filter(([column, values]) => {
      if (column === "name") return true;
      if (!column.startsWith("meta_")) return false;
      const texts = values.filter((value) => typeof value === "string" && value.trim() !== "");
      const distinct = new Set(texts).size;
      return distinct > 0 && texts.length < CATEGORY_SHARE * distinct;
    })
    .map(([column]) => column);
}

/** The text values of the nodes and links, to find features by name or code. */
export class TextIndex {
  private readonly kinds: FeatureKind[] = [];
  private readonly ids: number[] = [];
  private readonly columns: string[] = [];
  private readonly values: string[] = [];
  private readonly keys: string[] = [];
  private readonly names: Record<FeatureKind, Map<number, string>> = { node: new Map(), link: new Map() };

  /** Add a value of a feature; empty values and those that are no text are left out. */
  add(kind: FeatureKind, id: number, column: string, value: unknown): void {
    if (typeof value !== "string") return;
    const key = normalize(value);
    if (key === "") return;
    this.kinds.push(kind);
    this.ids.push(id);
    this.columns.push(column);
    this.values.push(value);
    this.keys.push(key);
    if (column === "name") this.names[kind].set(id, value);
  }

  /** The name of a feature, if it has one. */
  name(kind: FeatureKind, id: number): string | undefined {
    return this.names[kind].get(id);
  }

  /** The features that match a query, each by its best matching value, the best first. */
  search(query: string): Match[] {
    const normalized = normalize(query);
    if (normalized.length < MIN_QUERY_LENGTH) return [];
    const best = new Map<string, Match>();
    this.keys.forEach((key, i) => {
      const rank = matchRank(key, normalized);
      if (rank < 0) return;
      const match = { kind: this.kinds[i], id: this.ids[i], column: this.columns[i], value: this.values[i], rank };
      const feature = `${match.kind}${match.id}`;
      const current = best.get(feature);
      if (!current || compareMatches(match, current) < 0) best.set(feature, match);
    });
    return [...best.values()].sort(compareMatches);
  }
}

// The Locatieserver of PDOK finds Dutch places, see https://api.pdok.nl/bzk/locatieserver/search/v3_1/ui/
const PLACES_URL = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/";
/** The kinds of places to find, by their PDOK type; the model has the names of the waters itself */
export const PLACE_TYPES: Record<string, string> = {
  woonplaats: "town",
  gemeente: "municipality",
  provincie: "province",
};
const MAX_PLACES = 4;

/** A place found by the Locatieserver, with its center in lon/lat. */
export interface Place {
  id: string;
  name: string;
  type: string;
  center: [number, number];
}

/** West, south, east and north in degrees. */
export type Bounds = [number, number, number, number];

/** The bounds of the coordinates of a WKT geometry in lon/lat, or null if it has none. */
export function wktBounds(wkt: string): Bounds | null {
  const bounds: Bounds = [Infinity, Infinity, -Infinity, -Infinity];
  for (const [, x, y] of wkt.matchAll(/(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)/g)) {
    bounds[0] = Math.min(bounds[0], Number(x));
    bounds[1] = Math.min(bounds[1], Number(y));
    bounds[2] = Math.max(bounds[2], Number(x));
    bounds[3] = Math.max(bounds[3], Number(y));
  }
  return Number.isFinite(bounds[0]) ? bounds : null;
}

/** Whether to search places for a query: it has letters, and is no id. */
export function isPlaceQuery(query: string): boolean {
  return normalize(query).length >= MIN_QUERY_LENGTH && /\p{L}/u.test(query) && parseId(query) === null;
}

/** The places in a response of the Locatieserver, leaving out those of other types or without a center. */
export function parsePlaces(response: unknown): Place[] {
  const docs = (response as { response?: { docs?: unknown } } | null)?.response?.docs;
  if (!Array.isArray(docs)) throw new Error("Unexpected response of the place search");
  return docs.flatMap((doc: Record<string, unknown>) => {
    const { id, type, weergavenaam: name, centroide_ll: point } = doc;
    const center = typeof point === "string" ? wktBounds(point) : null;
    const valid = typeof id === "string" && typeof name === "string" && typeof type === "string" && type in PLACE_TYPES;
    return valid && center ? [{ id, name, type, center: [center[0], center[1]] }] : [];
  });
}

async function fetchPlaces(endpoint: "suggest" | "lookup", params: Record<string, string>, signal?: AbortSignal) {
  const url = new URL(endpoint, PLACES_URL);
  url.search = new URLSearchParams(params).toString();
  const response = await fetch(url, { signal });
  if (!response.ok) throw new Error(`The place search failed: HTTP ${response.status}`);
  return response.json();
}

/** The towns, municipalities and provinces whose name matches a query, the best first; none if it is no place query. */
export async function findPlaces(query: string, signal?: AbortSignal): Promise<Place[]> {
  if (!isPlaceQuery(query)) return [];
  const fq = `type:(${Object.keys(PLACE_TYPES).join(" OR ")})`;
  const fl = "id,type,weergavenaam,centroide_ll";
  return parsePlaces(await fetchPlaces("suggest", { q: query.trim(), rows: String(MAX_PLACES), fq, fl }, signal));
}

/** The bounds of the outline of a place, or null if it has none. */
export async function placeBounds(place: Place, signal?: AbortSignal): Promise<Bounds | null> {
  const json = await fetchPlaces("lookup", { id: place.id, fl: "id,geometrie_ll" }, signal);
  const wkt = json?.response?.docs?.[0]?.geometrie_ll;
  return typeof wkt === "string" ? wktBounds(wkt) : null;
}

/** An option in the list of the search box. */
export interface SearchOption {
  label: string;
  /** A second line, like the name of a node or the kind of a place, or empty */
  detail: string;
  symbol: Element | null;
  choose: () => void;
}

/** The options for a query, and how many more match than they show. */
export interface SearchResults {
  options: SearchOption[];
  more: number;
}

export type SearchSource = (query: string, signal: AbortSignal) => Promise<SearchResults>;

/** The results of a source, with the query they are for. */
interface Found extends SearchResults {
  query: string;
}

/** Places are searched once typing pauses this long, in milliseconds, so not every keystroke is a request */
const PLACE_DELAY_MS = 250;

function el<K extends keyof HTMLElementTagNameMap>(tag: K, className = "", text = ""): HTMLElementTagNameMap[K] {
  const element = document.createElement(tag);
  element.className = className;
  element.textContent = text;
  return element;
}

/**
 * A search box that lists the features and places that match while typing. Enter, or a click, goes to the chosen
 * option, by default the first; the arrow keys choose another.
 */
export class SearchBox {
  readonly element: HTMLElement;
  private readonly input: HTMLInputElement;
  private readonly list: HTMLElement;
  /** The latest results, which may be of an earlier query until those of the current query are found */
  private features: Found | null = null;
  private places: Found | null = null;
  private options: SearchOption[] = [];
  /** The option chosen with the arrow keys, or -1 */
  private active = -1;
  private controller = new AbortController();
  private readonly sources: Record<"features" | "places", SearchSource>;

  // No parameter properties, which Node cannot strip for the tests
  constructor(
    sources: Record<"features" | "places", SearchSource>,
    { placeholder, title, prepare }: { placeholder: string; title: string; prepare: () => void },
  ) {
    this.sources = sources;
    this.input = el("input", "search");
    Object.assign(this.input, { type: "search", placeholder, title, autocomplete: "off", spellcheck: false });
    this.list = el("div", "search-list");
    this.list.id = "search-list";
    this.list.hidden = true;
    this.list.setAttribute("role", "listbox");
    this.input.setAttribute("role", "combobox");
    this.input.setAttribute("aria-controls", this.list.id);
    this.input.setAttribute("aria-expanded", "false");
    // Keep the focus in the input while an option is clicked, which would otherwise close the list first
    this.list.addEventListener("mousedown", (event) => event.preventDefault());
    this.input.addEventListener("focus", prepare, { once: true });
    this.input.addEventListener("focus", () => this.render());
    this.input.addEventListener("blur", () => this.show(false));
    this.input.addEventListener("input", () => void this.search(PLACE_DELAY_MS));
    this.input.addEventListener("keydown", (event) => this.onKey(event));
    this.element = el("div", "search-box");
    this.element.append(this.input, this.list);
  }

  /** Find the options for the query; the places only once typing pauses for `delay` milliseconds. */
  private async search(delay: number): Promise<void> {
    this.controller.abort();
    const { signal } = (this.controller = new AbortController());
    const query = this.input.value;
    this.active = -1;
    if (query.trim() === "") {
      this.features = this.places = null;
      return this.render();
    }
    const found = (source: "features" | "places") => (results: SearchResults) => {
      if (signal.aborted) return;
      this[source] = { ...results, query };
      this.render();
    };
    const failed = (source: "features" | "places") => (error: unknown) => {
      if (signal.aborted) return;
      console.warn(`Searching ${source} failed`, error);
      found(source)({ options: [], more: 0 });
    };
    const paused = new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, delay);
      signal.addEventListener(
        "abort",
        () => {
          clearTimeout(timer);
          resolve();
        },
        { once: true },
      );
    });
    await Promise.all([
      this.sources.features(query, signal).then(found("features"), failed("features")),
      paused.then(() => this.sources.places(query, signal)).then(found("places"), failed("places")),
    ]);
  }

  private onKey(event: KeyboardEvent): void {
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      // Past the last or first option, no option is chosen
      const count = this.options.length + 1;
      this.active = ((this.active + 1 + (event.key === "ArrowDown" ? 1 : -1) + count) % count) - 1;
      this.render();
    } else if (event.key === "Enter") {
      event.preventDefault();
      void this.enter();
    } else if (event.key === "Escape" && !this.list.hidden) {
      // Close the list first, the next Escape clears the search
      event.preventDefault();
      this.show(false);
    }
  }

  /** Go to the chosen option, or else the first, once the options of the query are found. */
  private async enter(): Promise<void> {
    const chosen = this.options[this.active];
    if (chosen) return this.choose(chosen);
    const query = this.input.value;
    // Right after typing, the places can still be those of the previous query
    const placesFound = this.places?.query === query;
    const current = this.features?.query === query && (this.features.options.length > 0 || placesFound);
    if (!current) await this.search(0);
    if (this.input.value === query && this.options[0]) this.choose(this.options[0]);
  }

  private choose(option: SearchOption): void {
    this.input.blur();
    option.choose();
  }

  private show(shown: boolean): void {
    this.list.hidden = !shown;
    this.input.setAttribute("aria-expanded", String(shown));
  }

  /** List the options of the features, then those of the places, while the input has the focus. */
  private render(): void {
    const children: HTMLElement[] = [];
    this.options = [];
    for (const [title, found] of [["", this.features], ["Places", this.places]] as const) {
      if (!found?.options.length) continue;
      if (title) children.push(el("div", "search-heading", title));
      for (const option of found.options) children.push(this.optionElement(option, this.options.push(option) - 1));
      if (found.more > 0) children.push(el("div", "note", `and ${found.more.toLocaleString()} more`));
    }
    const query = this.input.value;
    if (!this.options.length && this.features?.query === query && this.places?.query === query) {
      children.push(el("div", "note", "No nodes, links or places found"));
    }
    this.list.replaceChildren(...children);
    if (this.options[this.active]) this.input.setAttribute("aria-activedescendant", `search-option-${this.active}`);
    else this.input.removeAttribute("aria-activedescendant");
    this.show(children.length > 0 && document.activeElement === this.input);
    this.list.querySelector(".active")?.scrollIntoView({ block: "nearest" });
  }

  private optionElement(option: SearchOption, index: number): HTMLElement {
    const active = index === this.active;
    const row = el("div", active ? "search-option active" : "search-option");
    row.id = `search-option-${index}`;
    row.title = option.detail ? `${option.label}\n${option.detail}` : option.label;
    row.setAttribute("role", "option");
    row.setAttribute("aria-selected", String(active));
    row.append(option.symbol ?? el("span"), el("span", "search-label", option.label));
    if (option.detail) row.append(el("span", "search-detail", option.detail));
    row.addEventListener("click", () => this.choose(option));
    return row;
  }
}
