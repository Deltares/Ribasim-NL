import type { StyleSpecification } from "maplibre-gl";

interface RasterTiles {
  url: string;
  attribution: string;
}

/** Raster base maps are stacks of tile layers, vector base maps are complete MapLibre styles. */
type Basemap = { label: string; tiles: RasterTiles[] } | { label: string; style: string };

const PDOK_ATTRIBUTION = 'Kaartgegevens &copy; <a href="https://www.kadaster.nl">Kadaster</a>';
const brt = (layer: string): RasterTiles => ({
  url: `https://service.pdok.nl/brt/achtergrondkaart/wmts/v2_0/${layer}/EPSG:3857/{z}/{x}/{y}.png`,
  attribution: PDOK_ATTRIBUTION,
});

export const BASEMAPS: Record<string, Basemap> = {
  liberty: { label: "Liberty (OpenFreeMap)", style: "https://tiles.openfreemap.org/styles/liberty" },
  positron: { label: "Positron (OpenFreeMap)", style: "https://tiles.openfreemap.org/styles/positron" },
  grey: { label: "Grey (PDOK)", tiles: [brt("grijs")] },
  water: { label: "Water (PDOK)", tiles: [brt("water")] },
  topographic: {
    label: "Topographic (PDOK)",
    style: "https://api.pdok.nl/kadaster/brt-achtergrondkaart/ogc/v1/styles/standaard__webmercatorquad?f=mapbox",
  },
  aerial: {
    label: "Aerial imagery (PDOK)",
    // The grey map fills in outside the Netherlands, which the aerial imagery does not cover
    tiles: [
      brt("grijs"),
      {
        url: "https://service.pdok.nl/hwh/luchtfotorgb/wmts/v1_0/Actueel_orthoHR/EPSG:3857/{z}/{x}/{y}.jpeg",
        attribution: 'Luchtfoto &copy; <a href="https://www.beeldmateriaal.nl">Beeldmateriaal Nederland</a>',
      },
    ],
  },
  osm: {
    label: "OpenStreetMap",
    tiles: [
      {
        url: "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      },
    ],
  },
};

export const DEFAULT_BASEMAP = "liberty";

function rasterStyle(tiles: RasterTiles[]): StyleSpecification {
  return {
    version: 8,
    sources: Object.fromEntries(
      tiles.map(({ url, attribution }, i) => [
        `basemap-${i}`,
        { type: "raster", tiles: [url], tileSize: 256, maxzoom: 19, attribution },
      ]),
    ),
    layers: tiles.map((_, i) => ({ id: `basemap-${i}`, type: "raster", source: `basemap-${i}` })),
  };
}

export async function basemapStyle(id: string): Promise<StyleSpecification> {
  const basemap = BASEMAPS[id] ?? BASEMAPS[DEFAULT_BASEMAP];
  if ("tiles" in basemap) return rasterStyle(basemap.tiles);
  const response = await fetch(basemap.style);
  if (!response.ok) throw new Error(`Failed to load base map ${basemap.label}: HTTP ${response.status}`);
  return response.json();
}

/** The next base map style with the overlay sources and layers of the current style on top. */
export function withOverlays(
  current: StyleSpecification | undefined,
  next: StyleSpecification,
  overlaySources: Set<string>,
): StyleSpecification {
  if (!current) return next;
  const sources = Object.entries(current.sources).filter(([id]) => overlaySources.has(id));
  const layers = current.layers.filter((layer) => "source" in layer && overlaySources.has(layer.source as string));
  return { ...next, sources: { ...next.sources, ...Object.fromEntries(sources) }, layers: [...next.layers, ...layers] };
}
