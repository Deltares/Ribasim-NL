import { defineConfig } from "vite";

// The dev server serves the local `export_webmap` output, the production build fetches from MinIO.
export default defineConfig(({ command }) => ({
  base: "./",
  publicDir: command === "serve" ? "../data/Rijkswaterstaat/webmap/lhm_coupled" : false,
  worker: { format: "es" },
  build: {
    outDir: "../docs/viewer-app",
    emptyOutDir: true,
    // maplibre-gl and deck.gl dominate the bundle size
    chunkSizeWarningLimit: 2500,
    rollupOptions: {
      input: "src/main.ts",
      output: {
        entryFileNames: "viewer.js",
        chunkFileNames: "[name]-[hash].js",
        assetFileNames: "viewer[extname]",
      },
    },
  },
}));
