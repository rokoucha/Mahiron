import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const apiTarget = process.env.MAHIRON_API_TARGET ?? "http://127.0.0.1:40772";

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "../internal/web/ui/dist/app",
    emptyOutDir: true,
  },
  server: {
    proxy: {
      "/api": apiTarget,
    },
  },
});
