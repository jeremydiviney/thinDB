# thinDB documentation site

The documentation site is an Astro 5 static site. It is isolated from the Zig
library and server build.

```powershell
cd docs-site
bun install
bun run dev
```

The development server defaults to `http://localhost:4321`. Run the strict
Astro check and production build with:

```powershell
bun run build
```

Generated output is written to `dist/`.
