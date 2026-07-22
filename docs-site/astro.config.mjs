import { defineConfig } from 'astro/config'

export default defineConfig({
  output: 'static',
  trailingSlash: 'always',
  // GitHub Pages serves project sites under /thinDB/; the docs workflow sets
  // DOCS_BASE=/thinDB/. Local dev and custom-domain deploys stay at the root.
  site: process.env.DOCS_SITE ?? 'https://jeremydiviney.github.io',
  base: process.env.DOCS_BASE ?? '/',
})
