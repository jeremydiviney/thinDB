// Base-path-aware links. The site deploys to GitHub Pages under /thinDB/
// (DOCS_BASE in the docs workflow); local dev and custom-domain deploys serve
// from the root. All internal hrefs go through withBase().
export const base = import.meta.env.BASE_URL.replace(/\/$/, '')
export const withBase = (path: string) => base + path
