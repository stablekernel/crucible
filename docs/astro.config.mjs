// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import mermaid from 'astro-mermaid';

// GitHub project page: https://stablekernel.github.io/crucible
// `site` + `base` must match the Pages URL so generated links and assets resolve.
export default defineConfig({
  site: 'https://stablekernel.github.io',
  base: '/crucible',
  integrations: [
    // astro-mermaid renders ```mermaid fenced code blocks client-side at runtime.
    // Chosen over rehype-mermaid because it needs no headless browser (Playwright)
    // at build time, keeping the CI `npm run build` step fast and dependency-light.
    // It registers a remark plugin that transforms mermaid code blocks into a
    // hydrated <pre class="mermaid"> element. `theme: 'dark'` matches the
    // dark-default Crucible brand; mermaid auto-syncs when the user toggles theme.
    mermaid({
      theme: 'dark',
      autoTheme: true,
    }),
    starlight({
      title: 'Crucible',
      tagline: 'Forge event-driven services in Go.',
      description:
        'Crucible is a Go suite for forging event-driven services: thin seams, no-op defaults, no forced dependencies.',
      logo: {
        src: './src/assets/placeholders/logo.svg',
        alt: 'Crucible wordmark (placeholder)',
        replacesTitle: false,
      },
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/stablekernel/crucible',
        },
      ],
      customCss: ['./src/styles/crucible.css'],
      head: [
        // Social card / og:image. Replaced with generated art in a later PR.
        {
          tag: 'meta',
          attrs: { property: 'og:image', content: '/crucible/social-card.svg' },
        },
        {
          tag: 'meta',
          attrs: { name: 'twitter:card', content: 'summary_large_image' },
        },
      ],
      sidebar: [
        // Each module is its own top-level section with nested subsections.
        // Pages live in per-topic directories and order by `sidebar.order`.
        {
          label: 'State machine',
          items: [
            { label: 'Start here', items: [{ autogenerate: { directory: 'start' } }] },
            { label: 'Concepts', items: [{ autogenerate: { directory: 'concepts' } }] },
            { label: 'Authoring guides', items: [{ autogenerate: { directory: 'authoring' } }] },
            {
              label: 'Serialization & visualization',
              items: [{ autogenerate: { directory: 'serialization' } }],
            },
            {
              label: 'Analysis & verification',
              items: [{ autogenerate: { directory: 'analysis' } }],
            },
            { label: 'Examples', items: [{ autogenerate: { directory: 'examples' } }] },
            { label: 'Integrating', items: [{ autogenerate: { directory: 'integrating' } }] },
          ],
        },
        {
          label: 'Sink',
          // The egress IO seam. As further IO seams (broker, source) are
          // documented, each gets its own top-level section like this.
          items: [{ autogenerate: { directory: 'sink' } }],
        },
        {
          label: 'Reference',
          // Generated API reference for every module by `tools/docsgen`
          // (gomarkdoc) at build time; gitignored. New packages appear
          // automatically, ordered by each page's `sidebar.order`.
          items: [{ autogenerate: { directory: 'reference' } }],
        },
      ],
    }),
  ],
});
