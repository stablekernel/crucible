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
        {
          label: 'Start here',
          items: [
            { slug: 'start/introduction' },
            { slug: 'start/quickstart' },
          ],
        },
        {
          label: 'Concepts',
          items: [{ slug: 'concepts/overview' }],
        },
        {
          label: 'Authoring guides',
          items: [{ slug: 'authoring/state-machines' }],
        },
        {
          label: 'Serialization & visualization',
          items: [{ slug: 'serialization/overview' }],
        },
        {
          label: 'Analysis & verification',
          items: [{ slug: 'analysis/overview' }],
        },
        {
          label: 'Reference',
          // TODO(DS2): replace this stub with gomarkdoc-generated API pages
          // (autogenerate from a generated `reference/` directory).
          items: [{ slug: 'reference/overview' }],
        },
        {
          label: 'Examples',
          items: [{ slug: 'examples/overview' }],
        },
        {
          label: 'Integrating',
          items: [{ slug: 'integrating/overview' }],
        },
      ],
    }),
  ],
});
