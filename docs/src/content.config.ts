import { defineCollection } from 'astro:content';
import { docsLoader } from '@astrojs/starlight/loaders';
import { docsSchema } from '@astrojs/starlight/schema';

// Registers the `docs` content collection backed by Starlight's loader so
// pages under src/content/docs/ are discovered and the sidebar slugs resolve.
export const collections = {
  docs: defineCollection({ loader: docsLoader(), schema: docsSchema() }),
};
