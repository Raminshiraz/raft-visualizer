/* Compile app.jsx -> app.js (JSX only, no polyfills, no bundling).
 *
 *   npm install && npm run build
 *
 * app.js is committed so the page works from a bare checkout and from
 * GitHub Pages without any build step.
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { transformAsync } from '@babel/core';
import presetReact from '@babel/preset-react';

const here = path.dirname(fileURLToPath(import.meta.url));
const src = fs.readFileSync(path.join(here, 'app.jsx'), 'utf8');

const { code } = await transformAsync(src, {
  presets: [[presetReact, { runtime: 'classic' }]],
  babelrc: false,
  configFile: false,
  compact: false,
});

fs.writeFileSync(path.join(here, 'app.js'), code + '\n');
console.log(`app.js written — ${code.length} chars`);
