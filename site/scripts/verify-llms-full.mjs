import { fileURLToPath } from 'node:url';
import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const siteRoot = path.resolve(scriptDir, '..');
const sourceDir = path.join(siteRoot, 'content', 'docs');
const llmsFullPath = path.join(siteRoot, '.output', 'public', 'llms-full.txt');

function normalize(text) {
  return text
    .replace(/\r\n/g, '\n')
    .replace(/\s+/g, ' ')
    .trim();
}

// The source side of the comparison is raw Markdown while the llms-full.txt
// side has been through the renderer, which resolves entities written in the
// source (`&mdash;` -> `—`) and escapes some literal punctuation on its way out
// (`*` -> `&#x2A;`). Decode both sides before stripping Markdown punctuation so
// the two spellings of the same character compare equal.
const NAMED_ENTITIES = {
  amp: '&',
  apos: "'",
  gt: '>',
  hellip: '…',
  laquo: '«',
  ldquo: '“',
  lsquo: '‘',
  lt: '<',
  mdash: '—',
  ndash: '–',
  nbsp: ' ',
  quot: '"',
  raquo: '»',
  rdquo: '”',
  rsquo: '’',
};

function decodeEntities(text) {
  return text.replace(/&(#[Xx][0-9A-Fa-f]+|#\d+|[A-Za-z][A-Za-z0-9]*);/g, (match, entity) => {
    const codePoint = entity[0] === '#'
      ? Number.parseInt(entity.slice(entity[1] === 'x' || entity[1] === 'X' ? 2 : 1), entity[1] === 'x' || entity[1] === 'X' ? 16 : 10)
      : Number.NaN;
    if (Number.isInteger(codePoint)) {
      if (codePoint < 0 || codePoint > 0x10FFFF) {
        return match;
      }
      return String.fromCodePoint(codePoint);
    }
    const named = NAMED_ENTITIES[entity];
    return named === undefined ? match : named;
  });
}

function normalizeContent(text) {
  return normalize(
    decodeEntities(text)
      .replace(/\[(.*?)\]\((.*?)\)/g, '$1')
      .replace(/[`*_{}]/g, ''),
  );
}

async function walkMarkdownFiles(dir) {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...await walkMarkdownFiles(fullPath));
      continue;
    }
    if (entry.isFile() && /\.(md|mdx)$/.test(entry.name)) {
      files.push(fullPath);
    }
  }
  return files.sort();
}

function parseFrontMatter(source) {
  if (!source.startsWith('---\n')) {
    return { attrs: {}, body: source };
  }
  const end = source.indexOf('\n---\n', 4);
  if (end === -1) {
    return { attrs: {}, body: source };
  }
  const raw = source.slice(4, end);
  const attrs = {};
  for (const line of raw.split('\n')) {
    const match = line.match(/^([A-Za-z0-9_-]+):\s*(.*)$/);
    if (!match) {
      continue;
    }
    attrs[match[1]] = match[2].trim().replace(/^['"]|['"]$/g, '');
  }
  return { attrs, body: source.slice(end + '\n---\n'.length) };
}

function firstHeading(body) {
  for (const line of body.split('\n')) {
    const match = line.match(/^#\s+(.+?)\s*$/);
    if (match) {
      return match[1].trim();
    }
  }
  return '';
}

function pageTitle(source) {
  const { attrs, body } = parseFrontMatter(source);
  return attrs.title || firstHeading(body);
}

function firstContentSignature(source) {
  const { body } = parseFrontMatter(source);
  let inFence = false;
  for (const rawLine of body.split('\n')) {
    const line = rawLine.trim();
    if (line.startsWith('```') || line.startsWith('~~~')) {
      inFence = !inFence;
      continue;
    }
    if (inFence || line === '' || line.startsWith('#') || line.startsWith('import ') || line.startsWith('export ')) {
      continue;
    }
    if (line.startsWith('|') || line.startsWith('<') || line.startsWith('{') || line.startsWith('::')) {
      continue;
    }
    const cleaned = normalizeContent(line);
    if (cleaned.length >= 40) {
      return cleaned.slice(0, 180);
    }
  }
  return '';
}

async function main() {
  let llmsFull;
  try {
    llmsFull = await fs.readFile(llmsFullPath, 'utf8');
  } catch (error) {
    console.error(`Missing ${path.relative(siteRoot, llmsFullPath)}. Run npm run build from site/ first.`);
    throw error;
  }

  const normalizedLlmsFull = normalizeContent(llmsFull);
  const docs = await walkMarkdownFiles(sourceDir);
  const failures = [];

  for (const file of docs) {
    const source = await fs.readFile(file, 'utf8');
    const relative = path.relative(siteRoot, file);
    const heading = pageTitle(source);
    const signature = firstContentSignature(source);

    if (!heading) {
      failures.push(`${relative}: missing title or first-level heading`);
      continue;
    }
    // normalizeContent, not normalize: the haystack has already had entities
    // decoded and Markdown punctuation stripped, so the needle must match.
    if (!normalizedLlmsFull.includes(normalizeContent(heading))) {
      failures.push(`${relative}: title ${JSON.stringify(heading)} not found in llms-full.txt`);
      continue;
    }
    if (signature && !normalizedLlmsFull.includes(signature)) {
      failures.push(`${relative}: content signature not found in llms-full.txt`);
    }
  }

  if (failures.length > 0) {
    console.error('llms-full.txt does not include every current docs page:');
    for (const failure of failures) {
      console.error(`- ${failure}`);
    }
    process.exit(1);
  }

  console.log(`Verified ${docs.length} docs pages in .output/public/llms-full.txt.`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
