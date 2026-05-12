import { fileURLToPath } from 'node:url';
import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const docsRoot = path.resolve(scriptDir, '..');
const sourceDir = path.join(docsRoot, 'docs');
const llmsFullPath = path.join(docsRoot, 'build', 'llms-full.txt');

function normalize(text) {
  return text
    .replace(/\r\n/g, '\n')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeContent(text) {
  return normalize(
    text
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

function stripFrontMatter(source) {
  if (!source.startsWith('---\n')) {
    return source;
  }
  const end = source.indexOf('\n---\n', 4);
  if (end === -1) {
    return source;
  }
  return source.slice(end + '\n---\n'.length);
}

function firstHeading(source) {
  for (const line of stripFrontMatter(source).split('\n')) {
    const match = line.match(/^#\s+(.+?)\s*$/);
    if (match) {
      return match[1].trim();
    }
  }
  return '';
}

function firstContentSignature(source) {
  let inFence = false;
  for (const rawLine of stripFrontMatter(source).split('\n')) {
    const line = rawLine.trim();
    if (line.startsWith('```') || line.startsWith('~~~')) {
      inFence = !inFence;
      continue;
    }
    if (inFence || line === '' || line.startsWith('#') || line.startsWith('import ') || line.startsWith('export ')) {
      continue;
    }
    if (line.startsWith('|') || line.startsWith('<') || line.startsWith('{')) {
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
    console.error(`Missing ${path.relative(docsRoot, llmsFullPath)}. Run npm run build from docs/ first.`);
    throw error;
  }

  const normalizedLlmsFull = normalizeContent(llmsFull);
  const docs = await walkMarkdownFiles(sourceDir);
  const failures = [];

  for (const file of docs) {
    const source = await fs.readFile(file, 'utf8');
    const relative = path.relative(docsRoot, file);
    const heading = firstHeading(source);
    const signature = firstContentSignature(source);

    if (!heading) {
      failures.push(`${relative}: missing first-level heading`);
      continue;
    }
    if (!normalizedLlmsFull.includes(normalize(heading))) {
      failures.push(`${relative}: heading ${JSON.stringify(heading)} not found in llms-full.txt`);
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

  console.log(`Verified ${docs.length} docs pages in build/llms-full.txt.`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
