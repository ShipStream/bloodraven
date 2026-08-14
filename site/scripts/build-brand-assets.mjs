/**
 * Derives the site's brand assets from the master logo exports.
 *
 * The masters are 1536x1024 renders with the artwork floating in a large flat
 * field, plus one alpha cut-out of the mark. Everything the site actually loads
 * (header mark, favicons, OG card) is generated from them here so the crops are
 * reproducible rather than hand-made once and forgotten.
 *
 * Usage:  node scripts/build-brand-assets.mjs [--masters <dir>]
 *
 * Masters expected in <dir> (default: ~/Dropbox/Scratch):
 *   bloodraven-mark-3-transparent.png   square mark, real alpha channel
 *   bloodraven-mark-3-clean.png         square mark on black
 *   bloodraven-lockup-dark.png          mark + wordmark, white type on black
 *   bloodraven-lockup-light.png         mark + wordmark, dark type on cream
 */
import { mkdir } from 'node:fs/promises'
import { homedir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { createRequire } from 'node:module'

const here = dirname(fileURLToPath(import.meta.url))
const siteRoot = resolve(here, '..')

// sharp is not a direct dependency of the site; it arrives with @nuxt/image's
// ipx. Resolving it explicitly keeps this script dependency-free.
const require = createRequire(import.meta.url)
const sharp = require(join(siteRoot, 'node_modules/ipx/node_modules/sharp'))

const mastersArg = process.argv.indexOf('--masters')
const MASTERS = mastersArg > -1
  ? resolve(process.argv[mastersArg + 1])
  : join(homedir(), 'Dropbox/Scratch')

const OUT = join(siteRoot, 'public/img/brand')
const INK = '#070a0f'

const master = name => join(MASTERS, `bloodraven-${name}.png`)

/** Trims the flat surround from a master and returns the raw buffer. */
async function trimmed(name, threshold = 1) {
  return sharp(master(name)).trim({ threshold }).toBuffer()
}

/** Fits `buf` inside a square canvas with `pad` fraction of breathing room. */
async function square(buf, size, background, pad = 0.1) {
  const inner = Math.round(size * (1 - pad * 2))
  const art = await sharp(buf).resize({ width: inner, height: inner, fit: 'inside' }).toBuffer()
  return sharp({
    create: { width: size, height: size, channels: 4, background: background ?? { r: 0, g: 0, b: 0, alpha: 0 } },
  })
    .composite([{ input: art, gravity: 'center' }])
    .png()
    .toBuffer()
}

await mkdir(OUT, { recursive: true })

// --- Header / footer mark -------------------------------------------------
// Kept at its natural aspect and sized by height in CSS. Both variants are
// fetched on every page (CSS picks one, so `display:none` does not prevent the
// download), which is why this is 96px rather than a retina-generous 256.
const MARK_H = 96
const markAlpha = await trimmed('mark-3-transparent')
await sharp(markAlpha).resize({ height: MARK_H }).png({ compressionLevel: 9 }).toFile(join(OUT, 'mark.png'))

// Dark-surface variant. The raven is nearly black, so on a dark header it
// reads as a red dot and nothing else. Rather than washing the artwork out by
// raising its brightness, this lays a blurred white silhouette (taken from the
// alpha channel) behind it -- the bird stays black and gains a soft rim that
// separates it from the background.
{
  const { width, height } = await sharp(markAlpha).metadata()
  const silhouette = await sharp({ create: { width, height, channels: 3, background: '#ffffff' } })
    .joinChannel(await sharp(markAlpha).extractChannel('alpha').blur(width * 0.02).toBuffer())
    .png()
    .toBuffer()

  // Composite at full size first: sharp applies `resize` to the base canvas
  // before compositing, so chaining it here would shrink the canvas below the
  // overlays and throw.
  const rimLit = await sharp({ create: { width, height, channels: 4, background: { r: 0, g: 0, b: 0, alpha: 0 } } })
    // Twice, to build the rim up to a visible density.
    .composite([{ input: silhouette }, { input: silhouette }, { input: markAlpha }])
    .png()
    .toBuffer()

  await sharp(rimLit).resize({ height: MARK_H }).png({ compressionLevel: 9 }).toFile(join(OUT, 'mark-dark.png'))
}

// --- Favicons -------------------------------------------------------------
// The raven is nearly black, so a transparent favicon disappears against a dark
// tab strip. These sit on the ink field instead, which reads in both.
for (const size of [32, 180, 512]) {
  await sharp(await square(markAlpha, size, INK, 0.08))
    .png({ compressionLevel: 9 })
    .toFile(join(OUT, `favicon-${size}.png`))
}

// --- Social card ----------------------------------------------------------
// 1200x630 is the OG/Twitter summary_large_image size.
//
// Cropped straight out of the master rather than trimmed-and-recomposited: the
// master's "black" is a subtle vignette, so a trimmed rectangle dropped onto a
// flat field shows up as a lighter box. Cover-cropping keeps the artwork on its
// own background. The lockup sits in the middle third of the master, so
// trimming the 3:2 master to 1.9:1 only removes empty field.
await sharp(master('lockup-dark'))
  .resize(1200, 630, { fit: 'cover', position: 'centre' })
  .png({ compressionLevel: 9 })
  .toFile(join(OUT, 'og.png'))

console.log(`brand assets written to ${OUT}`)
