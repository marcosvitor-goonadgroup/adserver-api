/**
 * scanner.js — Malware/unsafe creative scanner (in-house)
 *
 * Checks ad creatives before they are forwarded to the adserver.
 * Covers: HTML banners, image/ZIP files (base64), and VAST URLs.
 *
 * Returns: { safe: true } or { safe: false, reason: string }
 */

// ── Configurable VAST URL whitelist ──────────────────────────────────────────
// Add trusted video ad domains here. Subdomains are automatically allowed.
const VAST_DOMAIN_WHITELIST = [
  'aso1.net',
  'srv.aso1.net',
  'goonadgroup.com.br',
  'doubleclick.net',
  'googleads.g.doubleclick.net',
  'securepubads.g.doubleclick.net',
  'imasdk.googleapis.com',
  'pubads.g.doubleclick.net',
  'ad.doubleclick.net',
  'youtube.com',
  'spotxchange.com',
  'spotx.tv',
  'springserve.com',
  'improvedigital.com',
  'rubiconproject.com',
  'openx.net',
  'appnexus.com',
  'adnxs.com',
];

// ── File magic bytes (real MIME detection) ────────────────────────────────────
const MAGIC_BYTES = {
  // JPEG: FF D8 FF
  jpeg: [0xFF, 0xD8, 0xFF],
  // PNG: 89 50 4E 47
  png: [0x89, 0x50, 0x4E, 0x47],
  // GIF87a / GIF89a
  gif87: [0x47, 0x49, 0x46, 0x38, 0x37, 0x61],
  gif89: [0x47, 0x49, 0x46, 0x38, 0x39, 0x61],
  // ZIP (PK header)
  zip: [0x50, 0x4B, 0x03, 0x04],
  // WebP: RIFF????WEBP
  webp: [0x52, 0x49, 0x46, 0x46],
  // MP4 (ftyp box at offset 4)
  mp4: [0x66, 0x74, 0x79, 0x70],
};

// ── Max file size: 10 MB ─────────────────────────────────────────────────────
const MAX_FILE_BYTES = 10 * 1024 * 1024;

// ── Dangerous HTML patterns ───────────────────────────────────────────────────
const DANGEROUS_HTML_PATTERNS = [
  // Script tags (any form)
  /<script[\s\S]*?>/i,
  // Event handlers (onclick, onload, onerror, etc.)
  /\bon\w+\s*=/i,
  // javascript: URI
  /javascript\s*:/i,
  // data: URI with script content
  /data\s*:\s*text\s*\/\s*(html|javascript)/i,
  // eval / Function constructor
  /\beval\s*\(/i,
  /new\s+Function\s*\(/i,
  // document.write
  /document\s*\.\s*write\s*\(/i,
  // XHR / fetch
  /\bfetch\s*\(/i,
  /XMLHttpRequest/i,
  // Dynamic script injection
  /createElement\s*\(\s*['"]script['"]\s*\)/i,
  // Base64-encoded payloads inside HTML (common obfuscation)
  /atob\s*\(/i,
  // Meta refresh redirect
  /<meta[^>]+http-equiv\s*=\s*['"]refresh['"]/i,
  // Form tags (phishing)
  /<form[\s\S]*?>/i,
  // Object/embed/applet (plugin-based attacks)
  /<(object|embed|applet)[\s\S]*?>/i,
  // Link preload/prefetch to external resources
  /<link[^>]+rel\s*=\s*['"]?(preload|prefetch)/i,
  // SVG with scripts
  /<svg[\s\S]*?on\w+\s*=/i,
];

// ── Helpers ───────────────────────────────────────────────────────────────────

function bufferStartsWith(buf, magic) {
  for (let i = 0; i < magic.length; i++) {
    if (buf[i] !== magic[i]) return false;
  }
  return true;
}

function detectMimeFromBuffer(buf) {
  if (bufferStartsWith(buf, MAGIC_BYTES.jpeg)) return 'image/jpeg';
  if (bufferStartsWith(buf, MAGIC_BYTES.png))  return 'image/png';
  if (bufferStartsWith(buf, MAGIC_BYTES.gif87)) return 'image/gif';
  if (bufferStartsWith(buf, MAGIC_BYTES.gif89)) return 'image/gif';
  if (bufferStartsWith(buf, MAGIC_BYTES.zip))  return 'application/zip';
  if (bufferStartsWith(buf, MAGIC_BYTES.webp)) return 'image/webp';
  // MP4: ftyp box is at offset 4
  if (buf.length > 8 && bufferStartsWith(buf.slice(4), MAGIC_BYTES.mp4)) return 'video/mp4';
  return 'unknown';
}

function isVastDomainAllowed(urlStr) {
  try {
    const url = new URL(urlStr);
    const hostname = url.hostname.toLowerCase();
    return VAST_DOMAIN_WHITELIST.some(
      (allowed) => hostname === allowed || hostname.endsWith('.' + allowed)
    );
  } catch {
    return false;
  }
}

// ── Main scanner function ─────────────────────────────────────────────────────

/**
 * @param {object} details - The `details` field from POST /ads body
 * @returns {{ safe: boolean, reason?: string }}
 */
function scanCreative(details) {
  if (!details || typeof details !== 'object') {
    return { safe: true }; // No details = no creative to scan (text ad, etc.)
  }

  // ── 1. HTML Banner ──────────────────────────────────────────────────────────
  if (typeof details.content_html === 'string') {
    const html = details.content_html;

    if (html.length > MAX_FILE_BYTES) {
      return { safe: false, reason: 'HTML content exceeds maximum allowed size (10MB).' };
    }

    for (const pattern of DANGEROUS_HTML_PATTERNS) {
      if (pattern.test(html)) {
        return {
          safe: false,
          reason: `HTML creative contains unsafe pattern: ${pattern.toString()}. Only static HTML/CSS with <a>, <img>, and <div> tags are allowed.`,
        };
      }
    }

    return { safe: true };
  }

  // ── 2. File upload (base64) ─────────────────────────────────────────────────
  if (typeof details.file === 'string') {
    let buf;
    try {
      buf = Buffer.from(details.file, 'base64');
    } catch {
      return { safe: false, reason: 'File is not valid base64.' };
    }

    if (buf.length > MAX_FILE_BYTES) {
      return { safe: false, reason: `File exceeds maximum allowed size of ${MAX_FILE_BYTES / 1024 / 1024}MB.` };
    }

    const detectedMime = detectMimeFromBuffer(buf);

    const allowedMimes = ['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'video/mp4'];

    // ZIP: require manual review (can't safely scan contents without unzipping)
    if (detectedMime === 'application/zip') {
      return {
        safe: false,
        reason: 'ZIP files require manual review before activation. Please contact the platform administrator to validate the ZIP contents.',
      };
    }

    if (detectedMime === 'unknown') {
      return {
        safe: false,
        reason: 'File type could not be determined. Only JPG, PNG, GIF, WebP, and MP4 files are accepted.',
      };
    }

    if (!allowedMimes.includes(detectedMime)) {
      return {
        safe: false,
        reason: `File type "${detectedMime}" is not allowed. Accepted types: JPG, PNG, GIF, WebP, MP4.`,
      };
    }

    return { safe: true };
  }

  // ── 3. VAST URL ─────────────────────────────────────────────────────────────
  if (typeof details.vast_url === 'string') {
    const url = details.vast_url.trim();

    if (!url.startsWith('https://')) {
      return { safe: false, reason: 'VAST URL must use HTTPS.' };
    }

    if (!isVastDomainAllowed(url)) {
      return {
        safe: false,
        reason: `VAST URL domain is not in the approved whitelist. Contact the administrator to add your domain. URL: ${url}`,
      };
    }

    return { safe: true };
  }

  // ── 4. Video file (source_type = "file") ────────────────────────────────────
  // Handled by the base64 check above (details.file).

  // No scannable content found — allow through
  return { safe: true };
}

module.exports = { scanCreative };
