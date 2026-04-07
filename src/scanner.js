/**
 * scanner.js — Malware/unsafe creative scanner (in-house)
 *
 * Checks ad creatives before they are forwarded to the adserver.
 * Covers: HTML banners, image/ZIP files (base64), and VAST URLs.
 * Compliant with Google Display Network (GDN) creative specs.
 *
 * Returns: { safe: true } or { safe: false, reason: string }
 */

// ── File size limits (GDN spec: 2.2MB total ad load) ─────────────────────────
const MAX_FILE_BYTES     = 2.2 * 1024 * 1024; // 2.2 MB — GDN hard limit
const MAX_HTML_BYTES     = 2.2 * 1024 * 1024; // same limit for HTML banners

// ── Animation / audio limits (GDN spec) ──────────────────────────────────────
// These are enforced via HTML pattern checks below (autoplay audio/video).

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

// ── Social media domains blocked on GDN ──────────────────────────────────────
// GDN policy: no 3rd-party social calls or widgets allowed.
const SOCIAL_MEDIA_DOMAINS = [
  'facebook.com',
  'fb.com',
  'connect.facebook.net',
  'fbcdn.net',
  'twitter.com',
  'twimg.com',
  't.co',
  'instagram.com',
  'cdninstagram.com',
  'pinterest.com',
  'pinimg.com',
  'linkedin.com',
  'tiktok.com',
  'tiktokcdn.com',
  'snapchat.com',
  'sc-cdn.net',
  'youtube.com',       // allowed only in VAST, not in HTML banners
  'youtu.be',
  'reddit.com',
  'redditmedia.com',
  'whatsapp.com',
  'wa.me',
];

// ── Google trademark terms (GDN policy: no Google logos/trademarks in ads) ───
const GOOGLE_TRADEMARK_PATTERNS = [
  /google\s*logo/i,
  /android\s*logo/i,
  /"google"/i,
  /google\.com\/images\/branding/i,
  /lh\d+\.googleusercontent\.com/i,   // Google user content CDN (logo hosting)
  /ssl\.gstatic\.com\/images\/branding/i,
];

// ── File magic bytes (real MIME detection) ────────────────────────────────────
const MAGIC_BYTES = {
  // JPEG: FF D8 FF
  jpeg:  [0xFF, 0xD8, 0xFF],
  // PNG: 89 50 4E 47
  png:   [0x89, 0x50, 0x4E, 0x47],
  // GIF87a
  gif87: [0x47, 0x49, 0x46, 0x38, 0x37, 0x61],
  // GIF89a
  gif89: [0x47, 0x49, 0x46, 0x38, 0x39, 0x61],
  // ZIP (PK header)
  zip:   [0x50, 0x4B, 0x03, 0x04],
  // WebP: RIFF????WEBP
  webp:  [0x52, 0x49, 0x46, 0x46],
  // MP4 (ftyp box at offset 4)
  mp4:   [0x66, 0x74, 0x79, 0x70],
};

// ── Dangerous HTML patterns (security + GDN policy) ───────────────────────────
const DANGEROUS_HTML_PATTERNS = [
  // Script tags (any form) — malware + GDN policy
  { pattern: /<script[\s\S]*?>/i,                              reason: 'HTML contains a <script> tag. Only static HTML/CSS is allowed.' },
  // Event handlers (onclick, onload, onerror, etc.) — XSS
  { pattern: /\bon\w+\s*=/i,                                   reason: 'HTML contains an inline event handler (e.g. onclick, onload). Not allowed.' },
  // javascript: URI — XSS
  { pattern: /javascript\s*:/i,                                reason: 'HTML contains a javascript: URI. Not allowed.' },
  // data: URI with script/html content — XSS
  { pattern: /data\s*:\s*text\s*\/\s*(html|javascript)/i,      reason: 'HTML contains a data: URI with executable content. Not allowed.' },
  // eval / Function constructor — obfuscated JS
  { pattern: /\beval\s*\(/i,                                   reason: 'HTML contains eval(). Not allowed.' },
  { pattern: /new\s+Function\s*\(/i,                           reason: 'HTML contains new Function(). Not allowed.' },
  // document.write — XSS vector
  { pattern: /document\s*\.\s*write\s*\(/i,                    reason: 'HTML contains document.write(). Not allowed.' },
  // XHR / fetch — unauthorized data exfiltration
  { pattern: /\bfetch\s*\(/i,                                  reason: 'HTML contains fetch(). Not allowed.' },
  { pattern: /XMLHttpRequest/i,                                reason: 'HTML contains XMLHttpRequest. Not allowed.' },
  // Dynamic script injection
  { pattern: /createElement\s*\(\s*['"]script['"]\s*\)/i,      reason: 'HTML contains dynamic script injection. Not allowed.' },
  // atob — base64 obfuscation
  { pattern: /atob\s*\(/i,                                     reason: 'HTML contains atob() (base64 decode). Not allowed.' },
  // Meta refresh redirect — GDN policy
  { pattern: /<meta[^>]+http-equiv\s*=\s*['"]refresh['"]/i,    reason: 'HTML contains a meta refresh redirect. Not allowed.' },
  // Form tags — phishing / PII collection (GDN policy: no PII)
  { pattern: /<form[\s\S]*?>/i,                                reason: 'HTML contains a <form> tag. Ads may not collect personal information (PII).' },
  // Input fields — PII collection
  { pattern: /<input[\s\S]*?>/i,                               reason: 'HTML contains an <input> field. Ads may not collect personal information (PII).' },
  // Object/embed/applet — plugin-based attacks
  { pattern: /<(object|embed|applet)[\s\S]*?>/i,               reason: 'HTML contains <object>, <embed>, or <applet>. Plugin-based ads are not allowed.' },
  // Link preload/prefetch to external resources
  { pattern: /<link[^>]+rel\s*=\s*['"]?(preload|prefetch)/i,   reason: 'HTML contains resource preload/prefetch directives. Not allowed.' },
  // SVG with inline event handlers
  { pattern: /<svg[\s\S]*?on\w+\s*=/i,                         reason: 'HTML contains an SVG with inline event handlers. Not allowed.' },
  // Autoplay audio — GDN policy: audio must be user-initiated
  { pattern: /<audio[^>]*\bautoplay\b/i,                       reason: 'Ad contains autoplay audio. Sound must be user-initiated (GDN policy).' },
  // Autoplay video with audio — GDN policy
  { pattern: /<video[^>]*\bautoplay\b(?![^>]*\bmuted\b)/i,     reason: 'Ad contains autoplay video with audio. Autoplay is only allowed when muted (GDN policy).' },
];

// ── Helpers ───────────────────────────────────────────────────────────────────

function bufferStartsWith(buf, magic) {
  for (let i = 0; i < magic.length; i++) {
    if (buf[i] !== magic[i]) return false;
  }
  return true;
}

function detectMimeFromBuffer(buf) {
  if (bufferStartsWith(buf, MAGIC_BYTES.jpeg))  return 'image/jpeg';
  if (bufferStartsWith(buf, MAGIC_BYTES.png))   return 'image/png';
  if (bufferStartsWith(buf, MAGIC_BYTES.gif87)) return 'image/gif';
  if (bufferStartsWith(buf, MAGIC_BYTES.gif89)) return 'image/gif';
  if (bufferStartsWith(buf, MAGIC_BYTES.zip))   return 'application/zip';
  if (bufferStartsWith(buf, MAGIC_BYTES.webp))  return 'image/webp';
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

function containsSocialMediaDomain(html) {
  for (const domain of SOCIAL_MEDIA_DOMAINS) {
    // Match domain in src, href, or any attribute value
    const escaped = domain.replace('.', '\\.');
    const re = new RegExp(escaped, 'i');
    if (re.test(html)) return domain;
  }
  return null;
}

function containsGoogleTrademark(html) {
  for (const pattern of GOOGLE_TRADEMARK_PATTERNS) {
    if (pattern.test(html)) return true;
  }
  return false;
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

    // Size check (GDN: 2.2MB total)
    if (html.length > MAX_HTML_BYTES) {
      return {
        safe: false,
        reason: `HTML content exceeds the maximum allowed size of 2.2MB (GDN policy). Current size: ${(html.length / 1024 / 1024).toFixed(2)}MB.`,
      };
    }

    // Dangerous pattern checks
    for (const { pattern, reason } of DANGEROUS_HTML_PATTERNS) {
      if (pattern.test(html)) {
        return { safe: false, reason };
      }
    }

    // Social media domain check (GDN policy: no social widgets)
    const socialDomain = containsSocialMediaDomain(html);
    if (socialDomain) {
      return {
        safe: false,
        reason: `HTML creative references a social media domain (${socialDomain}). GDN policy does not allow social media calls or widgets in ads.`,
      };
    }

    // Google trademark check (GDN policy: no Google logos/trademarks)
    if (containsGoogleTrademark(html)) {
      return {
        safe: false,
        reason: 'HTML creative appears to contain Google logos or trademark references. Ads may not include Google or Android logos/trademarks (GDN policy).',
      };
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

    // Size check (GDN: 2.2MB total ad load)
    if (buf.length > MAX_FILE_BYTES) {
      return {
        safe: false,
        reason: `File exceeds the maximum allowed size of 2.2MB (GDN policy). Current size: ${(buf.length / 1024 / 1024).toFixed(2)}MB.`,
      };
    }

    const detectedMime = detectMimeFromBuffer(buf);

    // ZIP: require manual review
    if (detectedMime === 'application/zip') {
      return {
        safe: false,
        reason: 'ZIP files require manual review before activation. Please contact the platform administrator to validate the ZIP contents.',
      };
    }

    if (detectedMime === 'unknown') {
      return {
        safe: false,
        reason: 'File type could not be determined from its content. Only JPG, PNG, GIF, WebP, and MP4 files are accepted.',
      };
    }

    const allowedMimes = ['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'video/mp4'];
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

    // Must be HTTPS (GDN SSL requirement)
    if (!url.startsWith('https://')) {
      return { safe: false, reason: 'VAST URL must use HTTPS (GDN SSL requirement).' };
    }

    // Domain whitelist check
    if (!isVastDomainAllowed(url)) {
      return {
        safe: false,
        reason: `VAST URL domain is not in the approved whitelist. Contact the administrator to add your domain. URL: ${url}`,
      };
    }

    return { safe: true };
  }

  // No scannable content found — allow through
  return { safe: true };
}

module.exports = { scanCreative };
