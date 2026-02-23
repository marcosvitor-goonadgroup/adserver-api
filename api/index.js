require('dotenv').config();
const express = require('express');
const { proxy } = require('../src/proxy');

const app = express();
app.use(express.json());

// ── Users ────────────────────────────────────────────────────────────────────
app.get('/users', (req, res) => proxy(req, res, { path: '/user' }));
app.post('/users', (req, res) => proxy(req, res, { path: '/user' }));
app.get('/users/:id', (req, res) => proxy(req, res, { path: `/user/${req.params.id}` }));
app.put('/users/:id', (req, res) => proxy(req, res, { path: `/user/${req.params.id}` }));
app.delete('/users/:id', (req, res) => proxy(req, res, { path: `/user/${req.params.id}` }));

// ── Reports ──────────────────────────────────────────────────────────────────
app.get('/stats', (req, res) => proxy(req, res, { path: '/stats' }));
app.get('/events', (req, res) => proxy(req, res, { path: '/events' }));
app.get('/conversions', (req, res) => proxy(req, res, { path: '/conversion' }));
app.get('/statement', (req, res) => proxy(req, res, { path: '/statement' }));

// ── Campaigns ────────────────────────────────────────────────────────────────
app.get('/campaigns', (req, res) => proxy(req, res, { path: '/campaign' }));
app.post('/campaigns', (req, res) => proxy(req, res, { path: '/campaign' }));
app.get('/campaigns/:id', (req, res) => proxy(req, res, { path: `/campaign/${req.params.id}` }));
app.put('/campaigns/:id', (req, res) => proxy(req, res, { path: `/campaign/${req.params.id}` }));
app.delete('/campaigns/:id', (req, res) => proxy(req, res, { path: `/campaign/${req.params.id}` }));

// ── Ads ──────────────────────────────────────────────────────────────────────
// List ads for a campaign
app.get('/campaigns/:id/ads', (req, res) =>
  proxy(req, res, { path: '/ad', params: { idcampaign: req.params.id } })
);
// assign must be before /:id to avoid route conflict
app.post('/ads/assign', (req, res) =>
  proxy(req, res, { path: '/ad/assign' })
);
app.post('/ads', (req, res) => proxy(req, res, { path: '/ad' }));
app.get('/ads/:id', (req, res) => proxy(req, res, { path: `/ad/${req.params.id}` }));
app.put('/ads/:id', (req, res) => proxy(req, res, { path: `/ad/${req.params.id}` }));
app.delete('/ads/:id', (req, res) => proxy(req, res, { path: `/ad/${req.params.id}` }));

// ── Sites ────────────────────────────────────────────────────────────────────
app.get('/sites', (req, res) => proxy(req, res, { path: '/site' }));
app.post('/sites', (req, res) => proxy(req, res, { path: '/site' }));
app.get('/sites/:id', (req, res) => proxy(req, res, { path: `/site/${req.params.id}` }));
app.put('/sites/:id', (req, res) => proxy(req, res, { path: `/site/${req.params.id}` }));
app.delete('/sites/:id', (req, res) => proxy(req, res, { path: `/site/${req.params.id}` }));

// ── Zones ────────────────────────────────────────────────────────────────────
// GET /zones?idsite=123
// assign must be before /:id to avoid route conflict
app.post('/zones/assign', (req, res) =>
  proxy(req, res, { path: '/zone/assign' })
);
app.get('/zones', (req, res) => proxy(req, res, { path: '/zone' }));
app.post('/zones', (req, res) => proxy(req, res, { path: '/zone' }));
app.get('/zones/:id', (req, res) => proxy(req, res, { path: `/zone/${req.params.id}` }));
app.put('/zones/:id', (req, res) => proxy(req, res, { path: `/zone/${req.params.id}` }));
app.delete('/zones/:id', (req, res) => proxy(req, res, { path: `/zone/${req.params.id}` }));

// ── Payments ─────────────────────────────────────────────────────────────────
app.get('/payments', (req, res) => proxy(req, res, { path: '/payment' }));
app.post('/payments', (req, res) => proxy(req, res, { path: '/payment' }));

// ── Payouts ──────────────────────────────────────────────────────────────────
app.get('/payouts', (req, res) => proxy(req, res, { path: '/payout' }));
app.post('/payouts', (req, res) => proxy(req, res, { path: '/payout' }));

// ── Referrals ────────────────────────────────────────────────────────────────
app.get('/referrals', (req, res) => proxy(req, res, { path: '/referral' }));

// ── Transactions ─────────────────────────────────────────────────────────────
app.get('/transactions', (req, res) => proxy(req, res, { path: '/transaction' }));
app.post('/transactions', (req, res) => proxy(req, res, { path: '/transaction' }));
app.get('/transactions/:id', (req, res) => proxy(req, res, { path: `/transaction/${req.params.id}` }));

// ── Dictionaries ─────────────────────────────────────────────────────────────
app.get('/dict', (req, res) => proxy(req, res, { path: '/dict' }));

// ── Viewability ──────────────────────────────────────────────────────────────
app.use((req, res, next) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

app.post('/viewability', (req, res) => {
  const { zone, url, viewed, visible_pct, elapsed_ms, ts } = req.body || {};
  console.log(JSON.stringify({
    event: 'viewability',
    zone,
    url,
    viewed: !!viewed,
    visible_pct,
    elapsed_ms,
    ts: ts || new Date().toISOString(),
  }));
  res.sendStatus(204);
});

// ── Health ───────────────────────────────────────────────────────────────────
app.get('/', (req, res) => res.json({ status: 'ok', version: '1.0.0' }));

const PORT = process.env.PORT || 4000;
if (require.main === module) {
  app.listen(PORT, () => console.log(`Server running on http://localhost:${PORT}`));
}

module.exports = app;
