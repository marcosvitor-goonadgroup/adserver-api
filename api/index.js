require('dotenv').config();
const express = require('express');
const { proxy, adserver } = require('../src/proxy');

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

// ── Debug: raw stats (temporary) ─────────────────────────────────────────────
// GET /debug/stats?dateBegin=&dateEnd=&idcampaign=&group=day&group2=site
app.get('/debug/stats', async (req, res) => {
  try {
    const r = await adserver.get('/stats', { params: req.query });
    res.json({ count: r.data.length, sample: r.data.slice(0, 3) });
  } catch (err) {
    if (err.response) return res.status(err.response.status).json(err.response.data);
    res.status(500).json({ error: err.message });
  }
});

// ── Campaign report: Campaign → Site → Zone → Ad (by day) ────────────────────
// GET /campaigns/:id/report?dateBegin=2026-01-01&dateEnd=2026-01-31
app.get('/campaigns/:id/report', async (req, res) => {
  const { id } = req.params;
  const { dateBegin, dateEnd } = req.query;

  if (!dateBegin || !dateEnd) {
    return res.status(400).json({ error: 'dateBegin and dateEnd are required' });
  }

  const statsBase   = { dateBegin, dateEnd, idcampaign: id };
  const eventsBase  = { dateBegin, dateEnd, idcampaign: id, report: 1 };

  try {
    // 4 calls in parallel — each gives a different dimension breakdown
    const [resSite, resZone, resAd, resEvents] = await Promise.all([
      adserver.get('/stats',  { params: { ...statsBase,  group: 'day', group2: 'site' } }),
      adserver.get('/stats',  { params: { ...statsBase,  group: 'day', group2: 'zone' } }),
      adserver.get('/stats',  { params: { ...statsBase,  group: 'day', group2: 'ad'   } }),
      adserver.get('/events', { params: { ...eventsBase, group: 'ad'                  } }).catch(() => ({ data: [] })),
    ]);

    // ── helpers ──────────────────────────────────────────────────────────────
    // API uses iddimension_2 / dimension_2 (underscore) for the group2 field
    const id2  = r => r.iddimension_2 ?? null;
    const dim2 = r => r.dimension_2   ?? null;

    function statsRow(r) {
      return {
        requests:           Number(r.requests)           || 0,
        impressions:        Number(r.impressions)        || 0,
        impressions_unique: Number(r.impressions_unique) || 0,
        views:              Number(r.views)              || 0,
        clicks:             Number(r.clicks)             || 0,
        clicks_unique:      Number(r.clicks_unique)      || 0,
        conversions:        Number(r.conversions)        || 0,
        subscriptions:      Number(r.subscriptions)      || 0,
        passback:           Number(r.passback)           || 0,
        cpm:                Number(r.cpm)                || 0,
        cpc:                Number(r.cpc)                || 0,
        cpa:                Number(r.cpa)                || 0,
        amount:             Number(r.amount)             || 0,
        amount_pub:         Number(r.amount_pub)         || 0,
      };
    }

    function videoRow(e) {
      return {
        starts:         Number(e.start ?? e.impressions) || 0,
        first_quartile: Number(e.firstQuartile)          || 0,
        midpoint:       Number(e.midpoint)               || 0,
        third_quartile: Number(e.thirdQuartile)          || 0,
        complete:       Number(e.complete)               || 0,
      };
    }

    // ── index raw data ────────────────────────────────────────────────────────
    const siteIdx   = {};   // "date|siteId"
    const zoneIdx   = {};   // "date|zoneId"
    const adIdx     = {};   // "date|adId"
    const eventsIdx = {};   // "adId"

    for (const r of resSite.data)   siteIdx[`${r.dimension}|${id2(r)}`]  = r;
    for (const r of resZone.data)   zoneIdx[`${r.dimension}|${id2(r)}`]  = r;
    for (const r of resAd.data)     adIdx[`${r.dimension}|${id2(r)}`]    = r;
    for (const e of resEvents.data) eventsIdx[String(e.iddimension_2 ?? e.iddimension ?? '')] = e;

    // ── collect unique dates & ids ────────────────────────────────────────────
    const dates = [...new Set([
      ...resSite.data.map(r => r.dimension),
      ...resZone.data.map(r => r.dimension),
      ...resAd.data.map(r  => r.dimension),
    ])].sort();

    const siteIds = [...new Set(resSite.data.map(r => id2(r)))].filter(Boolean);
    const zoneIds = [...new Set(resZone.data.map(r => id2(r)))].filter(Boolean);
    const adIds   = [...new Set(resAd.data.map(r   => id2(r)))].filter(Boolean);

    // ── build hierarchy ───────────────────────────────────────────────────────
    const sites = siteIds.map(siteId => {
      const days = dates.map(date => {
        const sr = siteIdx[`${date}|${siteId}`];

        const zones = zoneIds.map(zoneId => {
          const zr = zoneIdx[`${date}|${zoneId}`];

          const ads = adIds.map(adId => {
            const ar = adIdx[`${date}|${adId}`];
            const ev = eventsIdx[String(adId)];
            if (!ar) return null;
            return {
              ad_id:   adId,
              ad_name: dim2(ar),
              stats:   statsRow(ar),
              video:   ev ? videoRow(ev) : null,
            };
          }).filter(Boolean);

          if (!zr && ads.length === 0) return null;
          return {
            zone_id:   zoneId,
            zone_name: zr ? dim2(zr) : null,
            stats:     zr ? statsRow(zr) : null,
            ads,
          };
        }).filter(Boolean);

        if (!sr && zones.length === 0) return null;
        return {
          date,
          stats:  sr ? statsRow(sr) : null,
          zones,
        };
      }).filter(Boolean);

      const siteRef = resSite.data.find(r => id2(r) === siteId);
      return {
        site_id:   siteId,
        site_name: siteRef ? dim2(siteRef) : null,
        days,
      };
    });

    res.json({
      campaign_id: Number(id),
      dateBegin,
      dateEnd,
      sites,
    });

  } catch (err) {
    if (err.response) return res.status(err.response.status).json(err.response.data);
    res.status(500).json({ error: err.message });
  }
});

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
