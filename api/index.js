require('dotenv').config();
const express = require('express');
const { proxy, adserver } = require('../src/proxy');
const { insertViewability } = require('../src/bigquery');

const app = express();

// CORS — allow any origin (must come before all routes)
app.use((req, res, next) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

app.use(express.json({ limit: '20mb' }));

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
// List zones of a site (shortcut for GET /zones?idsite=:id)
app.get('/sites/:id/zones', (req, res) =>
  proxy(req, res, { path: '/zone', params: { idsite: req.params.id } })
);

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

// ── Viewability script (served as JS) ────────────────────────────────────────
// GET /v.js?z=:zoneId — serves the tracking script for a given zone
app.get('/v.js', (req, res) => {
  const zoneId = req.query.z;
  if (!zoneId) return res.status(400).type('application/javascript').send('/* missing ?z= */');

  const beaconUrl = process.env.API_BASE_URL || `${req.protocol}://${req.get('host')}`;

  const js = `(function(){
var z='${zoneId}',B='${beaconUrl}/viewability',T=0.5,M=1000;
if(!window.IntersectionObserver)return;
var tm=null,st=null,fired=false;
function send(v,p,ms){
  if(fired)return;fired=true;
  var d=JSON.stringify({zone:z,url:location.href,referrer:document.referrer||null,user_agent:navigator.userAgent,viewed:v,visible_pct:Math.round(p*100),elapsed_ms:ms,ts:new Date().toISOString()});
  if(navigator.sendBeacon){navigator.sendBeacon(B,new Blob([d],{type:'application/json'}));}
  else{var x=new XMLHttpRequest();x.open('POST',B,true);x.setRequestHeader('Content-Type','application/json');x.send(d);}
}
var obs=new IntersectionObserver(function(es){
  var e=es[0];
  if(e.isIntersecting&&e.intersectionRatio>=T){if(!tm){st=Date.now();tm=setTimeout(function(){send(true,e.intersectionRatio,Date.now()-st);},M);}}
  else{if(tm){clearTimeout(tm);tm=null;}}
},{threshold:[0,T,1]});
function findEl(){
  // 1) div criado pelo adserver: class="... zid-{z} ..."
  var byClass=document.querySelector('.zid-'+z);
  if(byClass)return byClass;
  // 2) ins original (antes de ser processado)
  var ins=document.querySelector('ins[data-zone="'+z+'"]');
  if(ins)return ins;
  // 3) fallback pelo id original
  return document.getElementById('goon-zone-'+z);
}
var attempts=0,t=setInterval(function(){
  attempts++;
  var el=findEl();
  if(el){clearInterval(t);obs.observe(el);return;}
  if(attempts>=30)clearInterval(t);
},200);
})();`;

  res.set('Cache-Control', 'public, max-age=3600');
  res.type('application/javascript').send(js);
});

// ── Zone tag generator ────────────────────────────────────────────────────────
// GET /zones/:id/tag?type=normal|iframe|amp|prebid|email
// - type=normal (default): Standard tag with viewability tracking injected
// - type=iframe|amp|prebid|email: raw code from adserver API as-is
app.get('/zones/:id/tag', async (req, res) => {
  const zoneId = req.params.id;
  const type   = req.query.type || 'normal';
  const base   = process.env.API_BASE_URL || `${req.protocol}://${req.get('host')}`;

  let zoneData = null;
  try {
    const zr = await adserver.get(`/zone/${zoneId}`);
    zoneData = zr.data;
  } catch (_) {}

  if (type === 'normal') {
    // Standard tag: use adserver's "normal" code but inject viewability script
    const zoneName   = zoneData?.name   || '';
    const zoneWidth  = zoneData?.width  || '';
    const zoneHeight = zoneData?.height || '';
    const label = [zoneName, zoneWidth && zoneHeight ? `${zoneWidth}x${zoneHeight}` : ''].filter(Boolean).join(' / ');
    const tag = `<!-- Goonadgroup's Ad Server${label ? ' / ' + label : ''} --><ins class="ins-zone" data-zone="${zoneId}" id="goon-zone-${zoneId}"></ins><script data-cfasync="false" async src="https://media.aso1.net/js/code.min.js"></script><script async src="${base}/v.js?z=${zoneId}"></script><!-- /Goonadgroup's Ad Server -->`;
    return res.type('text/plain').send(tag);
  }

  // For all other types: return the raw code from the adserver API
  const codeEntry = (zoneData?.code || []).find(c => c.id === type);
  if (!codeEntry) {
    return res.status(404).type('text/plain').send(`Tag type "${type}" not found for zone ${zoneId}.`);
  }
  res.type('text/plain').send(codeEntry.code);
});

// ── Workflow guide (4 steps) ──────────────────────────────────────────────────
// GET /workflow/setup — returns the full setup guide with all required endpoints
app.get('/workflow/setup', (req, res) => {
  const base = process.env.API_BASE_URL || `${req.protocol}://${req.get('host')}`;
  res.json({
    description: 'Follow these 4 steps to create a site, zones, campaign and ads.',
    steps: [
      {
        step: 1,
        title: 'Create a Site',
        description: 'A site is a collection of zones. Each site must have at least one zone.',
        endpoints: {
          list_sites:     { method: 'GET',    url: `${base}/sites` },
          create_site:    { method: 'POST',   url: `${base}/sites`,
            body_example: { name: 'My Site', url: 'https://example.com', idcategory: 1, idpublisher: 123 } },
          get_site:       { method: 'GET',    url: `${base}/sites/:id` },
          update_site:    { method: 'PUT',    url: `${base}/sites/:id` },
          delete_site:    { method: 'DELETE', url: `${base}/sites/:id` },
          available_categories: { method: 'GET', url: `${base}/dict`, note: 'Check site_categories field' },
        },
      },
      {
        step: 2,
        title: 'Create Zones and Get Ad Tags',
        description: 'A zone is an area on a site where ads are displayed. After creating a zone, get the ready-to-paste HTML tag.',
        endpoints: {
          list_zones:     { method: 'GET',    url: `${base}/sites/:siteId/zones` },
          create_zone:    { method: 'POST',   url: `${base}/zones?idsite=:siteId&idformat=:formatId`,
            body_example: { name: 'Banner 300x250', idsize: 2, is_active: true } },
          get_zone:       { method: 'GET',    url: `${base}/zones/:id` },
          update_zone:    { method: 'PUT',    url: `${base}/zones/:id` },
          delete_zone:    { method: 'DELETE', url: `${base}/zones/:id` },
          get_tag:        { method: 'GET',    url: `${base}/zones/:id/tag`,
            note: 'Returns the instrumented HTML tag with viewability tracking. Paste into the publisher site.' },
          available_formats: { method: 'GET', url: `${base}/dict`, note: 'Check zone_formats field' },
        },
      },
      {
        step: 3,
        title: 'Create a Campaign',
        description: 'A campaign is a set of related ads. Each campaign must have at least one ad.',
        endpoints: {
          list_campaigns:  { method: 'GET',    url: `${base}/campaigns` },
          create_campaign: { method: 'POST',   url: `${base}/campaigns`,
            body_example: { name: 'My Campaign', idadvertiser: 123, idpricemodel: 1, rate: 1.5, start_date: '2026-01-01', finish_date: '2026-12-31' } },
          get_campaign:    { method: 'GET',    url: `${base}/campaigns/:id` },
          update_campaign: { method: 'PUT',    url: `${base}/campaigns/:id` },
          delete_campaign: { method: 'DELETE', url: `${base}/campaigns/:id` },
          price_models: { '1': 'CPM', '2': 'CPC', '3': 'CPA', '4': 'CPUC', '5': 'CPUM', '6': 'CPV' },
        },
      },
      {
        step: 4,
        title: 'Create Ads and Assign to Zones',
        description: 'An ad is the visual/audio content. The ad format must match the zone format.',
        endpoints: {
          list_ads:        { method: 'GET',    url: `${base}/campaigns/:campaignId/ads` },
          create_ad:       { method: 'POST',   url: `${base}/ads?idformat=:formatId`,
            body_example: { idcampaign: 123, name: 'My Ad', url: 'https://example.com', is_active: true, details: { idsize: 2, file: '<base64-encoded-file>' } } },
          get_ad:          { method: 'GET',    url: `${base}/ads/:id` },
          update_ad:       { method: 'PUT',    url: `${base}/ads/:id` },
          delete_ad:       { method: 'DELETE', url: `${base}/ads/:id` },
          assign_ad_to_zones: { method: 'POST', url: `${base}/ads/assign?id=:adId`,
            body_example: { zones: [160272, 160277] },
            note: 'Assign one ad to one or more zones. Formats must match.' },
          assign_zone_to_ads: { method: 'POST', url: `${base}/zones/assign?id=:zoneId`,
            body_example: { ads: [315734, 315735] },
            note: 'Alternatively, assign one zone to one or more ads.' },
          available_formats: { method: 'GET', url: `${base}/dict`, note: 'Check ad_formats field' },
        },
      },
    ],
    bonus: {
      campaign_report: { method: 'GET', url: `${base}/campaigns/:id/report?dateBegin=YYYY-MM-DD&dateEnd=YYYY-MM-DD`,
        note: 'Full report: Campaign → Site → Zone → Ad by day with all metrics.' },
    },
  });
});

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
app.post('/viewability', async (req, res) => {
  res.sendStatus(204); // responde imediatamente, não bloqueia o publisher

  const { zone, url, referrer, user_agent, viewed, visible_pct, elapsed_ms, ts } = req.body || {};

  // IP: X-Forwarded-For (Vercel/proxies) ou socket remoto
  const ip = (req.headers['x-forwarded-for'] || '').split(',')[0].trim()
          || req.socket.remoteAddress
          || null;

  // Resolve geo via ip-api.com (gratuito, sem key, 45 req/min)
  let geo = null;
  if (ip && ip !== '127.0.0.1' && ip !== '::1') {
    try {
      const axios  = require('axios');
      const geoRes = await axios.get(`http://ip-api.com/json/${ip}`, {
        params: { fields: 'country,countryCode,regionName,city,lat,lon,isp', lang: 'pt-BR' },
      });
      if (geoRes.data.country) geo = geoRes.data;
    } catch (_) { /* geo opcional, não quebra o fluxo */ }
  }

  const row = {
    zone:         String(zone ?? ''),
    url:          String(url  ?? ''),
    referrer:     referrer    || null,
    user_agent:   user_agent  || null,
    viewed:       !!viewed,
    visible_pct:  Number(visible_pct) || 0,
    elapsed_ms:   Number(elapsed_ms)  || 0,
    ts:           ts || new Date().toISOString(),
    ip:           ip || null,
    country:      geo?.country      || null,
    country_code: geo?.countryCode  || null,
    region:       geo?.regionName   || null,
    city:         geo?.city         || null,
    lat:          geo?.lat          ?? null,
    lon:          geo?.lon          ?? null,
    isp:          geo?.isp          || null,
  };

  console.log(JSON.stringify({ event: 'viewability', ...row }));

  try {
    await insertViewability(row);
  } catch (err) {
    console.error('BigQuery insert error:', err.message);
  }
});

// ── Health ───────────────────────────────────────────────────────────────────
app.get('/', (req, res) => res.json({ status: 'ok', version: '1.0.0' }));

const PORT = process.env.PORT || 4000;
if (require.main === module) {
  app.listen(PORT, () => console.log(`Server running on http://localhost:${PORT}`));
}

module.exports = app;
