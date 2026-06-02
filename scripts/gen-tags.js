/**
 * gen-tags.js — fetches the live tag for every zone from production and
 * builds two artifacts:
 *   1. tags-export.md   — all tags grouped by site (for republishing — step 4)
 *   2. render-test.html — a page to open in the browser to validate rendering
 *                         (display banners + a VAST video player — step 2)
 *
 * Run: node scripts/gen-tags.js
 */
const fs = require('fs');
const path = require('path');

const BASE = process.env.TAGS_BASE || 'https://api-adserver.crmaddesk.com';

// Zones grouped by site (from the /sites listing)
const SITES = [
  { name: 'Globo', id: 48136, zones: [
    { id: 160768, name: 'Banner - 300x250', fmt: 6 },
    { id: 160780, name: 'Mobile - 320x50', fmt: 6 },
    { id: 160781, name: 'VAST', fmt: 18 },
  ]},
  { name: 'goonadgroup.com', id: 47826, zones: [
    { id: 160276, name: 'Billboard - 970x250', fmt: 6 },
    { id: 160274, name: 'Half Page - 300x600', fmt: 6 },
    { id: 160275, name: 'Large Leaderboard - 970x90', fmt: 6 },
    { id: 160273, name: 'Leaderboard - 728x90', fmt: 6 },
    { id: 160277, name: 'Mobile Banner - 320x50', fmt: 6 },
    { id: 160272, name: 'MREC - 300x250', fmt: 6 },
    { id: 160315, name: 'VAST', fmt: 18 },
  ]},
  { name: 'Rodovida - Site', id: 47804, zones: [
    { id: 160233, name: 'Banner Rodovida', fmt: 6 },
    { id: 160234, name: 'GIF Animado', fmt: 6 },
    { id: 160224, name: 'Teste 00px', fmt: 21 },
    { id: 160225, name: 'Video Rodovida', fmt: 21 },
    { id: 160231, name: 'Video Rodovida', fmt: 6 },
    { id: 160226, name: 'Video VAST', fmt: 18 },
  ]},
  { name: 'Site GO ON', id: 47650, zones: [
    { id: 159988, name: 'Go on Banner GIF', fmt: 6 },
  ]},
  { name: 'Moovit Zone Test 2', id: 39674, zones: [
    { id: 135369, name: 'Banner 300x250 Test Zone 2', fmt: 6 },
    { id: 135370, name: 'Banner 320x50 Test Zone Moovit 2', fmt: 6 },
  ]},
  { name: 'Moovit Zone', id: 39439, zones: [
    { id: 134751, name: 'Banner 300x250 Test Zone Moovit', fmt: 6 },
    { id: 134728, name: 'Banner 320x50 Test Zone Moovit', fmt: 6 },
  ]},
  { name: 'Zone Test', id: 39298, zones: [
    { id: 134499, name: 'Banner 300x250 Test Zone', fmt: 6 },
  ]},
];

async function getTag(zoneId) {
  const r = await fetch(`${BASE}/zones/${zoneId}/tag`);
  return (await r.text()).trim();
}

(async () => {
  const md = ['# Tags de produção — Goonadgroup AdServer', '',
    `Geradas de \`${BASE}\` em ${new Date().toISOString()}`, '',
    '> Cole cada tag no site do publisher correspondente. As tags VAST devem ser passadas como URL ao player de vídeo.', ''];
  const displayTags = []; // for the render test page
  const vastTags = [];

  for (const site of SITES) {
    md.push(`\n## ${site.name} (site ${site.id})\n`);
    for (const z of site.zones) {
      process.stdout.write(`  fetching zona ${z.id} (${z.name})... `);
      let tag;
      try { tag = await getTag(z.id); } catch (e) { tag = `ERRO: ${e.message}`; }
      console.log('ok');
      const kind = z.fmt === 18 ? 'VAST' : z.fmt === 21 ? 'Direct link' : 'Display';
      md.push(`### ${z.name} — zona ${z.id} _(${kind})_`, '', '```html', tag, '```', '');
      if (z.fmt === 6) displayTags.push({ ...z, site: site.name, tag });
      if (z.fmt === 18) {
        const m = tag.match(/https?:\/\/\S+\/vast\?z=\d+/);
        if (m) vastTags.push({ ...z, site: site.name, url: m[0] });
      }
    }
  }

  const outDir = path.join(__dirname, '..', 'dist');
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(path.join(outDir, 'tags-export.md'), md.join('\n'), 'utf8');

  // Render test HTML
  const firstVast = vastTags[0];
  const html = `<!doctype html>
<html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Teste de Render — Goonadgroup AdServer</title>
<style>body{font-family:system-ui,Arial,sans-serif;background:#1e1f26;color:#eee;margin:0;padding:24px}
h1{font-size:20px}h2{font-size:15px;color:#9ab}.slot{border:1px dashed #556;margin:14px 0;padding:10px;background:#262833;display:inline-block;vertical-align:top}
.note{color:#8a8f9a;font-size:12px}#video{max-width:640px}</style>
<script src="//imasdk.googleapis.com/js/sdkloader/ima3.js"></script></head>
<body>
<h1>Teste de Render — AdServer (crmaddesk.com)</h1>
<p class="note">Abra o DevTools → Network e confirme: <code>media.crmaddesk.com/js/code.min.js</code> (200), o criativo renderiza, e <code>/track</code> + <code>/viewability</code> disparam para <code>api-adserver.crmaddesk.com</code>.</p>
<h2>Banners (display)</h2>
${displayTags.map(d => `<div class="slot"><div class="note">${d.site} / ${d.name} (zona ${d.id})</div>${d.tag}</div>`).join('\n')}
<h2>VAST (vídeo)${firstVast ? ` — ${firstVast.site} / zona ${firstVast.id}` : ''}</h2>
${firstVast ? `<div id="video"></div>
<button id="play">▶ Tocar anúncio VAST</button>
<script>
var VAST_URL=${JSON.stringify(firstVast.url)};
var videoEl,adDisplay,adsLoader,adsManager;
function init(){
  var c=document.getElementById('video');
  videoEl=document.createElement('video');videoEl.style.width='640px';videoEl.style.height='360px';videoEl.style.background='#000';
  c.appendChild(videoEl);
  adDisplay=new google.ima.AdDisplayContainer(c,videoEl);
  document.getElementById('play').addEventListener('click',function(){
    adDisplay.initialize();
    adsLoader=new google.ima.AdsLoader(adDisplay);
    adsLoader.addEventListener(google.ima.AdsManagerLoadedEvent.Type.ADS_MANAGER_LOADED,function(e){
      adsManager=e.getAdsManager(videoEl);
      try{adsManager.init(640,360,google.ima.ViewMode.NORMAL);adsManager.start();}catch(err){console.error(err);}
    });
    adsLoader.addEventListener(google.ima.AdErrorEvent.Type.AD_ERROR,function(e){alert('Erro VAST: '+e.getError());});
    var req=new google.ima.AdsRequest();req.adTagUrl=VAST_URL;
    req.linearAdSlotWidth=640;req.linearAdSlotHeight=360;
    adsLoader.requestAds(req);
  });
}
if(window.google&&google.ima)init();else window.addEventListener('load',init);
</script>` : '<p class="note">Nenhuma zona VAST encontrada.</p>'}
</body></html>`;
  fs.writeFileSync(path.join(outDir, 'render-test.html'), html, 'utf8');

  console.log(`\n✅ Gerado:`);
  console.log(`   dist/tags-export.md   (${SITES.reduce((n,s)=>n+s.zones.length,0)} tags)`);
  console.log(`   dist/render-test.html (${displayTags.length} banners + ${vastTags.length} VAST)`);
})();
