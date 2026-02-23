function gerarRelatorioAdSite() {
  const token = 'h0s21wuosAk3WT-dLUBl0HxJ9g05Qw2UDdjxgyoV';
  const diasParaTras = 366; // Define quantos dias o script deve processar
  
  const headers = { 'Authorization': 'Bearer ' + token, 'Accept': 'application/json' };
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  // 1. DICIONÁRIO DE SITES: ID -> {URL, Nome}
  const resSites = UrlFetchApp.fetch('https://api.adsrv.net/v2/site', { 'headers': headers });
  const dictSites = {};
  JSON.parse(resSites.getContentText()).forEach(s => {
    dictSites[s.id] = { url: s.url, nome: s.name }; //
  });

  // 2. LISTA DE CAMPANHAS
  const resCampanhas = UrlFetchApp.fetch('https://api.adsrv.net/v2/campaign', { 'headers': headers });
  const campanhas = JSON.parse(resCampanhas.getContentText());

  campanhas.forEach(camp => {
    // 3. DICIONÁRIO DE CRIATIVOS DA CAMPANHA: ID -> Nome
    const urlAds = `https://api.adsrv.net/v2/ad?idcampaign=${camp.id}`;
    const resAds = UrlFetchApp.fetch(urlAds, { 'headers': headers });
    const dictAds = {};
    JSON.parse(resAds.getContentText()).forEach(a => {
      dictAds[a.id] = a.name; //
    });

    let nomeAba = camp.name.substring(0, 30).replace(/[\\\/\?\*\[\]]/g, "");
    let aba = ss.getSheetByName(nomeAba) || ss.insertSheet(nomeAba);
    aba.clear();

    const cabecalho = ["Data", "Campaign", "Site (URL)", "Name", "Creative Name", "Impressions", "Views", "Cliques", "25%", "50%", "75%", "100%"];
    aba.appendRow(cabecalho);
    aba.getRange(1, 1, 1, cabecalho.length).setFontWeight("bold");

    let todasAsLinhas = [];

    // 4. LOOP POR DIA PARA CONSEGUIR 3 DIMENSÕES (Data + Ad + Site)
    for (let i = diasParaTras; i >= 0; i--) {
      let dataRef = new Date();
      dataRef.setDate(dataRef.getDate() - i);
      let dataStr = Utilities.formatDate(dataRef, "GMT", "yyyy-MM-dd");

      // Requisições agrupadas por Ad (group) e Site (group2)
      let urlStats = `https://api.adsrv.net/v2/stats?dateBegin=${dataStr}&dateEnd=${dataStr}&group=ad&group2=site&idcampaign=${camp.id}`;
      let urlEvents = `https://api.adsrv.net/v2/events?dateBegin=${dataStr}&dateEnd=${dataStr}&report=1&group=ad&group2=site&idcampaign=${camp.id}`;

      let resStats = UrlFetchApp.fetch(urlStats, { 'headers': headers, 'muteHttpExceptions': true });
      let resEvents = UrlFetchApp.fetch(urlEvents, { 'headers': headers, 'muteHttpExceptions': true });

      if (resStats.getResponseCode() === 200 && resEvents.getResponseCode() === 200) {
        let statsData = JSON.parse(resStats.getContentText());
        let eventsData = JSON.parse(resEvents.getContentText());

        // Mapa de vídeo por "AdID|SiteID"
        let videoMap = {};
        eventsData.forEach(ev => {
          videoMap[`${ev.iddimension}|${ev.iddimension2}`] = ev;
        });

        statsData.forEach(st => {
          let siteInfo = dictSites[st.iddimension2] || { url: "N/A", nome: "N/A" };
          let creativeName = dictAds[st.iddimension] || "Criativo Desconhecido";
          let v = videoMap[`${st.iddimension}|${st.iddimension2}`] || {};

          todasAsLinhas.push([
            dataStr,
            camp.name,
            siteInfo.url,
            siteInfo.nome,
            creativeName,       // Nome do Criativo (vindo do ID dimension)
            st.impressions,     // Impressions
            v.firstQuartile || 0, // Views (Mapeado para 25%)
            st.clicks,          // Cliques
            v.firstQuartile || 0, // 25%
            v.midpoint || 0,      // 50%
            v.thirdQuartile || 0, // 75%
            v.complete || 0       // 100%
          ]);
        });
      }
      Utilities.sleep(600); // Respeita o limite de 100 req/min
    }

    if (todasAsLinhas.length > 0) {
      aba.getRange(2, 1, todasAsLinhas.length, cabecalho.length).setValues(todasAsLinhas);
    }
    Logger.log(`Campanha finalizada: ${camp.name}`);
  });
}