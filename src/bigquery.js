const { BigQuery } = require('@google-cloud/bigquery');

let _bq = null;

function getBQ() {
  if (_bq) return _bq;

  const keyRaw = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!keyRaw) throw new Error('GOOGLE_SERVICE_ACCOUNT_KEY not set');

  const credentials = typeof keyRaw === 'string' ? JSON.parse(keyRaw) : keyRaw;

  _bq = new BigQuery({
    projectId: process.env.GOOGLE_PROJECT_ID,
    credentials,
  });

  return _bq;
}

async function insertViewability(row) {
  const dataset = process.env.BIGQUERY_DATASET || 'adserver';
  const table   = process.env.BIGQUERY_TABLE   || 'adserver-data';
  await getBQ().dataset(dataset).table(table).insert([row]);
}

async function insertVastEvent(row) {
  const dataset = process.env.BIGQUERY_DATASET    || 'adserver';
  const table   = process.env.BIGQUERY_VAST_TABLE || 'vast-events';
  await getBQ().dataset(dataset).table(table).insert([row]);
}

/**
 * Query viewability consolidated data for a campaign.
 * Table: go-on-adgroup.adserver.adserver_viewabitliy_consolidado
 */
async function queryViewability({ campaignId, dateBegin, dateEnd }) {
  const project = process.env.GOOGLE_PROJECT_ID || 'go-on-adgroup';
  const query = `
    SELECT
      CAST(Dia AS STRING) AS Dia,
      Campanha_ID,
      Site_ID,
      Zone_ID,
      AD_ID,
      country,
      region,
      SUM(Viewable) AS Viewable
    FROM \`${project}.adserver.adserver_viewabitliy_consolidado\`
    WHERE Campanha_ID = @campaignId
      AND Dia BETWEEN @dateBegin AND @dateEnd
    GROUP BY Dia, Campanha_ID, Site_ID, Zone_ID, AD_ID, country, region
    ORDER BY Dia
  `;

  const [rows] = await getBQ().query({
    query,
    params: {
      campaignId: String(campaignId),
      dateBegin: String(dateBegin),
      dateEnd: String(dateEnd),
    },
  });

  return rows;
}

/**
 * Query VAST consolidated data for a campaign.
 * Table: go-on-adgroup.adserver.vast_consolidado
 */
async function queryVast({ campaignId, dateBegin, dateEnd }) {
  const project = process.env.GOOGLE_PROJECT_ID || 'go-on-adgroup';
  const query = `
    SELECT
      CAST(Dia AS STRING) AS Dia,
      Campanha_ID,
      Site_ID,
      Zone_ID,
      AD_ID,
      country,
      region,
      SUM(Impression) AS Impression
    FROM \`${project}.adserver.vast_consolidado\`
    WHERE Campanha_ID = @campaignId
      AND Dia BETWEEN @dateBegin AND @dateEnd
    GROUP BY Dia, Campanha_ID, Site_ID, Zone_ID, AD_ID, country, region
    ORDER BY Dia
  `;

  const [rows] = await getBQ().query({
    query,
    params: {
      campaignId: String(campaignId),
      dateBegin: String(dateBegin),
      dateEnd: String(dateEnd),
    },
  });

  return rows;
}

module.exports = { insertViewability, insertVastEvent, queryViewability, queryVast };
