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

/**
 * Insert a viewability event row into BigQuery.
 */
async function insertViewability(row) {
  const dataset = process.env.BIGQUERY_DATASET || 'adserver';
  const table   = process.env.BIGQUERY_TABLE   || 'adserver-data';
  await getBQ().dataset(dataset).table(table).insert([row]);
}

/**
 * Insert a VAST tracking event row into BigQuery.
 * Table: vast-events
 * Schema: event STRING, zone_id STRING, ad_id STRING, campaign_id STRING,
 *         site_id STRING, ip STRING, country STRING, country_code STRING,
 *         region STRING, city STRING, lat FLOAT64, lon FLOAT64, isp STRING,
 *         ts TIMESTAMP
 */
async function insertVastEvent(row) {
  const dataset = process.env.BIGQUERY_DATASET    || 'adserver';
  const table   = process.env.BIGQUERY_VAST_TABLE || 'vast-events';
  await getBQ().dataset(dataset).table(table).insert([row]);
}

module.exports = { insertViewability, insertVastEvent };
