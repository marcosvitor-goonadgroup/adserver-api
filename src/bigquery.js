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
 * Table schema (auto-detected on first insert via insertAll):
 *   zone STRING, url STRING, referrer STRING, user_agent STRING,
 *   viewed BOOL, visible_pct INT64, elapsed_ms INT64, ts TIMESTAMP, ip STRING,
 *   country STRING, country_code STRING, region STRING, city STRING,
 *   lat FLOAT64, lon FLOAT64, isp STRING
 */
async function insertViewability(row) {
  const dataset = process.env.BIGQUERY_DATASET || 'adserver';
  const table   = process.env.BIGQUERY_TABLE   || 'adserver-data';

  const bq = getBQ();
  await bq.dataset(dataset).table(table).insert([row]);
}

module.exports = { insertViewability };
