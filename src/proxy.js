const axios = require('axios');

const PAGINATION_HEADERS = [
  'x-pagination-total-count',
  'x-pagination-page-count',
  'x-pagination-current-page',
  'x-pagination-per-page',
];

const RATE_LIMIT_HEADERS = [
  'x-rate-limit-limit',
  'x-rate-limit-remaining',
  'x-rate-limit-reset',
];

const FORWARD_HEADERS = [...PAGINATION_HEADERS, ...RATE_LIMIT_HEADERS];

const adserver = axios.create({
  baseURL: 'https://api.adsrv.net/v2',
  headers: {
    Authorization: `Bearer ${process.env.API_TOKEN}`,
    Accept: 'application/json',
  },
});

async function proxy(req, res, { method, path, params = {}, body } = {}) {
  try {
    const mergedParams = { ...req.query, ...params };

    const response = await adserver.request({
      method: method || req.method,
      url: path,
      params: mergedParams,
      data: body !== undefined ? body : req.body,
    });

    // Forward relevant headers
    for (const header of FORWARD_HEADERS) {
      if (response.headers[header] !== undefined) {
        res.set(header, response.headers[header]);
      }
    }

    res.status(response.status).json(response.data);
  } catch (err) {
    if (err.response) {
      res.status(err.response.status).json(err.response.data);
    } else {
      res.status(500).json({ error: err.message });
    }
  }
}

module.exports = { proxy, adserver };
