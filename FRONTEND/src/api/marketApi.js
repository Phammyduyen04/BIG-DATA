import axios from 'axios';

const BASE = 'http://localhost:3000/api/market';

export const getSymbols = () => axios.get(`${BASE}/symbols`).then(r => r.data.data);

export const getIntervals = () => axios.get(`${BASE}/intervals`).then(r => r.data.data);

export const getTicker24h = (symbol) =>
  axios.get(`${BASE}/${symbol}/ticker24h`).then(r => r.data.data);

export const getKlines = (symbol, interval, startTime, endTime) =>
  axios.get(`${BASE}/${symbol}/klines`, {
    params: { interval_code: interval, startTime, endTime },
  }).then(r => r.data.data);

export const getTrades = (symbol, limit = 50) =>
  axios.get(`${BASE}/${symbol}/trades`, { params: { limit } }).then(r => r.data.data);

export const getKlineLatestTime = (symbol, interval) =>
  axios.get(`${BASE}/${symbol}/klines/latest-time`, { params: { interval_code: interval } }).then(r => r.data.data);

export const getKlinesCount = (symbol, interval, startTime, endTime) =>
  axios.get(`${BASE}/${symbol}/klines/count`, {
    params: { interval_code: interval, startTime, endTime },
  }).then(r => r.data.data);

const TOP_COINS_BASE = 'http://localhost:3000/api/top-coins';
export const getTopCoins = () => axios.get(TOP_COINS_BASE).then(r => r.data.data);
export const getTopCoinDetail = (symbol) => axios.get(`${TOP_COINS_BASE}/${symbol}`).then(r => r.data.data);
