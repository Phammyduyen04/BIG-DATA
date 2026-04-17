import express from 'express';
import { MarketController } from '../controllers/MarketController.js';

const router = express.Router();

/**
 * Market API Routes
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │ API                                        │ Mục đích                  │
 * ├─────────────────────────────────────────────┼───────────────────────────┤
 * │ GET /api/market/health                      │ Health check              │
 * │ GET /api/market/symbols                     │ Danh sách symbols         │
 * │ GET /api/market/intervals                   │ Danh sách intervals       │
 * │ GET /api/market/:symbol_code/intervals      │ Intervals có data         │
 * │ GET /api/market/:symbol_code/ticker24h      │ ★ Header ticker 24h       │
 * │ GET /api/market/:symbol_code/klines         │ ★ Biểu đồ nến            │
 * │ GET /api/market/:symbol_code/trades         │ ★ Giao dịch gần đây      │
 * └─────────────────────────────────────────────┴───────────────────────────┘
 */

// Utility routes (không có params)
router.get('/health', MarketController.health);
router.get('/symbols', MarketController.getSymbols);
router.get('/intervals', MarketController.getIntervals);

// Symbol-specific routes
router.get('/:symbol_code/intervals', MarketController.getSymbolIntervals);

// ★ 3 API chính cho frontend
router.get('/:symbol_code/ticker24h', MarketController.getTicker24h);
router.get('/:symbol_code/klines/latest-time', MarketController.getKlineLatestTime);
router.get('/:symbol_code/klines', MarketController.getKlines);
router.get('/:symbol_code/trades', MarketController.getRecentTrades);

export default router;
