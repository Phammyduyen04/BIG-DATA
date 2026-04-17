import { MarketService } from '../services/MarketService.js';

/**
 * MarketController - xử lý HTTP request/response cho 3 API chính
 *
 * API 1: GET /api/market/:symbol_code/ticker24h  → Header (last_price, 24h change, high, low, volume)
 * API 2: GET /api/market/:symbol_code/klines      → Biểu đồ nến (interval_code, open_time, OHLCV)
 * API 3: GET /api/market/:symbol_code/trades       → Giao dịch gần đây (trade_time, price, qty, is_buyer_maker)
 */
export class MarketController {
    // ──────────────────────────────────────────────
    // API 1: Ticker 24h → Header
    // ──────────────────────────────────────────────

    /**
     * GET /api/market/:symbol_code/ticker24h
     *
     * Response:
     * {
     *   symbol_code: "BTCUSDT",
     *   last_price: 84210,
     *   price_change: 1760,
     *   price_change_percent: 2.13,
     *   high_price: 84800,
     *   low_price: 82950,
     *   volume_base_24h: ...,
     *   volume_quote_24h: ...,
     *   trade_count: ...
     * }
     */
    static async getTicker24h(req, res) {
        try {
            const { symbol_code } = req.params;
            const data = await MarketService.getTicker24h(symbol_code);

            res.json({
                success: true,
                data,
            });
        } catch (error) {
            res.status(400).json({
                success: false,
                message: error.message,
            });
        }
    }

    // ──────────────────────────────────────────────
    // API 2: Klines → Biểu đồ nến
    // ──────────────────────────────────────────────

    /**
     * GET /api/market/:symbol_code/klines?interval_code=1h&startTime=...&endTime=...
     *
     * Giống TradingView: trả TẤT CẢ nến trong [startTime, endTime]
     * Frontend gửi startTime/endTime khi user pan/zoom chart
     *
     * Response:
     * {
     *   data: [
     *     { interval_code: "1h", open_time: "...", close_time: "...", open: 84000, high: 84800, low: 83500, close: 84210, volume_base: ... },
     *     ...
     *   ]
     * }
     */
    static async getKlines(req, res) {
        try {
            const { symbol_code } = req.params;
            const { interval_code, startTime, endTime } = req.query;

            const data = await MarketService.getKlines(
                symbol_code,
                interval_code,
                startTime,
                endTime
            );

            res.json({
                success: true,
                data,
            });
        } catch (error) {
            res.status(400).json({
                success: false,
                message: error.message,
            });
        }
    }

    // ──────────────────────────────────────────────
    // API 3: Recent Trades → Giao dịch gần đây
    // ──────────────────────────────────────────────

    /**
     * GET /api/market/:symbol_code/trades?limit=50
     *
     * Response:
     * {
     *   data: [
     *     { trade_time: "...", price: 84210, qty_base: 0.5, quote_qty: 42105, is_buyer_maker: false },
     *     ...
     *   ]
     * }
     */
    static async getRecentTrades(req, res) {
        try {
            const { symbol_code } = req.params;
            const { limit } = req.query;

            const data = await MarketService.getRecentTrades(
                symbol_code,
                limit ? parseInt(limit) : undefined
            );

            res.json({
                success: true,
                data,
            });
        } catch (error) {
            res.status(400).json({
                success: false,
                message: error.message,
            });
        }
    }

    // ──────────────────────────────────────────────
    // Utility APIs
    // ──────────────────────────────────────────────

    /** GET /api/market/:symbol_code/klines/latest-time?interval_code=1m */
    static async getKlineLatestTime(req, res) {
        try {
            const { symbol_code } = req.params;
            const { interval_code } = req.query;
            const data = await MarketService.getLatestKlineTime(symbol_code, interval_code);
            res.json({ success: true, data });
        } catch (error) {
            res.status(400).json({ success: false, message: error.message });
        }
    }

    /** GET /api/market/symbols */
    static async getSymbols(req, res) {
        try {
            const data = await MarketService.getAvailableSymbols();
            res.json({ success: true, data });
        } catch (error) {
            res.status(400).json({ success: false, message: error.message });
        }
    }

    /** GET /api/market/intervals */
    static async getIntervals(req, res) {
        try {
            const data = MarketService.getValidIntervals();
            res.json({ success: true, data });
        } catch (error) {
            res.status(400).json({ success: false, message: error.message });
        }
    }

    /** GET /api/market/:symbol_code/intervals */
    static async getSymbolIntervals(req, res) {
        try {
            const { symbol_code } = req.params;
            const data = await MarketService.getAvailableIntervals(symbol_code);
            res.json({ success: true, data });
        } catch (error) {
            res.status(400).json({ success: false, message: error.message });
        }
    }

    /** GET /api/market/health */
    static async health(req, res) {
        res.json({
            success: true,
            message: 'Market API is running',
            timestamp: new Date().toISOString(),
        });
    }
}
