import { getPrismaClient } from '../config/database.js';

/**
 * MarketRepository - truy vấn dữ liệu market từ database qua Prisma
 * Tách riêng 3 nguồn dữ liệu: Ticker24h, Klines, Trades
 */
export class MarketRepository {
    // ──────────────────────────────────────────────
    // Helper: resolve symbol_code → symbol_id
    // ──────────────────────────────────────────────

    static async _resolveSymbol(symbolCode) {
        const prisma = getPrismaClient();
        const symbol = await prisma.dimSymbol.findUnique({
            where: { symbol_code: symbolCode },
        });
        return symbol; // null nếu không tồn tại
    }

    // ──────────────────────────────────────────────
    // 1. Ticker 24h - snapshot mới nhất
    // ──────────────────────────────────────────────

    /**
     * Lấy snapshot ticker 24h mới nhất cho 1 symbol
     * Trả về: { symbol_code, last_price, price_change, price_change_percent, high_price, low_price, volume_base_24h, volume_quote_24h, trade_count }
     */
    static async getTicker24h(symbolCode) {
        const symbol = await this._resolveSymbol(symbolCode);
        if (!symbol) return null;

        const prisma = getPrismaClient();
        const ticker = await prisma.factTicker24hSnapshot.findFirst({
            where: { symbol_id: symbol.symbol_id },
            orderBy: { snapshot_time: 'desc' },
        });

        if (!ticker) return null;

        return {
            symbol_code: symbolCode,
            last_price: ticker.last_price,
            price_change: ticker.price_change,
            price_change_percent: ticker.price_change_percent,
            high_price: ticker.high_price,
            low_price: ticker.low_price,
            volume_base_24h: ticker.volume_base_24h,
            volume_quote_24h: ticker.volume_quote_24h,
            trade_count: ticker.trade_count,
            snapshot_time: ticker.snapshot_time,
        };
    }

    // ──────────────────────────────────────────────
    // 2. Klines - dữ liệu nến
    // ──────────────────────────────────────────────

    /**
     * Lấy danh sách klines cho 1 symbol + interval trong khoảng thời gian
     * Giống TradingView: trả TẤT CẢ nến trong [startTime, endTime], không limit
     *
     * @param {string} symbolCode - VD: "BTCUSDT"
     * @param {string} intervalCode - VD: "1m", "5m", "15m", "30m", "1h", "4h", "1d"
     * @param {Date} startTime - thời gian bắt đầu trục (bắt buộc)
     * @param {Date} endTime - thời gian kết thúc trục (bắt buộc)
     * Trả về mảng: [{ interval_code, open_time, close_time, open, high, low, close, volume_base }]
     */
    static async getKlines(symbolCode, intervalCode, startTime, endTime) {
        const symbol = await this._resolveSymbol(symbolCode);
        if (!symbol) return [];

        const prisma = getPrismaClient();

        const klines = await prisma.factKline.findMany({
            where: {
                symbol_id: symbol.symbol_id,
                interval_code: intervalCode,
                open_time: {
                    gte: new Date(startTime),
                    lte: new Date(endTime),
                },
            },
            orderBy: { open_time: 'asc' },
            select: {
                interval_code: true,
                open_time: true,
                close_time: true,
                open_price: true,
                high_price: true,
                low_price: true,
                close_price: true,
                volume_base: true,
            },
        });

        // Map tên fields cho frontend dễ dùng
        return klines.map((k) => ({
            interval_code: k.interval_code,
            open_time: k.open_time,
            close_time: k.close_time,
            open: k.open_price,
            high: k.high_price,
            low: k.low_price,
            close: k.close_price,
            volume_base: k.volume_base,
        }));
    }

    // ──────────────────────────────────────────────
    // 3. Recent Trades - giao dịch gần đây
    // ──────────────────────────────────────────────

    /**
     * Lấy danh sách giao dịch gần nhất cho 1 symbol
     * @param {string} symbolCode
     * @param {number} limit - số lượng trades trả về (default 50)
     * Trả về mảng: [{ trade_time, price, qty_base, quote_qty, is_buyer_maker }]
     */
    static async getRecentTrades(symbolCode, limit = 50) {
        const symbol = await this._resolveSymbol(symbolCode);
        if (!symbol) return [];

        const prisma = getPrismaClient();

        const trades = await prisma.factRawTrade.findMany({
            where: { symbol_id: symbol.symbol_id },
            orderBy: { trade_time: 'desc' },
            take: limit,
            select: {
                trade_time: true,
                price: true,
                qty_base: true,
                quote_qty: true,
                is_buyer_maker: true,
            },
        });

        return trades;
    }

    // ──────────────────────────────────────────────
    // Utility: lấy danh sách symbols và intervals
    // ──────────────────────────────────────────────

    static async getAvailableSymbols() {
        const prisma = getPrismaClient();
        const symbols = await prisma.dimSymbol.findMany({
            select: {
                symbol_code: true,
                base_asset: true,
                quote_asset: true,
            },
            orderBy: { symbol_code: 'asc' },
        });
        return symbols;
    }

    static async getLatestKlineTime(symbolCode, intervalCode) {
        const symbol = await this._resolveSymbol(symbolCode);
        if (!symbol) return null;

        const prisma = getPrismaClient();
        const latest = await prisma.factKline.findFirst({
            where: { symbol_id: symbol.symbol_id, interval_code: intervalCode },
            orderBy: { open_time: 'desc' },
            select: { open_time: true },
        });
        return latest?.open_time ?? null;
    }

    static async getAvailableIntervals(symbolCode) {
        const symbol = await this._resolveSymbol(symbolCode);
        if (!symbol) return [];

        const prisma = getPrismaClient();
        const intervals = await prisma.factKline.findMany({
            where: { symbol_id: symbol.symbol_id },
            distinct: ['interval_code'],
            select: { interval_code: true },
            orderBy: { interval_code: 'asc' },
        });

        return intervals.map((i) => i.interval_code);
    }
}
