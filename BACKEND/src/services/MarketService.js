import { MarketRepository } from '../repositories/MarketRepository.js';

const VALID_INTERVALS = ['1m', '5m', '15m', '30m', '1h', '4h', '1d'];
const MAX_TRADE_LIMIT = 1000;

/**
 * MarketService - business logic cho 3 API chính
 */
export class MarketService {
    // ──────────────────────────────────────────────
    // 1. Ticker 24h
    // ──────────────────────────────────────────────

    /**
     * Lấy thông tin ticker 24h cho 1 symbol
     * Frontend hiển thị ở header: Last Price, 24h Change, 24h High/Low, 24h Volume
     */
    static async getTicker24h(symbolCode) {
        if (!symbolCode) {
            throw new Error('symbol_code là bắt buộc');
        }

        const ticker = await MarketRepository.getTicker24h(symbolCode);
        if (!ticker) {
            throw new Error(`Không tìm thấy dữ liệu ticker cho symbol: ${symbolCode}`);
        }

        return ticker;
    }

    // ──────────────────────────────────────────────
    // 2. Klines (Candlestick)
    // ──────────────────────────────────────────────

    /**
     * Lấy dữ liệu klines cho biểu đồ nến
     * Giống TradingView: trả tất cả nến trong [startTime, endTime]
     * Frontend gửi startTime/endTime khi user pan/zoom
     */
    static async getKlines(symbolCode, intervalCode, startTime, endTime) {
        // Validation
        if (!symbolCode) {
            throw new Error('symbol_code là bắt buộc');
        }
        if (!intervalCode) {
            throw new Error('interval_code là bắt buộc');
        }
        if (!VALID_INTERVALS.includes(intervalCode)) {
            throw new Error(
                `interval_code không hợp lệ. Chọn: ${VALID_INTERVALS.join(', ')}`
            );
        }
        if (!startTime || !endTime) {
            throw new Error('startTime và endTime là bắt buộc');
        }

        const start = new Date(startTime);
        const end = new Date(endTime);

        if (isNaN(start.getTime()) || isNaN(end.getTime())) {
            throw new Error('startTime hoặc endTime không phải ngày hợp lệ');
        }
        if (start >= end) {
            throw new Error('startTime phải nhỏ hơn endTime');
        }

        return MarketRepository.getKlines(symbolCode, intervalCode, start, end);
    }

    /**
     * Đếm số lượng klines trong khoảng thời gian
     */
    static async getKlinesCount(symbolCode, intervalCode, startTime, endTime) {
        if (!symbolCode) throw new Error('symbol_code là bắt buộc');
        if (!intervalCode || !VALID_INTERVALS.includes(intervalCode))
            throw new Error(`interval_code không hợp lệ. Chọn: ${VALID_INTERVALS.join(', ')}`);
        if (!startTime || !endTime) throw new Error('startTime và endTime là bắt buộc');

        const start = new Date(startTime);
        const end = new Date(endTime);
        if (isNaN(start.getTime()) || isNaN(end.getTime()))
            throw new Error('startTime hoặc endTime không phải ngày hợp lệ');
        if (start >= end) throw new Error('startTime phải nhỏ hơn endTime');

        return MarketRepository.getKlinesCount(symbolCode, intervalCode, start, end);
    }

    // ──────────────────────────────────────────────
    // 3. Recent Trades
    // ──────────────────────────────────────────────

    /**
     * Lấy danh sách giao dịch gần đây
     * Frontend hiển thị bên phải: trade_time, price, qty_base, quote_qty, is_buyer_maker
     */
    static async getRecentTrades(symbolCode, limit = 50) {
        if (!symbolCode) {
            throw new Error('symbol_code là bắt buộc');
        }

        limit = Math.min(Math.max(1, limit), MAX_TRADE_LIMIT);

        const trades = await MarketRepository.getRecentTrades(symbolCode, limit);
        return trades;
    }

    // ──────────────────────────────────────────────
    // Utility
    // ──────────────────────────────────────────────

    static async getLatestKlineTime(symbolCode, intervalCode) {
        if (!symbolCode) throw new Error('symbol_code là bắt buộc');
        if (!intervalCode || !VALID_INTERVALS.includes(intervalCode))
            throw new Error(`interval_code không hợp lệ`);
        return MarketRepository.getLatestKlineTime(symbolCode, intervalCode);
    }

    static getValidIntervals() {
        return VALID_INTERVALS;
    }

    static async getAvailableSymbols() {
        return MarketRepository.getAvailableSymbols();
    }

    static async getAvailableIntervals(symbolCode) {
        if (!symbolCode) {
            throw new Error('symbol_code là bắt buộc');
        }
        return MarketRepository.getAvailableIntervals(symbolCode);
    }
}
