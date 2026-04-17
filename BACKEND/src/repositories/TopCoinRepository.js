import { getPrismaClient } from '../config/database.js';

/**
 * TopCoinRepository - truy vấn dữ liệu mart_top_coins từ database qua Prisma
 */
export class TopCoinRepository {
    static async getTopCoins() {
        const prisma = getPrismaClient();

        const latest = await prisma.martTopCoin.findFirst({
            where: { timeframe: '1d' },
            orderBy: { computed_at: 'desc' },
            select: { computed_at: true },
        });

        if (!latest?.computed_at) return [];

        // Dùng range ±1s thay vì exact match để tránh lỗi microsecond precision
        // (PostgreSQL lưu microseconds, Prisma/JS chỉ giữ milliseconds)
        const t = latest.computed_at.getTime();
        return prisma.martTopCoin.findMany({
            where: {
                timeframe: '1d',
                computed_at: { gte: new Date(t - 1000), lte: new Date(t + 1000) },
            },
            orderBy: { rank: 'asc' },
            select: {
                rank: true,
                symbol_code: true,
                long_term_score: true,
                signal: true,
                total_num_trades: true,
                computed_at: true,
            },
        });
    }

    static async getTopCoinDetail(symbolCode) {
        const prisma = getPrismaClient();

        const latest = await prisma.martTopCoin.findFirst({
            where: { timeframe: '1d' },
            orderBy: { computed_at: 'desc' },
            select: { computed_at: true },
        });

        if (!latest?.computed_at) return null;

        const t = latest.computed_at.getTime();
        return prisma.martTopCoin.findFirst({
            where: {
                timeframe: '1d',
                computed_at: { gte: new Date(t - 1000), lte: new Date(t + 1000) },
                symbol_code: symbolCode,
            },
            select: {
                rank: true,
                symbol_code: true,
                long_term_score: true,
                signal: true,
                total_num_trades: true,
                total_volume_quote: true,
                avg_taker_buy_ratio: true,
                bullish_ratio: true,
                volume_trend: true,
                money_flow: true,
                whale_buy_ratio: true,
                computed_at: true,
            },
        });
    }
}
