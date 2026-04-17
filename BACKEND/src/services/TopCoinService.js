import { TopCoinRepository } from '../repositories/TopCoinRepository.js';

/**
 * TopCoinService - business logic cho mart_top_coins
 */
export class TopCoinService {
    static async getTopCoins() {
        const topCoins = await TopCoinRepository.getTopCoins();
        if (!topCoins || topCoins.length === 0) {
            throw new Error('Không tìm thấy dữ liệu top coins');
        }
        return topCoins;
    }

    static async getTopCoinDetail(symbolCode) {
        if (!symbolCode) {
            throw new Error('symbol_code là bắt buộc');
        }
        const detail = await TopCoinRepository.getTopCoinDetail(symbolCode);
        if (!detail) {
            throw new Error(`Không tìm thấy dữ liệu cho symbol: ${symbolCode}`);
        }
        return detail;
    }
}
