import { TopCoinService } from '../services/TopCoinService.js';

/**
 * TopCoinController - xử lý HTTP request/response cho mart_top_coins
 *
 * API 1: GET /api/top-coins          → Danh sách top coins (symbol_code, long_term_score, signal, total_num_trades)
 * API 2: GET /api/top-coins/:symbol_code → Chi tiết 1 symbol (thêm volume, ratios, money_flow, whale_buy_ratio)
 */
export class TopCoinController {
    static async getTopCoins(req, res) {
        try {
            const data = await TopCoinService.getTopCoins();
            res.json({ success: true, data });
        } catch (error) {
            res.status(400).json({ success: false, message: error.message });
        }
    }

    static async getTopCoinDetail(req, res) {
        try {
            const { symbol_code } = req.params;
            const data = await TopCoinService.getTopCoinDetail(symbol_code);
            res.json({ success: true, data });
        } catch (error) {
            res.status(400).json({ success: false, message: error.message });
        }
    }
}
