import express from 'express';
import { TopCoinController } from '../controllers/TopCoinController.js';

const router = express.Router();

// Danh sách top coins — symbol_code, long_term_score, signal, total_num_trades
router.get('/', TopCoinController.getTopCoins);

// Chi tiết 1 symbol — thêm total_volume_quote, avg_taker_buy_ratio, bullish_ratio, volume_trend, money_flow, whale_buy_ratio
router.get('/:symbol_code', TopCoinController.getTopCoinDetail);

export default router;
