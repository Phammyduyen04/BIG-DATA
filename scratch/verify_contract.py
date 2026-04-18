import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import unittest
from unittest.mock import MagicMock
from consumer_to_minio import BufferManager

class TestContractRestoration(unittest.TestCase):
    def setUp(self):
        # Mock S3 client as it's required by BufferManager
        self.mock_s3 = MagicMock()
        self.bucket = "test-bucket"
        self.mgr = BufferManager(self.mock_s3, self.bucket, 100, 60)

    def test_ticker_contract_restoration(self):
        """Test if ticker snake_case fields are restored to camelCase."""
        raw_msg = {
            "symbol": "BTCUSDT",
            "price_change": 100.5,
            "price_change_pct": 1.2,
            "weighted_avg_price": 40000.0,
            "prev_close_price": 39900.0,
            "last_price": 40100.0,
            "last_qty": 0.5,
            "bid_price": 40090.0,
            "ask_price": 40110.0,
            "open_price": 39500.0,
            "high_price": 40500.0,
            "low_price": 39000.0,
            "volume": 1500.0,
            "quote_volume": 60000000.0,
            "num_trades": 5000,
            "event_time": 1713430000000
        }
        
        restored = self.mgr._restore_contract("binance.ticker.raw", raw_msg)
        
        # Verify specific critical fields
        self.assertEqual(restored["priceChange"], 100.5)
        self.assertEqual(restored["priceChangePercent"], 1.2)
        self.assertEqual(restored["lastPrice"], 40100.0)
        self.assertEqual(restored["count"], 5000) # num_trades -> count
        self.assertEqual(restored["quoteVolume"], 60000000.0)
        
        # Verify old keys are gone
        self.assertNotIn("price_change", restored)
        self.assertNotIn("num_trades", restored)
        
        print("✅ Ticker contract restoration verified.")

    def test_kline_contract_restoration(self):
        """Test if kline fields are restored."""
        raw_msg = {
            "symbol": "BTCUSDT",
            "taker_buy_quote_volume": 200.5,
            "event_time": 1713430000000
        }
        
        restored = self.mgr._restore_contract("binance.kline.1m.raw", raw_msg)
        
        self.assertEqual(restored["taker_buy_quote_vol"], 200.5)
        self.assertNotIn("taker_buy_quote_volume", restored)
        
        print("✅ Kline contract restoration verified.")

if __name__ == "__main__":
    unittest.main()
