
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import unittest
from unittest.mock import MagicMock
from consumer_to_minio import BufferManager

class TestContractRestorationV2(unittest.TestCase):
    def setUp(self):
        # Mock S3 client
        self.mock_s3 = MagicMock()
        self.bucket = "test-bucket"
        # Small batch size to trigger flush or just check internal buffer
        self.mgr = BufferManager(self.mock_s3, self.bucket, 100, 60)

    def test_ticker_normalization_and_persistence(self):
        """
        Verify ticker contract mapping AND that BufferManager persists the 
        NORMALIZED message, not the raw one.
        """
        raw_msg = {
            "symbol": "BTCUSDT",
            "price_change": 100.5,
            "num_trades": 5000,
            "event_time": 1713430000000
        }
        
        # 1. Add to buffer
        self.mgr.add("binance.ticker.raw", raw_msg)
        
        # 2. Inspect the buffer
        # Buffer keys: (topic, symbol, date, hour)
        key = ("binance.ticker.raw", "BTCUSDT", "2024-04-18", "08")
        self.assertIn(key, self.mgr._buffers)
        
        stored_msg = self.mgr._buffers[key][0]
        
        # 3. VERIFY BUG FIX: Normalized fields must be present in stored msg
        self.assertEqual(stored_msg["priceChange"], 100.5)
        self.assertEqual(stored_msg["count"], 5000)
        
        # 4. VERIFY BUG FIX: Raw fields must NOT be present in stored msg
        self.assertNotIn("price_change", stored_msg)
        self.assertNotIn("num_trades", stored_msg)
        
        print("✅ BUG FIX VERIFIED: BufferManager persists normalized ticker.")

    def test_kline_normalization_and_persistence(self):
        """Verify kline contract and persistence."""
        raw_msg = {
            "symbol": "ETHUSDT",
            "taker_buy_quote_volume": 200.5,
            "event_time": 1713430000000
        }
        
        self.mgr.add("binance.kline.1m.raw", raw_msg)
        
        key = ("binance.kline.1m.raw", "ETHUSDT", "2024-04-18", "08")
        stored_msg = self.mgr._buffers[key][0]
        
        # Specific Contract Check
        self.assertEqual(stored_msg["taker_buy_quote_vol"], 200.5)
        self.assertNotIn("taker_buy_quote_volume", stored_msg)
        
        print("✅ BUG FIX VERIFIED: BufferManager persists normalized klines.")

if __name__ == "__main__":
    unittest.main()
