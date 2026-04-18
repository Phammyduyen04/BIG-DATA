
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import unittest
import time
from unittest.mock import MagicMock
from consumer_to_minio import BufferManager

class TestContractRestorationV3(unittest.TestCase):
    def setUp(self):
        # Mock S3 client
        self.mock_s3 = MagicMock()
        self.bucket = "test-bucket"
        self.mgr = BufferManager(self.mock_s3, self.bucket, 100, 60)

    def test_trades_normalization_and_persistence(self):
        """
        Verify trades contract mapping (event_time -> time) AND that 
        BufferManager persists the NORMALIZED message.
        """
        raw_msg = {
            "symbol": "BTCUSDT",
            "event_time": 1713430000000,
            "quote_qty": 500.5,
            "is_buyer_maker": True
        }
        
        # 1. Add to buffer
        self.mgr.add("binance.trade.raw", raw_msg)
        
        # 2. Inspect the buffer
        key = ("binance.trade.raw", "BTCUSDT", "2024-04-18", "08")
        self.assertIn(key, self.mgr._buffers)
        
        stored_msg = self.mgr._buffers[key][0]
        
        # 3. VERIFY: event_time should be remapped to time
        self.assertEqual(stored_msg["time"], 1713430000000)
        self.assertNotIn("event_time", stored_msg)
        self.assertEqual(stored_msg["quote_qty"], 500.5)
        
        print("✅ BUG FIX VERIFIED: BufferManager persists normalized trades with 'time' field.")

    def test_ticker_normalization(self):
        raw_msg = {
            "symbol": "BTCUSDT",
            "price_change": 100.5,
            "num_trades": 5000,
            "event_time": 1713430000000
        }
        self.mgr.add("binance.ticker.raw", raw_msg)
        key = ("binance.ticker.raw", "BTCUSDT", "2024-04-18", "08")
        stored_msg = self.mgr._buffers[key][0]
        self.assertEqual(stored_msg["priceChange"], 100.5)
        self.assertEqual(stored_msg["count"], 5000)
        print("✅ TICKER CONTRACT VERIFIED.")

if __name__ == "__main__":
    unittest.main()
