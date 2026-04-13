# Binance Data Streamer

Thu thập dữ liệu real-time và historical từ Binance cho **100 đồng crypto phổ biến nhất** (theo 24h volume), kết hợp REST API và WebSocket đồng thời.

---

## Dữ liệu thu thập

| Loại | Nguồn | Tần suất | Lưu tại |
|------|-------|----------|---------|
| **Klines 1m** | REST (historical) + WebSocket (real-time) | Mỗi phút | `data/klines/` |
| **Order Book** | REST (snapshot) + WebSocket (top-20, 1s) | Mỗi giây | `data/orderbook/` |
| **Ticker 24h** | REST (snapshot) + WebSocket (rolling) | Real-time | `data/ticker24h/` |

Định dạng lưu: **JSON Lines** (`.jsonl`) – mỗi dòng là 1 JSON object hoàn chỉnh.

---

## Cấu trúc project

```
DATA-STREAMING/
├── .env                  # API keys (không commit)
├── .gitignore
├── requirements.txt
├── config.py             # Cấu hình tập trung
├── main.py               # Entry point
├── rest_collector.py     # Lấy dữ liệu historical qua REST API
├── ws_collector.py       # Stream real-time qua WebSocket
├── storage.py            # Ghi dữ liệu ra file JSONL
├── data/
│   ├── klines/           # <SYMBOL>_klines_1m.jsonl   (100 files)
│   ├── orderbook/        # <SYMBOL>_orderbook.jsonl   (100 files)
│   └── ticker24h/        # <SYMBOL>_ticker24h.jsonl   (100 files)
└── logs/                 # Log file theo ngày
```

---

## Cài đặt

```bash
pip install -r requirements.txt
```

Tạo file `.env`:

```env
BINANCE_API_KEY=your_api_key
BINANCE_SECRET_KEY=your_secret_key
```

---

## Chạy

```bash
python main.py
```

Streamer sẽ tự dừng sau **24 giờ**. Nhấn `Ctrl+C` để dừng sớm.

### Tuỳ chỉnh trong `config.py`

| Tham số | Mặc định | Mô tả |
|---------|----------|-------|
| `TOP_N_SYMBOLS` | `100` | Số lượng symbols muốn stream |
| `KLINE_INTERVAL` | `1m` | Timeframe nến |
| `KLINES_LOOKBACK_LIMIT` | `1440` | Số nến lịch sử (1440 = 24h) |
| `DEPTH_LEVELS` | `20` | Số mức giá order book |
| `STREAM_DURATION_SECONDS` | `86400` | Thời gian chạy (giây) |
| `REST_REFRESH_INTERVAL` | `60` | Chu kỳ refresh REST snapshot (giây) |

---

## Kiến trúc

```
Khởi động
    │
    ├─► [REST] Khám phá top 100 symbols (by 24h volume)
    │
    ├─► [REST] Fetch lịch sử 24h ──────────────────────────┐
    │         100 symbols × 1440 nến = 144,000 records     │
    │                                                       ├─► Ghi JSONL
    ├─► [WS]  2 connections × 150 streams mỗi connection   │
    │         kline_1m + depth20@1000ms + ticker            │
    │         (300 streams tổng)                           ─┘
    │
    └─► [REST] Refresh snapshot mỗi 60 giây
```

**Phân biệt historical vs real-time** qua field `source`:

| Giá trị | Ý nghĩa |
|---------|---------|
| `rest_historical` | Dữ liệu lịch sử lấy lúc khởi động |
| `rest_snapshot` | Snapshot định kỳ qua REST |
| `websocket` | Dữ liệu real-time qua WebSocket |

---

## Đọc dữ liệu

```python
import pandas as pd

# Klines
klines = pd.read_json("data/klines/BTCUSDT_klines_1m.jsonl", lines=True)

# Phân tách historical vs real-time
historical = klines[klines["source"] == "rest_historical"]
realtime   = klines[klines["source"] == "websocket"]

# Chỉ lấy nến đã đóng hoàn chỉnh
closed = klines[klines["is_closed"] == True]

# Order book
book = pd.read_json("data/orderbook/BTCUSDT_orderbook.jsonl", lines=True)

# Ticker 24h
ticker = pd.read_json("data/ticker24h/BTCUSDT_ticker24h.jsonl", lines=True)
```

---

## Ước tính dữ liệu sau 24h (100 symbols)

| Thư mục | Records | Dung lượng |
|---------|--------:|----------:|
| `klines/` | ~288,000 | ~120 MB |
| `orderbook/` | ~8,700,000 | ~13 GB |
| `ticker24h/` | ~8,700,000 | ~4 GB |

> Order book và ticker cập nhật liên tục (1 lần/giây × 100 symbols × 86,400 giây).
