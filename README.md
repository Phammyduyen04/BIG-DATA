# Binance Data Streaming

Dữ liệu thị trường tiền mã hóa được cào từ Binance cho **100 cặp giao dịch USDT phổ biến nhất** trong 3 tháng gần nhất.

---

## Nguồn dữ liệu

| Nguồn | Endpoint |
|---|---|
| Klines & Ticker | Binance REST API (`api.binance.com`) |
| Trades | Binance Bulk S3 (`data.binance.vision`) + fallback REST API |

---

## Cách chọn 100 symbols

- Lấy tất cả cặp giao dịch USDT đang hoạt động từ `/api/v3/exchangeInfo`
- Fetch klines theo khung `1M` (monthly) trong 3 tháng gần nhất cho từng symbol
- Tổng hợp `quoteAssetVolume` (khối lượng giao dịch quy về USDT) của 3 tháng
- Xếp hạng giảm dần → lấy top 100

---

## Scripts

| Script | Mô tả |
|---|---|
| `stream_binance.py` | Stream klines (1m, 3 tháng) + ticker_24h → lưu CSV vào `DATA/` |
| `stream_trades_bulk.py` | Download trades (7 ngày) via Bulk S3 + fallback REST → lưu CSV vào `DATA/` |

### Chạy

```bash
pip install -r requirements.txt

# Bước 1: stream klines + ticker_24h
python stream_binance.py

# Bước 2: stream trades
python stream_trades_bulk.py
```

---

## Cấu trúc thư mục DATA/

```
DATA/
├── top100_usdt_symbols.csv         # Danh sách 100 symbols đã chọn
├── active_symbols_matched.csv      # Symbols có đủ dữ liệu klines (input cho trades)
├── klines_1m_<timestamp>.csv       # Klines 1 phút, 3 tháng, 100 symbols
├── ticker_24h_<timestamp>.csv      # Thống kê 24h, 100 symbols
├── trades_<SYMBOL>_7d.csv          # Trades 7 ngày, mỗi symbol 1 file (100 files)
├── trades_symbols_matched.csv      # Symbols đã download trades thành công
├── stream.log                      # Log của stream_binance.py
└── stream_trades.log               # Log của stream_trades_bulk.py
```

---

## Mô tả dữ liệu

### 1. klines_1m_\<timestamp\>.csv

Nến (candlestick) 1 phút cho 100 symbols trong **3 tháng** (`2026-01-17` → `2026-04-18`).

| Cột | Kiểu | Mô tả |
|---|---|---|
| `symbol` | string | Tên cặp giao dịch (vd: `BTCUSDT`) |
| `open_time` | int | Thời điểm mở nến (Unix ms) |
| `open_time_dt` | string | Thời điểm mở nến (ISO 8601 UTC) |
| `open` | float | Giá mở |
| `high` | float | Giá cao nhất |
| `low` | float | Giá thấp nhất |
| `close` | float | Giá đóng |
| `volume` | float | Khối lượng giao dịch (đơn vị base, vd: BTC) |
| `close_time` | int | Thời điểm đóng nến (Unix ms) |
| `quote_asset_volume` | float | Khối lượng quy về USDT |
| `num_trades` | int | Số lệnh giao dịch trong nến |
| `taker_buy_base_vol` | float | Khối lượng mua taker (base) |
| `taker_buy_quote_vol` | float | Khối lượng mua taker (USDT) |
| `ignore` | string | Trường dự phòng của Binance |

- **Số dòng:** ~12,550,520 (100 symbols × ~91 ngày × 1,440 phút)
- **Dung lượng:** ~2.1 GB

---

### 2. ticker_24h_\<timestamp\>.csv

Thống kê rolling 24 giờ tại thời điểm chạy script, 1 dòng per symbol.

| Cột | Kiểu | Mô tả |
|---|---|---|
| `symbol` | string | Tên cặp giao dịch |
| `priceChange` | float | Thay đổi giá tuyệt đối (24h) |
| `priceChangePercent` | float | Thay đổi giá phần trăm (24h) |
| `weightedAvgPrice` | float | Giá trung bình có trọng số (24h) |
| `prevClosePrice` | float | Giá đóng cửa phiên trước |
| `lastPrice` | float | Giá giao dịch gần nhất |
| `lastQty` | float | Khối lượng giao dịch gần nhất |
| `bidPrice` | float | Giá mua tốt nhất |
| `bidQty` | float | Khối lượng mua tốt nhất |
| `askPrice` | float | Giá bán tốt nhất |
| `askQty` | float | Khối lượng bán tốt nhất |
| `openPrice` | float | Giá mở cửa (cách đây 24h) |
| `highPrice` | float | Giá cao nhất (24h) |
| `lowPrice` | float | Giá thấp nhất (24h) |
| `volume` | float | Tổng khối lượng giao dịch (base, 24h) |
| `quoteVolume` | float | Tổng khối lượng quy về USDT (24h) |
| `openTime` | int | Thời điểm bắt đầu cửa sổ 24h (Unix ms) |
| `closeTime` | int | Thời điểm kết thúc cửa sổ 24h (Unix ms) |
| `firstId` | int | ID giao dịch đầu tiên |
| `lastId` | int | ID giao dịch cuối cùng |
| `count` | int | Tổng số giao dịch (24h) |

- **Số dòng:** 100 (1 dòng/symbol)
- **Dung lượng:** ~0.1 MB

---

### 3. trades_\<SYMBOL\>\_7d.csv

Toàn bộ giao dịch thực tế (tick data) trong **7 ngày** gần nhất, mỗi symbol 1 file.

| Cột | Kiểu | Mô tả |
|---|---|---|
| `trade_id` | int | ID giao dịch duy nhất |
| `price` | float | Giá khớp lệnh |
| `qty` | float | Khối lượng khớp lệnh (base) |
| `quote_qty` | float | Khối lượng khớp lệnh (USDT) |
| `time` | int | Thời điểm giao dịch (Unix ms) |
| `is_buyer_maker` | bool | `True` nếu người mua là maker |
| `is_best_match` | bool | `True` nếu là khớp lệnh tốt nhất |

- **Số dòng:** ~130,740,142 tổng (vd: BTCUSDT có ~22,948,656 dòng)
- **Dung lượng:** ~10.6 GB (100 files)
- **Nguồn:** Binance Bulk S3; các symbols thiếu trên S3 fallback sang REST API (`/api/v3/aggTrades`)

---

## Thống kê tổng quan

| Loại dữ liệu | Files | Dòng | Dung lượng |
|---|---|---|---|
| klines (1m, 3 tháng) | 1 | ~12,550,520 | ~2.1 GB |
| ticker_24h | 1 | 100 | ~0.1 MB |
| trades (7 ngày) | 100 | ~130,740,142 | ~10.6 GB |
| **Tổng** | **102** | **~143,290,762** | **~12.7 GB** |

---

## Yêu cầu

- Python 3.10+
- Binance API Key (tùy chọn — cần thiết cho REST fallback của trades)
- Tạo file `.env` với nội dung:

```
BINANCE_API_KEY=your_api_key_here
```
