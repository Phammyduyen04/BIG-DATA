# Binance Data Streaming — Top 100 USDT Pairs

Dữ liệu được thu thập từ Binance cho **99 cặp giao dịch USDT phổ biến nhất** (1 symbol bị loại do không có giao dịch thực tế trong 24h), bao gồm 3 loại dữ liệu: **klines**, **ticker_24h**, và **trades**.

---

## Tổng quan

| Thông số | Giá trị |
|---|---|
| Số symbols | **99** (top 100 USDT theo quoteVolume, loại symbol không có klines) |
| Ngày dữ liệu | **2026-04-17** (UTC) |
| Klines interval | **1m** (1 phút), window 24h |
| Nguồn klines & ticker | Binance REST API (`api.binance.com`) |
| Nguồn trades | Binance Bulk Download (`data.binance.vision`) |
| Tổng dung lượng | **~8.8 GB** (JSON: ~6.8 GB, CSV: ~2.3 GB) |

---

## Danh sách Symbols (99)

Được chọn dựa trên `quoteVolume` 24h từ lớn đến nhỏ, chỉ gồm các cặp giao dịch USDT.

```
USDCUSDT, BTCUSDT, ETHUSDT, XAUTUSDT, SOLUSDT, XRPUSDT, USD1USDT, DOGEUSDT,
BNBUSDT, ORDIUSDT, AVNTUSDT, MOVRUSDT, ZECUSDT, PEPEUSDT, RLUSDUSDT, ADAUSDT,
TAOUSDT, FDUSDUSDT, SUIUSDT, WLDUSDT, ... (xem active_symbols_matched.json)
```

> Symbol bị loại: `UTKUSDT` — có trong ticker nhưng không có giao dịch thực tế (0 klines).

---

## Cấu trúc thư mục

```
output/
├── json/                                      # Dữ liệu dạng JSON
│   ├── klines_1m_20260418T061641Z.json        # Klines toàn bộ 99 symbols
│   ├── ticker_24h_20260418T061641Z.json       # Ticker 24h toàn bộ 99 symbols
│   ├── trades_<SYMBOL>_2026-04-17.json        # Trades từng symbol (99 files)
│   ├── active_symbols_matched.json            # Danh sách 99 symbols đã khớp
│   ├── top100_usdt_symbols.json               # Danh sách 100 symbols ban đầu
│   └── trades_symbols_matched.json            # Symbols đã download trades thành công
├── csv/                                       # Dữ liệu dạng CSV (cùng nội dung với JSON)
│   ├── klines_1m_20260418T061641Z.csv
│   ├── ticker_24h_20260418T061641Z.csv
│   └── trades_<SYMBOL>_2026-04-17.csv         # Trades từng symbol (99 files)
└── stream.log                                 # Log toàn bộ quá trình thu thập
```

---

## Mô tả từng loại dữ liệu

### 1. Klines (`klines_1m_*.json / .csv`)

Nến 1 phút (OHLCV) trong 24h qua cho toàn bộ 99 symbols.

| Trường | Kiểu | Mô tả |
|---|---|---|
| `symbol` | string | Cặp giao dịch (ví dụ: `BTCUSDT`) |
| `open_time` | int (ms) | Thời gian mở nến (Unix timestamp, milliseconds) |
| `open_time_dt` | string (ISO 8601) | Thời gian mở nến dạng UTC |
| `open` | string | Giá mở cửa |
| `high` | string | Giá cao nhất |
| `low` | string | Giá thấp nhất |
| `close` | string | Giá đóng cửa |
| `volume` | string | Khối lượng base asset |
| `close_time` | int (ms) | Thời gian đóng nến |
| `quote_asset_volume` | string | Khối lượng quote asset (USDT) |
| `num_trades` | int | Số lượng giao dịch trong nến |
| `taker_buy_base_vol` | string | Khối lượng mua taker (base) |
| `taker_buy_quote_vol` | string | Khối lượng mua taker (USDT) |
| `ignore` | string | Trường bỏ qua (Binance reserved) |

**Thống kê:**
- Tổng records: **142,560** (99 symbols × 1,440 nến/symbol)
- Dung lượng: ~64 MB (JSON), ~18 MB (CSV)

---

### 2. Ticker 24h (`ticker_24h_*.json / .csv`)

Thống kê tổng hợp 24h cho 99 symbols, snapshot tại thời điểm stream.

| Trường | Kiểu | Mô tả |
|---|---|---|
| `symbol` | string | Cặp giao dịch |
| `priceChange` | string | Thay đổi giá tuyệt đối trong 24h |
| `priceChangePercent` | string | Thay đổi giá theo % trong 24h |
| `weightedAvgPrice` | string | Giá trung bình theo khối lượng |
| `prevClosePrice` | string | Giá đóng cửa phiên trước |
| `lastPrice` | string | Giá giao dịch gần nhất |
| `lastQty` | string | Khối lượng giao dịch gần nhất |
| `bidPrice` | string | Giá mua tốt nhất |
| `bidQty` | string | Khối lượng mua tốt nhất |
| `askPrice` | string | Giá bán tốt nhất |
| `askQty` | string | Khối lượng bán tốt nhất |
| `openPrice` | string | Giá mở cửa 24h |
| `highPrice` | string | Giá cao nhất 24h |
| `lowPrice` | string | Giá thấp nhất 24h |
| `volume` | string | Tổng khối lượng base asset 24h |
| `quoteVolume` | string | Tổng khối lượng USDT 24h |
| `openTime` | int (ms) | Thời điểm bắt đầu window 24h |
| `closeTime` | int (ms) | Thời điểm kết thúc window 24h |
| `firstId` | int | Trade ID đầu tiên trong 24h |
| `lastId` | int | Trade ID cuối cùng trong 24h |
| `count` | int | Tổng số giao dịch trong 24h |

**Thống kê:**
- Tổng records: **99** (1 dòng/symbol)

---

### 3. Trades (`trades_<SYMBOL>_2026-04-17.json / .csv`)

Toàn bộ giao dịch khớp lệnh trong ngày 2026-04-17 (UTC) cho từng symbol. Tải từ Binance Bulk Download với xác minh checksum SHA-256.

| Trường | Kiểu | Mô tả |
|---|---|---|
| `trade_id` | string | ID giao dịch duy nhất |
| `price` | string | Giá khớp lệnh |
| `qty` | string | Khối lượng base asset |
| `quote_qty` | string | Khối lượng USDT |
| `time` | string (ms) | Thời điểm giao dịch (Unix timestamp, milliseconds) |
| `is_buyer_maker` | string | `True` nếu bên mua là maker |
| `is_best_match` | string | `True` nếu là khớp lệnh tốt nhất |

**Thống kê:**
- Tổng records: **31,532,046** trades
- Số files: 99 (1 file/symbol)
- Dung lượng: ~6.7 GB (JSON), ~2.2 GB (CSV)

---

## Scripts

| Script | Mô tả |
|---|---|
| `stream_binance.py` | Stream klines (1m) và ticker_24h qua Binance REST API |
| `stream_trades_bulk.py` | Download trades qua Binance Bulk Download (S3) với checksum |

### Cài đặt

```bash
pip install -r requirements.txt
```

### Chạy

```bash
# Stream klines + ticker_24h
python stream_binance.py

# Download trades
python stream_trades_bulk.py
```

---

## Đảm bảo tính nhất quán (Symbol Matching)

Ba dataset **klines**, **ticker_24h**, và **trades** đều dùng đúng cùng **99 symbols**:

1. `stream_binance.py` lấy top 100 USDT symbols theo quoteVolume → fetch klines → loại symbols có 0 candles → fetch ticker chỉ cho symbols còn lại → lưu `active_symbols_matched.json`
2. `stream_trades_bulk.py` đọc `active_symbols_matched.json` làm input → đảm bảo trades khớp hoàn toàn với klines và ticker

```
klines  ∩  ticker_24h  ∩  trades  =  99 symbols  ✓
```
