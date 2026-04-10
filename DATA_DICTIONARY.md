# Binance Data Dictionary
## Mô tả ý nghĩa các loại dữ liệu đã stream về

**Nguồn**: Binance REST API (`api.binance.com`, `fapi.binance.com`, `data.binance.vision`)  
**Khoảng thời gian**: 1 ngày (09/04/2026)  
**Tổng**: 2.5 GB | 6,904 files | ~38.9 triệu dòng

---

## 1. Klines (OHLCV) — `data/klines/`

**Ý nghĩa**: Nến giá theo khung thời gian 1 phút (candlestick). Cho biết giá mở, đóng, cao nhất, thấp nhất và khối lượng giao dịch trong mỗi phút.

**File**: `data/klines/{SYMBOL}/{SYMBOL}_1m_YYYYMMDD_YYYYMMDD.csv`  
**Số lượng**: 1,424 symbols | 2,047,359 candles

| Cột | Kiểu | Ý nghĩa |
|-----|------|---------|
| `open_time` | datetime (UTC) | Thời điểm bắt đầu nến |
| `open` | float | Giá mở cửa (giá đầu tiên của phút đó) |
| `high` | float | Giá cao nhất trong phút |
| `low` | float | Giá thấp nhất trong phút |
| `close` | float | Giá đóng cửa (giá cuối cùng của phút đó) |
| `volume` | float | Khối lượng giao dịch tính theo đồng gốc (base asset) |
| `close_time` | datetime (UTC) | Thời điểm kết thúc nến |
| `quote_volume` | float | Khối lượng giao dịch tính theo đồng quote (VD: USDT) |
| `num_trades` | int | Số lệnh giao dịch được khớp trong phút |
| `taker_buy_base_vol` | float | Khối lượng taker mua tính theo base asset |
| `taker_buy_quote_vol` | float | Khối lượng taker mua tính theo quote asset |

> **Taker** là người đặt lệnh market (khớp ngay), **Maker** là người đặt lệnh limit (chờ khớp).

---

## 2. Aggregate Trades — `data/agg_trades/`

**Ý nghĩa**: Giao dịch đã được gộp nhóm — nhiều lệnh khớp tại cùng mức giá, cùng chiều, trong cùng thời điểm được gộp thành 1 dòng. Phù hợp để phân tích dòng tiền và market microstructure.

**File**: `data/agg_trades/{SYMBOL}/{SYMBOL}_agg_trades_YYYYMMDD_YYYYMMDD.csv`  
**Số lượng**: 1,423 symbols | 5,807,223 giao dịch

| Cột | Kiểu | Ý nghĩa |
|-----|------|---------|
| `agg_trade_id` | int | ID duy nhất của giao dịch gộp |
| `price` | float | Giá khớp lệnh |
| `quantity` | float | Tổng khối lượng khớp (đã gộp) |
| `first_trade_id` | int | ID lệnh đầu tiên trong nhóm gộp |
| `last_trade_id` | int | ID lệnh cuối cùng trong nhóm gộp |
| `timestamp` | datetime (UTC) | Thời điểm xảy ra giao dịch |
| `is_buyer_maker` | bool | `True` = người mua là maker (đặt lệnh limit chờ); `False` = người mua là taker (đặt lệnh market) |
| `is_best_match` | bool | `True` = giao dịch khớp ở giá tốt nhất |

---

## 3. Raw Trades — `data/trades/`

**Ý nghĩa**: Từng giao dịch lẻ, KHÔNG gộp nhóm. Chi tiết nhất, cho biết chính xác từng lệnh được khớp. Dữ liệu từ kho lưu trữ chính thức `data.binance.vision`.

**File**: `data/trades/{SYMBOL}/{SYMBOL}-trades-YYYY-MM-DD.csv`  
**Số lượng**: 1,418 symbols | ~19.8 triệu giao dịch | 1.8 GB

| Cột | Kiểu | Ý nghĩa |
|-----|------|---------|
| `trade_id` | int | ID duy nhất của lệnh giao dịch |
| `price` | float | Giá khớp lệnh |
| `qty` | float | Khối lượng giao dịch (base asset) |
| `quote_qty` | float | Giá trị giao dịch (quote asset, VD: USDT) |
| `time` | int (ms) | Timestamp Unix (milliseconds) |
| `is_buyer_maker` | bool | `True` = người mua là maker; `False` = người mua là taker |
| `is_best_match` | bool | `True` = khớp ở giá tốt nhất trong sổ lệnh |

> **So sánh với AggTrades**: Raw Trades ghi từng lệnh riêng lẻ → chi tiết hơn nhưng dung lượng lớn hơn nhiều (1.8GB vs 474MB).

---

## 4. Order Book (Depth Snapshot) — `data/order_book/`

**Ý nghĩa**: Ảnh chụp sổ lệnh tại 1 thời điểm duy nhất (16:02 UTC ngày 09/04). Cho biết tất cả lệnh mua/bán đang chờ khớp ở các mức giá khác nhau, với độ sâu 100 mức mỗi chiều.

> ⚠️ Đây là **snapshot** (ảnh chụp tức thời), không phải dữ liệu lịch sử liên tục. Binance không cung cấp lịch sử Order Book qua bất kỳ API nào.

**File**: `data/order_book/{SYMBOL}/{SYMBOL}_depth_YYYYMMDD_HHMMSS.csv`  
**Số lượng**: 1,424 symbols | 100 bids + 100 asks = 200 dòng/symbol

| Cột | Kiểu | Ý nghĩa |
|-----|------|---------|
| `side` | string | `bid` = lệnh mua; `ask` = lệnh bán |
| `price` | float | Mức giá đặt lệnh |
| `quantity` | float | Khối lượng đang chờ tại mức giá đó |
| `snapshot_time` | datetime | Thời điểm chụp snapshot |

> **Bids** sắp xếp giá giảm dần (cao → thấp); **Asks** sắp xếp giá tăng dần (thấp → cao).  
> Khoảng cách giữa bid cao nhất và ask thấp nhất gọi là **spread**.

---

## 5. Ticker 24h — `data/ticker_24h/`

**Ý nghĩa**: Thống kê tổng hợp hoạt động giao dịch trong 24 giờ qua của tất cả symbols, lấy tại 1 thời điểm. Dữ liệu cửa sổ trượt (rolling 24h), không phải ngày calendar.

> ⚠️ Đây cũng là **snapshot** tại thời điểm chạy script. Không phải dữ liệu lịch sử.

**File**: `data/ticker_24h/ticker_24h_YYYYMMDD_HHMMSS.csv`  
**Số lượng**: 1 file | 3,562 symbols

| Cột | Kiểu | Ý nghĩa |
|-----|------|---------|
| `symbol` | string | Tên cặp giao dịch (VD: BTCUSDT) |
| `priceChange` | float | Thay đổi giá trong 24h (tuyệt đối) |
| `priceChangePercent` | float | Thay đổi giá trong 24h (%) |
| `weightedAvgPrice` | float | Giá trung bình có trọng số theo khối lượng |
| `prevClosePrice` | float | Giá đóng cửa của phiên trước |
| `lastPrice` | float | Giá giao dịch gần nhất |
| `lastQty` | float | Khối lượng giao dịch gần nhất |
| `bidPrice` | float | Giá mua tốt nhất hiện tại |
| `bidQty` | float | Khối lượng tại giá mua tốt nhất |
| `askPrice` | float | Giá bán tốt nhất hiện tại |
| `askQty` | float | Khối lượng tại giá bán tốt nhất |
| `openPrice` | float | Giá mở cửa cách đây 24h |
| `highPrice` | float | Giá cao nhất trong 24h |
| `lowPrice` | float | Giá thấp nhất trong 24h |
| `volume` | float | Tổng khối lượng giao dịch 24h (base asset) |
| `quoteVolume` | float | Tổng giá trị giao dịch 24h (quote asset) |
| `openTime` | datetime | Thời điểm bắt đầu cửa sổ 24h |
| `closeTime` | datetime | Thời điểm kết thúc cửa sổ 24h |
| `firstId` | int | ID giao dịch đầu tiên trong 24h |
| `lastId` | int | ID giao dịch cuối cùng trong 24h |
| `count` | int | Tổng số giao dịch trong 24h |

---

## 6. Mark Price Klines — `data/futures/mark_price/`

**Ý nghĩa**: Nến giá Mark Price theo khung 1 phút cho thị trường Futures. Mark Price là giá tham chiếu được Binance tính toán từ nhiều sàn để tránh thao túng giá và xác định ngưỡng thanh lý (liquidation).

**File**: `data/futures/mark_price/{SYMBOL}/{SYMBOL}_mark_1m_YYYYMMDD.csv`  
**Số lượng**: 609 futures symbols

| Cột | Kiểu | Ý nghĩa |
|-----|------|---------|
| `open_time` | datetime (UTC) | Thời điểm bắt đầu nến |
| `open` | float | Mark Price đầu phút |
| `high` | float | Mark Price cao nhất trong phút |
| `low` | float | Mark Price thấp nhất trong phút |
| `close` | float | Mark Price cuối phút |
| `close_time` | datetime (UTC) | Thời điểm kết thúc nến |
| `num_trades` | int | Số giao dịch futures trong phút |

> **Mark Price** khác với **Last Price**: Last Price là giá giao dịch thực tế trên Binance Futures; Mark Price là giá tham chiếu chống thao túng. Liquidation được tính theo Mark Price.

---

## 7. Funding Rate — `data/futures/funding_rate/`

**Ý nghĩa**: Tỷ lệ thanh toán định kỳ giữa vị thế Long và Short trong Perpetual Futures (hợp đồng vĩnh viễn). Cơ chế này giữ giá Futures bám sát giá Spot.

**File**: `data/futures/funding_rate/{SYMBOL}/{SYMBOL}_funding_YYYYMMDD.csv`  
**Số lượng**: 605 perpetual futures symbols (mỗi 8 giờ/lần = 3 records/ngày)

| Cột | Kiểu | Ý nghĩa |
|-----|------|---------|
| `symbol` | string | Tên cặp futures |
| `fundingTime` | datetime (UTC) | Thời điểm thu/trả funding (mỗi 8h: 00:00, 08:00, 16:00 UTC) |
| `fundingRate` | float | Tỷ lệ funding (dương = Long trả Short; âm = Short trả Long) |
| `markPrice` | float | Mark Price tại thời điểm tính funding |

> **Ví dụ**: `fundingRate = 0.0001` (0.01%) → người giữ vị thế Long trả 0.01% giá trị vị thế cho Short mỗi 8 giờ. Funding Rate phản ánh tâm lý thị trường: dương = thị trường thiên Long (bullish); âm = thiên Short (bearish).

> **Quarterly Futures** (`BTCUSDT_260626`, v.v.) không có Funding Rate vì chúng có ngày đáo hạn cố định, không cần cơ chế neo giá.

---

## Tóm tắt so sánh các loại dữ liệu

| Loại | Timeframe | Tần suất | Mục đích chính | Dung lượng/ngày |
|------|-----------|----------|----------------|-----------------|
| Klines | **1m** | 1 phút/candle | Phân tích kỹ thuật, backtesting | 224 MB |
| AggTrades | **Tick** | Từng giao dịch (gộp) | Phân tích dòng tiền, order flow | 474 MB |
| Raw Trades | **Tick** | Từng giao dịch (chi tiết) | Microstructure, HFT research | 1.8 GB |
| Order Book | **Snapshot** | 1 lần duy nhất | Thanh khoản tức thời, spread | 15 MB |
| Ticker 24h | **Snapshot** | 1 lần duy nhất | Tổng quan thị trường | 684 KB |
| Mark Price | **1m** | 1 phút/candle | Futures pricing, liquidation | 86 MB |
| Funding Rate | **8h** | 3 lần/ngày | Sentiment, basis trading | 748 KB |

---

## Thay đổi Timeframe Klines

**Cách 1** — Tải trực tiếp từ API với interval khác (chỉnh trong `fetch_binance_data.py`):
```python
INTERVAL = "5m"   # hoặc "15m", "1h", "4h", "1d"
```

**Cách 2** — Resample từ dữ liệu 1m đã có (không cần gọi API thêm):
```python
df.resample("5min", on="open_time").agg({
    "open": "first", "high": "max",
    "low": "min",    "close": "last", "volume": "sum"
})
```
