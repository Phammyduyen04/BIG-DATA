# Mô tả dữ liệu Binance Crawl
**Thời gian crawl:** 2026-04-02
**Tổng requests:** 1052
**Tổng dung lượng:** ~82.4 MB
**Nguồn:** Binance Public + Private API (api.binance.com)

---

## 1. exchange_symbols.csv
**Kích thước:** 3556 dòng × 9 cột
**Mô tả:** Danh sách toàn bộ cặp giao dịch trên Binance Spot.

| Cột | Ý nghĩa |
|-----|---------|
| `symbol` | Tên cặp giao dịch (ví dụ: BTCUSDT) |
| `baseAsset` | Đồng coin gốc (ví dụ: BTC) |
| `quoteAsset` | Đồng coin định giá (ví dụ: USDT) |
| `status` | Trạng thái: TRADING / BREAK / END_OF_DAY |
| `baseAssetPrecision` | Số chữ số thập phân của đồng gốc |
| `quoteAssetPrecision` | Số chữ số thập phân của đồng định giá |
| `isSpotTradingAllowed` | Có cho phép giao dịch Spot không |
| `isMarginTradingAllowed` | Có cho phép giao dịch Margin không |
| `orderTypes` | Các loại lệnh được phép (LIMIT, MARKET, STOP_LOSS...) |

---

## 2. tickers_24hr_all.csv
**Kích thước:** 3559 dòng × 23 cột
**Mô tả:** Thống kê giá và khối lượng giao dịch trong 24 giờ của toàn bộ cặp giao dịch.

| Cột | Ý nghĩa |
|-----|---------|
| `symbol` | Tên cặp giao dịch |
| `priceChange` | Thay đổi giá tuyệt đối so với 24h trước |
| `priceChangePercent` | Thay đổi giá theo % so với 24h trước |
| `weightedAvgPrice` | Giá trung bình có trọng số theo khối lượng |
| `prevClosePrice` | Giá đóng cửa của kỳ trước |
| `lastPrice` | Giá giao dịch gần nhất |
| `lastQty` | Khối lượng của lệnh giao dịch gần nhất |
| `bidPrice` | Giá mua tốt nhất hiện tại (Best Bid) |
| `bidQty` | Khối lượng ở mức giá mua tốt nhất |
| `askPrice` | Giá bán tốt nhất hiện tại (Best Ask) |
| `askQty` | Khối lượng ở mức giá bán tốt nhất |
| `openPrice` | Giá mở cửa của kỳ 24h |
| `highPrice` | Giá cao nhất trong 24h |
| `lowPrice` | Giá thấp nhất trong 24h |
| `volume` | Tổng khối lượng giao dịch (tính theo đồng gốc) |
| `quoteVolume` | Tổng khối lượng giao dịch (tính theo đồng định giá) |
| `openTime` | Thời điểm mở cửa kỳ 24h (Unix ms) |
| `closeTime` | Thời điểm đóng cửa kỳ 24h (Unix ms) |
| `firstId` | ID lệnh đầu tiên trong kỳ 24h |
| `lastId` | ID lệnh cuối cùng trong kỳ 24h |
| `count` | Tổng số lệnh giao dịch trong 24h |
| `openTime_dt` | Thời điểm mở cửa (datetime đã chuyển đổi) |
| `closeTime_dt` | Thời điểm đóng cửa (datetime đã chuyển đổi) |

---

## 3. book_tickers_all.csv
**Kích thước:** 3559 dòng × 5 cột
**Mô tả:** Snapshot giá mua/bán tốt nhất (Best Bid/Ask) tại thời điểm crawl của toàn bộ cặp.

| Cột | Ý nghĩa |
|-----|---------|
| `symbol` | Tên cặp giao dịch |
| `bidPrice` | Giá mua cao nhất người mua sẵn sàng trả |
| `bidQty` | Khối lượng đặt mua ở mức giá đó |
| `askPrice` | Giá bán thấp nhất người bán sẵn sàng chấp nhận |
| `askQty` | Khối lượng đặt bán ở mức giá đó |

---

## 4. latest_prices.csv
**Kích thước:** 3559 dòng × 2 cột
**Mô tả:** Giá giao dịch mới nhất của toàn bộ cặp tại thời điểm crawl.

| Cột | Ý nghĩa |
|-----|---------|
| `symbol` | Tên cặp giao dịch |
| `price` | Giá giao dịch gần nhất |

---

## 5. klines_1m / 5m / 15m / 1h / 4h / 1d / 1w.csv
**Kích thước:** ~18k–50k dòng × 16 cột mỗi file
**Mô tả:** Dữ liệu nến (candlestick / OHLCV) theo 7 khung thời gian của Top 100 cặp USDT có khối lượng lớn nhất. Mỗi dòng là 1 nến.

| Cột | Ý nghĩa |
|-----|---------|
| `symbol` | Tên cặp giao dịch |
| `interval` | Khung thời gian nến (1m, 5m, 15m, 1h, 4h, 1d, 1w) |
| `openTime` | Thời điểm mở nến (Unix ms) |
| `open` | Giá mở cửa |
| `high` | Giá cao nhất trong nến |
| `low` | Giá thấp nhất trong nến |
| `close` | Giá đóng cửa |
| `volume` | Khối lượng giao dịch (đồng gốc) |
| `closeTime` | Thời điểm đóng nến (Unix ms) |
| `quoteAssetVolume` | Khối lượng giao dịch (đồng định giá) |
| `numberOfTrades` | Số lượng lệnh khớp trong nến |
| `takerBuyBaseVolume` | Khối lượng Taker mua (đồng gốc) — bên chủ động mua |
| `takerBuyQuoteVolume` | Khối lượng Taker mua (đồng định giá) |
| `ignore` | Trường dự phòng của Binance (bỏ qua) |
| `openTime_dt` | Thời điểm mở nến (datetime) |
| `closeTime_dt` | Thời điểm đóng nến (datetime) |

> **Lưu ý takerBuy:** Khi `takerBuyBaseVolume` cao so với `volume` → áp lực mua mạnh (bullish). Ngược lại → áp lực bán mạnh (bearish).

---

## 6. order_book_depth.csv
**Kích thước:** 9626 dòng × 5 cột
**Mô tả:** Sổ lệnh (Order Book) của 50 cặp USDT lớn nhất, mỗi cặp lấy 100 mức giá mua + 100 mức giá bán.

| Cột | Ý nghĩa |
|-----|---------|
| `symbol` | Tên cặp giao dịch |
| `lastUpdateId` | ID cập nhật sổ lệnh gần nhất |
| `side` | Chiều lệnh: `bid` (mua) hoặc `ask` (bán) |
| `price` | Mức giá đặt lệnh |
| `qty` | Khối lượng đặt tại mức giá đó |

> Dùng để phân tích **độ thanh khoản**, **spread**, và **wall order** (tường lệnh lớn).

---

## 7. recent_trades.csv
**Kích thước:** 25000 dòng × 9 cột
**Mô tả:** 500 lệnh giao dịch thực tế gần nhất của 50 cặp USDT lớn nhất.

| Cột | Ý nghĩa |
|-----|---------|
| `id` | ID của giao dịch |
| `price` | Giá khớp lệnh |
| `qty` | Khối lượng khớp (đồng gốc) |
| `quoteQty` | Khối lượng khớp (đồng định giá) |
| `time` | Thời điểm giao dịch (Unix ms) |
| `isBuyerMaker` | True = người bán chủ động (Taker bán); False = người mua chủ động (Taker mua) |
| `isBestMatch` | True nếu là lệnh khớp tốt nhất |
| `symbol` | Tên cặp giao dịch |
| `time_dt` | Thời điểm giao dịch (datetime) |

---

## 8. agg_trades.csv
**Kích thước:** 25000 dòng × 10 cột
**Mô tả:** Giao dịch tổng hợp (Aggregate Trades) — gộp nhiều lệnh cùng giá, cùng thời điểm, cùng chiều thành 1 dòng. Ít nhiễu hơn `recent_trades`.

| Cột | Ý nghĩa |
|-----|---------|
| `aggTradeId` | ID giao dịch tổng hợp |
| `price` | Giá khớp |
| `qty` | Tổng khối lượng gộp |
| `firstTradeId` | ID lệnh đầu tiên được gộp |
| `lastTradeId` | ID lệnh cuối cùng được gộp |
| `timestamp` | Thời điểm giao dịch (Unix ms) |
| `isBuyerMaker` | True = Taker bán; False = Taker mua |
| `isBestMatch` | Lệnh khớp tốt nhất |
| `symbol` | Tên cặp giao dịch |
| `time_dt` | Thời điểm giao dịch (datetime) |

---

## 9. balances_all.csv
**Kích thước:** 750 dòng × 4 cột
**Mô tả:** Số dư toàn bộ assets trong tài khoản Binance (Private — cần API key).

| Cột | Ý nghĩa |
|-----|---------|
| `asset` | Tên đồng coin (BTC, ETH, USDT...) |
| `free` | Số dư khả dụng (có thể giao dịch) |
| `locked` | Số dư đang bị khóa (trong lệnh chờ) |
| `total` | Tổng = free + locked |

> **Kết quả:** Tài khoản hiện tại không có số dư (tất cả = 0).

---

## 10. selected_symbols.csv
**Kích thước:** 100 dòng × 1 cột
**Mô tả:** Danh sách 100 cặp USDT được chọn để crawl klines/depth/trades, sắp xếp theo `quoteVolume` 24h giảm dần (top thanh khoản nhất).

---

## Tóm tắt quan hệ giữa các file

```
exchange_symbols.csv        ← Danh mục tất cả symbols
        |
        +-- tickers_24hr_all.csv    ← Thống kê 24h (giá, volume, bid/ask)
        +-- book_tickers_all.csv    ← Best bid/ask snapshot
        +-- latest_prices.csv       ← Giá mới nhất
        |
selected_symbols.csv (top 100 USDT)
        |
        +-- klines_1m/5m/15m/1h/4h/1d/1w.csv  ← Dữ liệu nến OHLCV
        +-- order_book_depth.csv    ← Sổ lệnh (50 symbols)
        +-- recent_trades.csv       ← Lệnh khớp gần nhất (50 symbols)
        +-- agg_trades.csv          ← Lệnh khớp tổng hợp (50 symbols)
        |
[Private - API Key]
        +-- balances_all.csv        ← Số dư tài khoản
        +-- account_info.json       ← Thông tin tài khoản đầy đủ
```
