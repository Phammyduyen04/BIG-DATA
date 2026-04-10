# Binance Historical Data Fetcher

Stream toàn bộ dữ liệu lịch sử từ Binance về máy local.

**Dữ liệu**: 1,424 spot symbols + 609 futures symbols  
**Dung lượng**: ~2.5 GB / ngày  
**Thời gian chạy**: ~8–10 tiếng (cho 1 ngày lịch sử)

---

## Yêu cầu

- Python 3.10+
- Binance API Key (tạo tại [binance.com/en/my/settings/api-management](https://www.binance.com/en/my/settings/api-management))

---

## Cài đặt

**1. Clone repo**
```bash
git clone <repo-url>
cd <repo-folder>
```

**2. Cài thư viện**
```bash
pip install -r requirements.txt
```

**3. Tạo file `.env`**
```bash
cp .env.example .env
```
Mở `.env` và điền API key:
```
BINANCE_API_KEY=your_api_key_here
BINANCE_SECRET_KEY=your_secret_key_here
```

---

## Cấu hình

Mở từng script và chỉnh phần `CONFIG` ở đầu file:

| File | Tham số | Mặc định | Ý nghĩa |
|------|---------|----------|---------|
| `fetch_binance_data.py` | `DAYS_BACK` | `1` | Số ngày lịch sử cần lấy |
| `fetch_binance_data.py` | `INTERVAL` | `1m` | Timeframe klines (`1m`, `5m`, `1h`, `1d`...) |
| `fetch_binance_data.py` | `USDT_ONLY` | `False` | `True` = chỉ lấy cặp USDT (~500 symbols, nhanh hơn) |
| `fetch_binance_data.py` | `FETCH_KLINES` | `True` | Bật/tắt tải Klines |
| `fetch_binance_data.py` | `FETCH_AGG_TRADES` | `True` | Bật/tắt tải AggTrades |
| `fetch_binance_vision.py` | `DAYS_BACK` | `1` | Số ngày lịch sử Raw Trades |
| `fetch_futures.py` | `DAYS_BACK` | `1` | Số ngày lịch sử Futures |

---

## Chạy

Mở **4 terminal riêng biệt** và chạy song song:

**Terminal 1** — Klines + AggTrades (chạy lâu nhất ~7–8 tiếng):
```bash
python fetch_binance_data.py
```

**Terminal 2** — Ticker 24h + Order Book snapshot:
```bash
python fetch_snapshot.py
```

**Terminal 3** — Raw Trades từ data.binance.vision (~10–15 phút):
```bash
python fetch_binance_vision.py
```

**Terminal 4** — Mark Price + Funding Rate Futures (~1–2 tiếng):
```bash
python fetch_futures.py
```

**Kiểm tra tiến độ** (bất kỳ lúc nào):
```bash
python check_progress.py
```

---

## Resume nếu bị ngắt

Script tự động lưu checkpoint vào `progress.json` và `futures_progress.json`.  
Nếu bị ngắt giữa chừng, **chạy lại đúng lệnh cũ** — script sẽ bỏ qua các symbols đã xong và tiếp tục từ chỗ dừng.

---

## Cấu trúc output

```
data/
├── klines/                          # Klines OHLCV 1m
│   └── {SYMBOL}/{SYMBOL}_1m_*.csv
├── agg_trades/                      # Aggregate Trades
│   └── {SYMBOL}/{SYMBOL}_agg_trades_*.csv
├── trades/                          # Raw Trades (data.binance.vision)
│   └── {SYMBOL}/{SYMBOL}-trades-*.csv
├── order_book/                      # Order Book snapshot
│   └── {SYMBOL}/{SYMBOL}_depth_*.csv
├── ticker_24h/                      # Ticker 24h snapshot
│   └── ticker_24h_*.csv
└── futures/
    ├── mark_price/                  # Mark Price Klines 1m
    │   └── {SYMBOL}/{SYMBOL}_mark_1m_*.csv
    └── funding_rate/                # Funding Rate (mỗi 8h)
        └── {SYMBOL}/{SYMBOL}_funding_*.csv
```

---

## Mô tả dữ liệu

Xem [DATA_DICTIONARY.md](DATA_DICTIONARY.md) để biết ý nghĩa từng trường dữ liệu.

---

## Lưu ý

- **Rate limit**: Script tự động nghỉ đúng batch để không bị Binance chặn (1200 weight/phút)
- **data.binance.vision**: Thường upload trễ 1–2 ngày — một số symbols có thể 404, chạy lại sau 1–2 ngày
- **Liquidations**: Cần bật quyền **Futures** trong Binance API key, sau đó đặt `FETCH_LIQUIDATIONS = True` trong `fetch_futures.py`
- **Dung lượng**: ~2.5 GB/ngày → cần ổ cứng đủ lớn nếu lấy nhiều ngày
