# Tối ưu hiệu suất biểu đồ nến (Candlestick Chart)

## Phân tích nguyên nhân gốc rễ

### Tại sao chart bị lag?

> [!CAUTION]
> **ApexCharts (SVG-based) là thủ phạm chính.** Mỗi nến, bấc, và thanh volume là một phần tử SVG riêng biệt trong DOM. Với 500 nến → ~2000+ SVG elements → trình duyệt phải tính toán layout, paint, composite cho từng element khi zoom/pan.

| Vấn đề | Chi tiết |
|--------|---------|
| **Rendering engine** | ApexCharts dùng SVG → mỗi candle = 3-4 SVG elements (body + wick + volume bar) |
| **Khối lượng dữ liệu** | 1m interval × 10 ngày = **14,400 nến** (hiện giới hạn 500 nhưng vẫn lag) |
| **Tương lai 6 tháng** | 1m × 180 ngày = **259,200 nến** → hoàn toàn không khả thi với SVG |
| **Sort + Map mỗi render** | `useMemo` sort toàn bộ mảng klines + tạo object mới cho mỗi data point |
| **Không có data windowing** | Frontend fetch ALL data cùng lúc, không lazy-load khi pan |

### Giải pháp: Chuyển sang Canvas-based rendering

```
ApexCharts (SVG)          →    Lightweight Charts (Canvas)
~2000 DOM elements/500 nến →    1 <canvas> element cho TẤT CẢ data
Lag ở 500 nến             →    Mượt với 100,000+ nến
Không có built-in pan     →    Pan/zoom mượt mà native
```

## User Review Required

> [!IMPORTANT]
> **Thay đổi thư viện chart**: Chuyển từ `react-apexcharts` (ApexCharts) sang `lightweight-charts` (TradingView). Giao diện sẽ giống TradingView hơn (vốn là thư viện gốc của TradingView). Các tính năng hiện tại (zoom, pan, volume overlay, tooltip) đều được giữ nguyên.

> [!WARNING]  
> **Giao diện chart sẽ thay đổi** — Lightweight Charts có style riêng giống TradingView. Các nút toolbar hiện tại (cursor/hand/zoom in/zoom out) sẽ được thay bằng cơ chế zoom/pan native mượt mà hơn (scroll để zoom, kéo chuột để pan).

## Proposed Changes

### Component 1: Frontend — Thay thế Chart Library

#### [MODIFY] [package.json](file:///d:/full_stack/BIG-DATA/FRONTEND/package.json)
- Thêm dependency: `lightweight-charts` (~45KB, canvas-based, by TradingView)
- Giữ lại `apexcharts` + `react-apexcharts` (được dùng bởi `TopCoinsChart.jsx`)

---

#### [MODIFY] [CandlestickChart.jsx](file:///d:/full_stack/BIG-DATA/FRONTEND/src/components/CandlestickChart.jsx)
Viết lại hoàn toàn component sử dụng `lightweight-charts`:

**Tính năng mới:**
- **Canvas rendering**: 1 element thay vì hàng nghìn SVG nodes
- **Built-in pan**: Kéo chuột trái/phải mượt mà (không cần toolbar button)
- **Built-in zoom**: Scroll chuột để zoom in/out
- **Volume histogram**: Overlay trực tiếp bên dưới candle (giống TradingView)
- **Crosshair + Legend**: Hiển thị OHLCV khi hover
- **Auto-fit**: Tự điều chỉnh viewport khi data thay đổi
- **Lazy loading khi pan**: Khi user kéo sang trái (xem data cũ), tự động fetch thêm data từ backend

**Kiến trúc mới:**
```
CandlestickChart
├── useRef(chartContainerRef)     — DOM container
├── useRef(chartRef)              — IChartApi instance
├── useRef(candleSeriesRef)       — Candle series
├── useRef(volumeSeriesRef)       — Volume series
├── useEffect[mount]              — Khởi tạo chart + series
├── useEffect[klines]             — Cập nhật data khi klines thay đổi
├── useEffect[resize]             — ResizeObserver cho responsive
├── Legend overlay                — Hiển thị OHLCV khi crosshair move
└── onVisibleLogicalRangeChanged  — Trigger lazy-load khi pan đến rìa
```

---

#### [MODIFY] [App.jsx](file:///d:/full_stack/BIG-DATA/FRONTEND/src/App.jsx)
- Thêm callback `onLoadMore(direction)` để fetch thêm data khi user pan
- Giữ data klines trong state nhưng APPEND data mới thay vì replace
- Thêm logic merge + deduplicate klines khi load thêm

---

### Component 2: Backend — API hỗ trợ data windowing

#### [MODIFY] [MarketRepository.js](file:///d:/full_stack/BIG-DATA/BACKEND/src/repositories/MarketRepository.js)
- Thêm method `getKlinesCount(symbolCode, intervalCode, startTime, endTime)` — trả về số lượng records để frontend biết tổng data

#### [MODIFY] [MarketService.js](file:///d:/full_stack/BIG-DATA/BACKEND/src/services/MarketService.js)  
- Thêm method `getKlinesCount` với validation

#### [MODIFY] [marketRoutes.js](file:///d:/full_stack/BIG-DATA/BACKEND/src/routes/marketRoutes.js)
- Thêm route `GET /:symbol_code/klines/count`

---

### Component 3: Frontend — API layer

#### [MODIFY] [marketApi.js](file:///d:/full_stack/BIG-DATA/FRONTEND/src/api/marketApi.js)
- Thêm function `getKlinesCount(symbol, interval, startTime, endTime)`

---

## Chi tiết kỹ thuật

### Lightweight Charts — Tại sao chọn thư viện này?

| Tiêu chí | ApexCharts (hiện tại) | Lightweight Charts |
|-----------|----------------------|-------------------|
| Rendering | SVG (DOM elements) | **Canvas (1 element)** |
| Tốc độ render 10,000 candles | **Không khả thi** | **< 50ms** |
| Bundle size | ~460KB | **~45KB** |
| Pan/Zoom | Plugin, lag | **Native, 60fps** |
| Financial chart | Generic | **Chuyên biệt** |
| TradingView look & feel | Không | **Có (chính hãng)** |
| Crosshair | Cơ bản | **Giống TradingView** |
| Maintained by | ApexCharts Inc | **TradingView** |

### Lazy Loading khi Pan — Cách hoạt động

```
  ◀── load more ──┤ VISIBLE VIEWPORT ├── load more ──▶
                  │                  │
  [older data]    │ [displayed data] │   [newer data]
                  │                  │
  onVisibleLogicalRangeChanged → check if near edge → fetch + append
```

1. Frontend giữ một "buffer" data (VD: 500 candles)
2. Khi user pan sang trái và viewport gần rìa data → fetch thêm 300 candles cũ hơn
3. Data mới được PREPEND vào mảng hiện tại
4. Chart tự động update mà không nhảy viewport

### Responsive + Dark Theme

- Sử dụng `ResizeObserver` để chart tự co giãn
- Dark theme matching Binance style: `background: #0b0e11`, candle up: `#0ecb81`, candle down: `#f6465d`
- Volume bars với opacity matching candle color

## Open Questions

Không có câu hỏi mở — giải pháp đã rõ ràng.

## Verification Plan

### Automated Tests
- Chạy `npm run build` để đảm bảo không có lỗi compile
- Chạy frontend dev server và kiểm tra chart render

### Manual Verification  
- Mở browser, chọn 1m interval
- Zoom in/out bằng scroll — phải mượt mà 60fps
- Pan trái/phải bằng kéo chuột — phải mượt mà, không giật
- Hover để xem crosshair + OHLCV legend
- Pan đến rìa trái → data tự động load thêm
- Chuyển interval (1m → 5m → 1h) — data cập nhật đúng
- So sánh tốc độ trước/sau với cùng dataset
