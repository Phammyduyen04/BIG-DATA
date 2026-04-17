# Crypto Market Dashboard — Frontend

Giao diện web hiển thị biểu đồ nến (candlestick), volume, giao dịch gần đây và bộ lọc coin theo thời gian thực.

---

## Công nghệ sử dụng

| Công nghệ | Phiên bản | Mục đích |
|---|---|---|
| [React](https://react.dev/) | 19.x | UI framework |
| [Vite](https://vitejs.dev/) | 8.x | Build tool & dev server |
| [ApexCharts](https://apexcharts.com/) + react-apexcharts | 5.x / 2.x | Biểu đồ nến, volume |
| [Tailwind CSS](https://tailwindcss.com/) | 3.x | Styling utility-first |
| [Axios](https://axios-http.com/) | 1.x | HTTP client gọi Backend API |
| [PostCSS](https://postcss.org/) + Autoprefixer | — | Xử lý CSS |

---

## Yêu cầu môi trường

- **Node.js** >= 18.x — [tải tại đây](https://nodejs.org/)
- **npm** >= 9.x (đi kèm Node.js)
- **Backend API** đang chạy tại `http://localhost:3000` (xem thư mục `BACKEND/`)

---

## Cài đặt & chạy

```bash
# 1. Di chuyển vào thư mục frontend
cd FRONTEND

# 2. Cài đặt dependencies
npm install

# 3. Chạy dev server
npm run dev
```

Mở trình duyệt tại: `http://localhost:5173`

---

## Các lệnh khác

```bash
npm run build      # Build production (output: dist/)
npm run preview    # Preview bản build production
npm run lint       # Kiểm tra lỗi ESLint
```

---

## Cấu trúc thư mục

```
FRONTEND/
├── src/
│   ├── api/
│   │   └── marketApi.js          # Các hàm gọi Backend API
│   ├── components/
│   │   ├── CandlestickChart.jsx  # Biểu đồ nến + volume (ApexCharts)
│   │   ├── Ticker24h.jsx         # Header giá & thống kê 24h
│   │   ├── IntervalSelector.jsx  # Bộ lọc khung thời gian & range
│   │   ├── TradesList.jsx        # Danh sách giao dịch gần đây
│   │   ├── CoinFilter.jsx        # Bộ lọc & tìm kiếm coin
│   │   └── SymbolSelector.jsx    # Chọn cặp giao dịch
│   ├── App.jsx                   # Component gốc, quản lý state
│   ├── main.jsx                  # Entry point React
│   └── index.css                 # Global styles + Tailwind
├── index.html
├── vite.config.js
├── tailwind.config.js
└── package.json
```

---

## Kết nối Backend

Frontend gọi API tại `http://localhost:3000/api/market`. Đảm bảo backend đã chạy trước khi mở frontend.

Các endpoint sử dụng:

| Endpoint | Mô tả |
|---|---|
| `GET /api/market/symbols` | Danh sách coin |
| `GET /api/market/:symbol/ticker24h` | Giá & thống kê 24h |
| `GET /api/market/:symbol/klines` | Dữ liệu nến (OHLCV) |
| `GET /api/market/:symbol/klines/latest-time` | Thời điểm dữ liệu mới nhất |
| `GET /api/market/:symbol/trades` | Giao dịch gần đây |
