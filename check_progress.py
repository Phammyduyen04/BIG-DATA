"""
Xem tiến độ tải dữ liệu Binance.
Chạy: python check_progress.py
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

PROGRESS_FILE = Path("progress.json")
META_FILE     = Path("meta.json")
DATA_DIR      = Path("data")


def human_size(path: Path) -> str:
    total = sum(f.stat().st_size for f in path.rglob("*.csv") if f.is_file())
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if total < 1024:
            return f"{total:.1f} {unit}"
        total /= 1024
    return f"{total:.1f} PB"


def fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        m, s = divmod(seconds, 60)
        return f"{m}p {s}s"
    else:
        h, rem = divmod(seconds, 3600)
        m = rem // 60
        return f"{h}h {m}p"


def progress_bar(done: int, total: int, width: int = 30) -> str:
    if total == 0:
        return f"[{'─' * width}]"
    pct  = done / total
    fill = int(pct * width)
    bar  = "█" * fill + "░" * (width - fill)
    return f"[{bar}] {pct*100:.1f}%"


def calc_eta(done: int, total: int, started_at_str: str) -> str:
    if done == 0:
        return "Đang tính..."
    try:
        started = datetime.fromisoformat(started_at_str)
        elapsed = (datetime.now() - started).total_seconds()
        speed   = done / elapsed          # symbols / giây
        remaining = (total - done) / speed
        eta_time = datetime.now() + timedelta(seconds=remaining)
        return f"{fmt_duration(remaining)} (xong lúc ~{eta_time.strftime('%H:%M')})"
    except Exception:
        return "N/A"


def main():
    if not PROGRESS_FILE.exists():
        print("Chưa có progress.json — script chưa được chạy.")
        return

    with open(PROGRESS_FILE, encoding="utf-8") as f:
        progress = json.load(f)

    # Đọc meta từ file riêng (tránh bị script chính ghi đè)
    meta    = json.load(open(META_FILE, encoding="utf-8")) if META_FILE.exists() else {}
    klines  = progress.get("klines", {})
    trades  = progress.get("agg_trades", {})
    total   = meta.get("total_symbols") or len(klines) or 0
    started = meta.get("started_at", "")
    phase   = "agg_trades" if trades else "klines"

    # Thời gian đã chạy
    elapsed_str = "N/A"
    if started:
        elapsed_sec = (datetime.now() - datetime.fromisoformat(started)).total_seconds()
        elapsed_str = fmt_duration(elapsed_sec)

    print()
    print("=" * 55)
    print("      TIẾN ĐỘ TẢI DỮ LIỆU BINANCE")
    print("=" * 55)
    print(f"  Tổng symbols    : {total:,}")
    print(f"  Bắt đầu lúc    : {started[:19] if started else 'N/A'}")
    print(f"  Đã chạy        : {elapsed_str}")
    print(f"  Phase hiện tại : {'Klines' if phase == 'klines' else 'Aggregate Trades'}")
    print("─" * 55)

    # ── PHASE 1: KLINES ──────────────────────────────────────
    k_done      = len(klines)
    k_rows      = sum(v.get("rows", 0) for v in klines.values())
    k_started   = meta.get("started_at", "")

    # Tính ETA klines (chỉ tính khi đang ở phase klines)
    if phase == "klines" and k_started:
        k_eta = calc_eta(k_done, total, k_started)
    elif k_done >= total and total > 0:
        k_eta = "Hoàn tất ✓"
    else:
        k_eta = "Chờ phase 1..."

    print(f"\n  [PHASE 1] Klines (1m)")
    print(f"  {progress_bar(k_done, total)}")
    print(f"  Symbols xong   : {k_done:,} / {total:,}")
    print(f"  Tổng candles   : {k_rows:,}")
    print(f"  ETA            : {k_eta}")

    # ── PHASE 2: AGG TRADES ───────────────────────────────────
    t_done    = len(trades)
    t_rows    = sum(v.get("rows", 0) for v in trades.values())

    # Tính ETA trades (chỉ tính khi đang ở phase agg_trades)
    if phase == "agg_trades" and t_done > 0:
        # Lấy thời điểm bắt đầu phase 2
        phase2_start = meta.get("phase2_started_at") or \
                       min(v.get("done_at", started) for v in trades.values())
        t_eta = calc_eta(t_done, total, phase2_start)
    elif t_done >= total and total > 0:
        t_eta = "Hoàn tất ✓"
    elif phase == "klines":
        t_eta = "Chờ Phase 1 xong..."
    else:
        t_eta = "Đang tính..."

    print(f"\n  [PHASE 2] Aggregate Trades")
    print(f"  {progress_bar(t_done, total)}")
    print(f"  Symbols xong   : {t_done:,} / {total:,}")
    print(f"  Tổng trades    : {t_rows:,}")
    print(f"  ETA            : {t_eta}")

    # ── DUNG LƯỢNG ─────────────────────────────────────────────
    print(f"\n  [Dung lượng]")
    for sub in ["klines", "agg_trades"]:
        sp = DATA_DIR / sub
        if sp.exists():
            print(f"  {sub:<14}: {human_size(sp)}")

    print("=" * 55)
    print()


if __name__ == "__main__":
    main()
