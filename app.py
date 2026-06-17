"""
app.py - Daangn local-life crawler (terminal UI)
==========================================
run:
  python app.py --start START_ID --end END_ID --step 100

exit: Ctrl+C (automatic progress save)
required packages: pip install rich openpyxl
"""

import threading
import signal
import sys
import os
import csv
import time
import argparse
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
from rich import box

from crawler import Config, Stats, run_crawl, FIELDNAMES

console = Console()

# ── chunk constants ─────────────────────────────────────────────
CHUNK_TOTAL_START = 0
CHUNK_TOTAL_END   = 0
CHUNK_SIZE        = 2_000
CHUNK_PEOPLE      = 5


def _all_chunks():
    """Return all dbId chunks as (index, start_dbid, end_dbid)."""
    chunks, cur, idx = [], CHUNK_TOTAL_START, 1
    while cur > CHUNK_TOTAL_END:
        end = max(cur - CHUNK_SIZE + 1, CHUNK_TOTAL_END)
        chunks.append((idx, cur, end))
        cur = end - 1
        idx += 1
    return chunks


def _parse_chunk_range(raw: str, total: int):
    """Parse inputs such as '500' or '1-847' into an inclusive chunk range."""
    raw = raw.strip()
    if "-" in raw:
        a, b = raw.split("-", 1)
        c_from, c_to = int(a), int(b)
    else:
        c_from = c_to = int(raw)
    if not (1 <= c_from <= c_to <= total):
        raise ValueError
    return c_from, c_to


def select_chunk_interactive():
    """
    chunk selection UI.
    return: (start_dbid, end_dbid, label)
    """
    chunks = _all_chunks()
    total  = len(chunks)
    per    = total // CHUNK_PEOPLE
    rem    = total % CHUNK_PEOPLE

    console.print("\n [bold orange1]chunk distribution guide[/]")
    console.print(f"  total chunks: {total:,}  (2,000 dbIds per chunk)\n")

    start_i = 0
    for p in range(1, CHUNK_PEOPLE + 1):
        count = per + (1 if p <= rem else 0)
        end_i = start_i + count - 1
        first, last = chunks[start_i], chunks[end_i]
        console.print(
            f"  [bold]{p}number[/]: chunk {first[0]:>4} ~ {last[0]:>4}  "
            f"dbId {last[2]:,} ~ {first[1]:,}  ({count}count chunk)"
        )
        start_i = end_i + 1

    console.print()
    console.print("  enter chunk number to crawl.")
    console.print("  example) single: [bold]500[/]   range: [bold]1-847[/]")
    console.print()

    while True:
        raw = input("  chunk number: ").strip()
        try:
            c_from, c_to = _parse_chunk_range(raw, total)
            start_dbid = chunks[c_from - 1][1]
            end_dbid   = chunks[c_to   - 1][2]
            label      = raw.replace(" ", "")
            console.print(
                f"\n  [green]selection complete[/]: chunk {c_from}~{c_to}  "
                f"(dbId {end_dbid:,} ~ {start_dbid:,})\n"
            )
            return start_dbid, end_dbid, label
        except ValueError:
            console.print(f"  [red]enter a valid format. (range: 1~{total:,})[/]")
# ─────────────────────────────────────────────────────────

# ── CLI settings ────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(
        description="Karrot community research crawler",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument("--start", type=int, required=True,
                   help="approved start dbId")
    p.add_argument("--end", type=int, required=True,
                   help="approved end dbId")
    p.add_argument("--step", type=int, default=100,
                   help="dbId step (default: 100)")
    p.add_argument("--rps", type=float, default=1.0,
                   help="max requests per second (default: 1.0; reduced after HTTP 429)")
    p.add_argument("--concurrency", type=int, default=1,
                   help="concurrent connections (default: 1)")
    p.add_argument("--batch", type=int, default=10,
                   help="batch size (default: 10)")
    p.add_argument("--pause", type=float, default=2.0,
                   help="minimum wait between batches seconds (default: 2.0)")
    p.add_argument("--output", type=str, default="daangn_seoul.csv",
                   help="output CSV filename (default: daangn_seoul.csv)")
    p.add_argument("--chunk", type=str, default=None,
                   help="chunk number or range (example: 500, 1-847)")
    p.add_argument("--format", choices=["csv", "xlsx"], default=None,
                   help="output format: csv or xlsx")
    p.add_argument("--reset", action="store_true",
                   help="ignore previous progress and start over")
    return p.parse_args()

args = parse_args()

cfg = Config(
    rps=args.rps,
    concurrency=args.concurrency,
    batch_size=args.batch,
    batch_pause=args.pause,
    step=args.step,
    save_every=500,
    start_dbid=args.start,
    end_dbid=args.end,
    output_csv=args.output,
    progress_file=args.output.replace(".csv", "_progress.json"),
    regions_file="seoul_regions.json",
)

# Chunk-specific values are assigned in main().
# ────────────────────────────────────────────────────────

stats = Stats()
logs = []
stop_event = threading.Event()
pause_event = threading.Event()


def on_log(msg):
    ts = time.strftime("%H:%M:%S")
    logs.append(f"[dim]{ts}[/] {msg}")
    if len(logs) > 50:
        logs.pop(0)


def on_batch(s):
    pass


def on_save(s):
    on_log(f"[green]💾 save: {s.collected:,}cases[/]")


def fmt_num(n):
    return f"{n:,}"


def fmt_time(sec):
    if sec < 60:
        return f"{int(sec)}seconds"
    if sec < 3600:
        return f"{int(sec//60)}minutes {int(sec%60)}seconds"
    return f"{int(sec//3600)}time {int((sec%3600)//60)}minutes"


def build_display():
    total_scans = max((cfg.start_dbid - cfg.end_dbid) // cfg.step, 1)
    pct = min(stats.scanned / total_scans * 100, 100) if total_scans > 0 else 0
    eta = (total_scans - stats.scanned) / stats.speed if stats.speed > 0 else 0

    # ── top: progress bar ──
    bar_width = 50
    filled = int(bar_width * pct / 100)
    bar = f"[orange1]{'━' * filled}[/][dim]{'─' * (bar_width - filled)}[/]"
    progress_text = f"{bar}  [bold]{pct:.3f}%[/]"

    # ── statistics ──
    status_icon = "[bold green]● running[/]" if stats.running and not stats.paused else \
                  "[bold yellow]● paused[/]" if stats.paused else \
                  "[dim]● waiting[/]"

    stats_lines = [
        f"  status      {status_icon}",
        f"  scan      [bold]{fmt_num(stats.scanned)}[/]  [dim]({stats.speed:.1f}/seconds)[/]",
        f"  Seoul posts  [bold orange1]{fmt_num(stats.collected)}[/]  [dim]({stats.seoul_speed:.1f}/seconds)[/]",
        f"  current dbId  [bold]{fmt_num(stats.current_dbid)}[/]",
        f"  current RPS   [bold cyan]{stats.current_rps:.1f}[/]/second",
        f"  error rate    {'[red]' if stats.err_rate > 0.1 else ''}{stats.err_rate:.0%}{'[/]' if stats.err_rate > 0.1 else ''}",
        f"  elapsed       {fmt_time(stats.elapsed)}",
        f"  estimated remaining  {fmt_time(eta) if eta > 0 else '-'}",
    ]

    # ── district distribution ──
    gu_sorted = sorted(stats.gu_counts.items(), key=lambda x: -x[1])[:12]
    if gu_sorted:
        max_cnt = gu_sorted[0][1] if gu_sorted else 1
        gu_lines = []
        for gu, cnt in gu_sorted:
            bar_len = int(cnt / max_cnt * 20)
            gu_lines.append(f"  {gu:<8} [orange1]{'█' * bar_len}[/] {cnt}")
        gu_text = "\n".join(gu_lines)
    else:
        gu_text = "  [dim]shown after collection starts[/]"

    # ── settings summary ──
    sample_pct = f"{1/cfg.step*100:.2f}%" if cfg.step > 1 else "full scan"
    est_seoul = int(total_scans * 0.187)
    config_text = (
        f"  RPS        {cfg.rps} (local)\n"
        f"  Concurrency {cfg.concurrency}\n"
        f"  Batch      {cfg.batch_size}\n"
        f"  Pause      {cfg.batch_pause}seconds\n"
        f"  Step       {cfg.step}  [dim]({sample_pct})[/]\n"
        f"  estimated scans  {fmt_num(total_scans)}\n"
        f"  estimated Seoul hits  ~{fmt_num(est_seoul)}\n"
        f"  CSV file   {cfg.output_csv}"
    )

    # ── local ──
    log_text = "\n".join(logs[-12:]) if logs else "[dim]  waiting...[/]"

    # ── compose ──
    output = Text.from_markup(
        f"\n [bold orange1]🥕 Daangn local-life crawler[/]  [dim]Seoul full scan local[/]\n"
        f" {'─' * 58}\n\n"
        f" {progress_text}\n\n"
    )

    # compose panels as text
    sections = (
        f"[bold dim]─── progress ───[/]\n{chr(10).join(stats_lines)}\n\n"
        f"[bold dim]─── district distribution ───[/]\n{gu_text}\n\n"
        f"[bold dim]─── settings ───[/]\n{config_text}\n\n"
        f"[bold dim]─── local ───[/]\n{log_text}\n\n"
        f" [dim]Ctrl+C: save and exit | P: pause/resume[/]"
    )

    return Panel(
        Text.from_markup(
            f" [bold orange1]🥕 Daangn local-life crawler[/]  [dim]Seoul full scan local[/]\n"
            f" {'─' * 56}\n\n"
            f" {progress_text}\n\n"
            f"{sections}"
        ),
        border_style="dim",
        box=box.ROUNDED,
        padding=(1, 2),
    )


def crawl_thread():
    try:
        run_crawl(
            cfg, stats,
            on_batch=on_batch,
            on_save=on_save,
            on_log=on_log,
            stop_flag=lambda: stop_event.is_set(),
            pause_flag=lambda: pause_event.is_set(),
        )
    except Exception as e:
        on_log(f"[red]error: {e}[/]")


def export_excel():
    """convert current CSV to Excel"""
    if not os.path.exists(cfg.output_csv):
        return None
    try:
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Seoul local-life"
        with open(cfg.output_csv, encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            for row in reader:
                ws.append(row)
        xlsx_path = cfg.output_csv.replace(".csv", ".xlsx")
        wb.save(xlsx_path)
        return xlsx_path
    except ImportError:
        return None


def main():
    console.clear()
    console.print(f"\n [bold orange1]🥕 Daangn local-life crawler[/]\n")

    # ── chunk local ─────────────────────────────────────────
    chunk_raw = args.chunk
    if chunk_raw is None and "--start" not in sys.argv and "--end" not in sys.argv:
        # interactive chunk selection
        try:
            s_dbid, e_dbid, label = select_chunk_interactive()
        except KeyboardInterrupt:
            return
        cfg.start_dbid = s_dbid
        cfg.end_dbid   = e_dbid
        if args.output == "daangn_seoul.csv":
            cfg.output_csv    = f"daangn_chunk_{label}.csv"
            cfg.progress_file = f"daangn_chunk_{label}_progress.json"
    elif chunk_raw is not None:
        # --chunk specified by argument
        chunks = _all_chunks()
        try:
            c_from, c_to = _parse_chunk_range(chunk_raw, len(chunks))
        except ValueError:
            console.print(f"[red]--chunk invalid range: {chunk_raw}[/]")
            return
        cfg.start_dbid = chunks[c_from - 1][1]
        cfg.end_dbid   = chunks[c_to   - 1][2]
        label = chunk_raw.replace(" ", "")
        if args.output == "daangn_seoul.csv":
            cfg.output_csv    = f"daangn_chunk_{label}.csv"
            cfg.progress_file = f"daangn_chunk_{label}_progress.json"
        console.print(
            f"  chunk {c_from}~{c_to}  "
            f"(dbId {cfg.end_dbid:,} ~ {cfg.start_dbid:,})\n"
        )
    # ──────────────────────────────────────────────────────

    # ── output format local ────────────────────────────────────
    if args.format:
        output_format = args.format
    else:
        console.print("  select output format.")
        console.print("  [bold]1[/]) CSV   [bold]2[/]) Excel")
        console.print()
        while True:
            fmt_input = input("  format (1/2): ").strip()
            if fmt_input == "1":
                output_format = "csv"
                break
            elif fmt_input == "2":
                output_format = "xlsx"
                break
            else:
                console.print("  [red]Enter 1 or 2.[/]")
        console.print()
    # ──────────────────────────────────────────────────────

    # Reset chunk output if requested.
    if args.reset:
        for f in [cfg.progress_file, cfg.output_csv]:
            if os.path.exists(f):
                os.remove(f)

    # start notice
    total_scans = max((cfg.start_dbid - cfg.end_dbid) // cfg.step, 1)
    console.print(f"  Step={cfg.step} → scan ~{fmt_num(total_scans)}cases, Seoul ~{fmt_num(int(total_scans*0.187))}cases")
    console.print(f"  estimated duration: ~{fmt_time(total_scans / 34000 * 3600)}\n")

    # previous progress check
    if os.path.exists(cfg.progress_file):
        import json
        with open(cfg.progress_file) as f:
            p = json.load(f)
        console.print(f"  [yellow]⚡ previous progress found: scan={p['scanned']:,}, Seoul={p['collected']:,}[/]")
        console.print(f"     resume; to start over {cfg.progress_file} delete and rerun\n")

    console.print(f"  [dim]Press Enter to start, Ctrl+C to exit[/]")
    try:
        input()
    except KeyboardInterrupt:
        return

    # start crawling thread
    t = threading.Thread(target=crawl_thread, daemon=True)
    t.start()

    # UI loop
    try:
        with Live(build_display(), console=console, refresh_per_second=2) as live:
            while t.is_alive():
                live.update(build_display())
                time.sleep(0.5)
            live.update(build_display())
    except KeyboardInterrupt:
        on_log("[yellow]interrupt requested, saving...[/]")
        stop_event.set()
        t.join(timeout=10)

    # completion summary
    console.print(f"\n [bold]complete[/]: {fmt_num(stats.scanned)}cases scan → [orange1]{fmt_num(stats.collected)}cases[/] Seoul local")

    if stats.collected > 0:
        if output_format == "xlsx":
            xlsx = export_excel()
            if xlsx:
                os.remove(cfg.output_csv)
                console.print(f" save: {xlsx}")
            else:
                console.print(f" [red]Excel conversion failed (pip install openpyxl). CSV saved: {cfg.output_csv}[/]")
        else:
            console.print(f" save: {cfg.output_csv}")

    console.print()


if __name__ == "__main__":
    main()
