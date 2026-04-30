"""
Pipeline logging 工具

每次 pipeline 執行建立獨立 log 檔（方便按問題 debug），
同時自動清理舊檔，預設只保留最近 50 個。
可透過環境變數 LOG_MAX_FILES 調整上限。

Thread-safe：並行 eval 時每個 thread 用獨立的 file handler。
"""

import logging
import os
import threading
from datetime import datetime
from pathlib import Path

from config import DEBUG

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

LOG_MAX_FILES = int(os.getenv("LOG_MAX_FILES", "50"))

_logger = logging.getLogger("pipeline")
_logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)
_logger.handlers.clear()

_console = logging.StreamHandler()
_console.setLevel(logging.DEBUG if DEBUG else logging.INFO)
_console.setFormatter(logging.Formatter("%(message)s"))
_logger.addHandler(_console)

_lock = threading.Lock()
_thread_handlers: dict[int, logging.FileHandler] = {}


def _cleanup_old_logs():
    """保留最近 LOG_MAX_FILES 個 log 檔，刪除其餘"""
    try:
        logs = sorted(LOG_DIR.glob("pipeline_*.log"), key=lambda p: p.stat().st_mtime)
    except OSError:
        return
    to_delete = logs[:-LOG_MAX_FILES] if len(logs) > LOG_MAX_FILES else []
    # Collect active log file paths to avoid deleting them
    active_files = set()
    with _lock:
        for h in _thread_handlers.values():
            try:
                active_files.add(Path(h.baseFilename).resolve())
            except Exception:
                pass
    for f in to_delete:
        if f.resolve() in active_files:
            continue
        try:
            f.unlink()
        except OSError:
            pass


def init_run_logger(question: str):
    """為每次 pipeline 執行建立獨立的 log 檔，並清理舊檔。Thread-safe。"""
    tid = threading.get_ident()

    with _lock:
        # 移除此 thread 之前的 handler
        old_handler = _thread_handlers.pop(tid, None)
        if old_handler:
            _logger.removeHandler(old_handler)
            old_handler.close()

    _cleanup_old_logs()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_q = (
        question[:30]
        .replace("/", "_")
        .replace(" ", "_")
        .replace("?", "")
        .replace("？", "")
    )
    log_file = LOG_DIR / f"pipeline_{ts}_{tid}_{safe_q}.log"
    handler = logging.FileHandler(log_file, encoding="utf-8")
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter("%(asctime)s | %(message)s", datefmt="%H:%M:%S")
    )

    with _lock:
        _thread_handlers[tid] = handler
        _logger.addHandler(handler)

    _logger.info(f"📁 Log: {log_file}")
    _logger.info(f"❓ Question: {question}")
