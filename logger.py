"""
Pipeline logging 工具

每次 pipeline 執行建立獨立 log 檔（方便按問題 debug），
同時自動清理舊檔，預設只保留最近 50 個。
可透過環境變數 LOG_MAX_FILES 調整上限。
"""

import logging
import os
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

_file_handler = None


def _cleanup_old_logs():
    """保留最近 LOG_MAX_FILES 個 log 檔，刪除其餘"""
    logs = sorted(LOG_DIR.glob("pipeline_*.log"), key=lambda p: p.stat().st_mtime)
    to_delete = logs[:-LOG_MAX_FILES] if len(logs) > LOG_MAX_FILES else []
    for f in to_delete:
        try:
            f.unlink()
        except OSError:
            pass


def init_run_logger(question: str):
    """為每次 pipeline 執行建立獨立的 log 檔，並清理舊檔"""
    global _file_handler
    if _file_handler:
        _logger.removeHandler(_file_handler)
        _file_handler.close()

    _cleanup_old_logs()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_q = (
        question[:30]
        .replace("/", "_")
        .replace(" ", "_")
        .replace("?", "")
        .replace("？", "")
    )
    log_file = LOG_DIR / f"pipeline_{ts}_{safe_q}.log"
    _file_handler = logging.FileHandler(log_file, encoding="utf-8")
    _file_handler.setLevel(logging.DEBUG)
    _file_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(message)s", datefmt="%H:%M:%S")
    )
    _logger.addHandler(_file_handler)
    _logger.info(f"📁 Log: {log_file}")
    _logger.info(f"❓ Question: {question}")
