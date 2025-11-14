#!/usr/bin/env python3
"""
Publish today's daily CSV to GitHub.

- Reads node_id and timezone from config/node.yaml
- Looks for data/daily/<node_id>_YYYY-MM-DD.csv (local date)
- Copies it to published/<node_id>/<node_id>_YYYY-MM-DD.csv
- Runs git add / commit / push from the repo root

Intended to be run periodically (e.g., via systemd timer) so GitHub
always has an up-to-date daily CSV.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import yaml
from zoneinfo import ZoneInfo


def load_config(root: Path) -> Dict[str, Any]:
    cfg_path = root / "config" / "node.yaml"
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logging(root: Path) -> logging.Logger:
    log_dir = root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "publish.log"

    logger = logging.getLogger("emis.publish")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(fh)

        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(sh)

    return logger


def main() -> None:
    root = Path(__file__).resolve().parents[1]  # project root
    log = setup_logging(root)

    cfg = load_config(root)
    node_id: str = cfg.get("node_id", "NodeX")
    tz_name: str = cfg.get("timezone", "UTC")

    # Determine today's date in the node's local timezone
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz)
    date_str = now_local.date().isoformat()

    daily_dir = root / "data" / "daily"
    src_path = daily_dir / f"{node_id}_{date_str}.csv"

    if not src_path.exists():
        log.info(f"No daily file found for today ({date_str}): {src_path}")
        return

    # Destination in the repo under published/<node_id>/
    pub_dir = root / "published" / node_id
    pub_dir.mkdir(parents=True, exist_ok=True)
    dest_path = pub_dir / f"{node_id}_{date_str}.csv"

    log.info(f"Copying {src_path} -> {dest_path}")
    shutil.copy2(src_path, dest_path)

    # Now run git add / commit / push from repo root
    rel_dest = dest_path.relative_to(root)

    log.info(f"Running git add {rel_dest}")
    add_result = subprocess.run(
        ["git", "add", str(rel_dest)],
        cwd=root,
        check=False,
    )
    if add_result.returncode != 0:
        log.warning(f"git add returned {add_result.returncode}")

    # Commit; it's OK if there's nothing to commit
    commit_msg = f"Update {node_id} {date_str}"
    log.info(f"Running git commit -m '{commit_msg}'")
    commit_result = subprocess.run(
        ["git", "commit", "-m", commit_msg],
        cwd=root,
        check=False,
    )
    if commit_result.returncode != 0:
        log.info("git commit had no changes or failed; continuing to push anyway (if needed).")

    log.info("Running git push")
    push_result = subprocess.run(
        ["git", "push"],
        cwd=root,
        check=False,
    )
    if push_result.returncode != 0:
        log.warning(f"git push returned {push_result.returncode}")

    log.info("Publish run complete.")


if __name__ == "__main__":
    main()

