from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import openpyxl

import video_dedupe as vd


DONE_STATUS_VALUES = {"done", "complete", "completed", "archived"}


@dataclass
class RenameAction:
    sheet_name: str
    row_index: int
    source_path: str
    target_path: str
    proposed_new_name: str
    workflow_status: str


@dataclass
class RemuxAction:
    sheet_name: str
    row_index: int
    source_path: str
    output_path: str
    start_s: float
    end_s: float
    proposed_output_name: str
    workflow_status: str


@dataclass(frozen=True)
class KeyframeAlignedClip:
    requested_start_s: float
    requested_end_s: float
    remux_start_s: float
    remux_end_s: float
    padded_head_s: float
    padded_tail_s: float


def _run_subprocess(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def ffprobe_duration_seconds(path: str) -> float:
    proc = _run_subprocess(
        [
            "ffprobe",
            "-hide_banner",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            path,
        ]
    )
    if proc.returncode != 0:
        raise RuntimeError(f"ffprobe duration failed for {path}:\n{proc.stderr}")
    try:
        return float(proc.stdout.strip())
    except ValueError as exc:
        raise RuntimeError(f"Could not parse duration for {path}") from exc


def get_video_keyframes(path: str) -> list[float]:
    proc = _run_subprocess(
        [
            "ffprobe",
            "-hide_banner",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-skip_frame",
            "nokey",
            "-show_frames",
            "-show_entries",
            "frame=best_effort_timestamp_time,pkt_dts_time,pkt_pts_time",
            "-of",
            "json",
            path,
        ]
    )
    if proc.returncode != 0:
        raise RuntimeError(f"ffprobe keyframes failed for {path}:\n{proc.stderr}")
    try:
        data = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Could not decode ffprobe keyframes for {path}") from exc
    out: list[float] = []
    for frame in data.get("frames", []):
        if not isinstance(frame, dict):
            continue
        val = frame.get("best_effort_timestamp_time")
        if val in (None, ""):
            val = frame.get("pkt_pts_time")
        if val in (None, ""):
            val = frame.get("pkt_dts_time")
        if val in (None, ""):
            continue
        try:
            t_s = float(val)
        except Exception:
            continue
        if math.isfinite(t_s):
            out.append(t_s)
    return sorted(set(round(x, 6) for x in out if x >= 0.0))


def align_clip_to_keyframes(
    path: str,
    *,
    start_s: float,
    end_s: float,
    keyframes: list[float] | None = None,
) -> KeyframeAlignedClip:
    req_start = max(0.0, float(start_s))
    req_end = max(req_start, float(end_s))
    duration_s = ffprobe_duration_seconds(path)
    req_end = min(req_end, duration_s)
    if keyframes is None:
        keyframes = get_video_keyframes(path)
    if not keyframes:
        return KeyframeAlignedClip(
            requested_start_s=req_start,
            requested_end_s=req_end,
            remux_start_s=req_start,
            remux_end_s=req_end,
            padded_head_s=0.0,
            padded_tail_s=0.0,
        )
    prev_keys = [t for t in keyframes if t <= req_start]
    next_keys = [t for t in keyframes if t >= req_end]
    remux_start = prev_keys[-1] if prev_keys else 0.0
    remux_end = next_keys[0] if next_keys else duration_s
    remux_start = max(0.0, min(remux_start, duration_s))
    remux_end = max(remux_start, min(remux_end, duration_s))
    return KeyframeAlignedClip(
        requested_start_s=req_start,
        requested_end_s=req_end,
        remux_start_s=remux_start,
        remux_end_s=remux_end,
        padded_head_s=max(0.0, req_start - remux_start),
        padded_tail_s=max(0.0, remux_end - req_end),
    )


def remux_clip_lossless(
    input_path: str,
    output_path: str,
    *,
    start_s: float,
    end_s: float,
    align_to_keyframes: bool = True,
    overwrite: bool = True,
) -> KeyframeAlignedClip:
    input_path = str(input_path)
    output_path = str(output_path)
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    aligned = align_clip_to_keyframes(input_path, start_s=start_s, end_s=end_s) if align_to_keyframes else KeyframeAlignedClip(
        requested_start_s=max(0.0, float(start_s)),
        requested_end_s=max(0.0, float(end_s)),
        remux_start_s=max(0.0, float(start_s)),
        remux_end_s=max(0.0, float(end_s)),
        padded_head_s=0.0,
        padded_tail_s=0.0,
    )
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-nostdin",
        "-y" if overwrite else "-n",
        "-ss",
        f"{aligned.remux_start_s:.3f}",
        "-to",
        f"{aligned.remux_end_s:.3f}",
        "-i",
        input_path,
        "-map",
        "0",
        "-c",
        "copy",
        "-avoid_negative_ts",
        "make_zero",
        output_path,
    ]
    proc = _run_subprocess(cmd)
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg remux failed for {input_path}:\n{proc.stderr}")
    return aligned


def _safe_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value.strip()
        return "" if text.lower() in {"nan", "nat", "none"} else text
    return str(value).strip()


def _coerce_seconds(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if hasattr(value, "total_seconds"):
        try:
            return float(value.total_seconds())
        except Exception:
            return None
    if isinstance(value, (int, float)):
        if math.isnan(float(value)):
            return None
        return float(value)
    text = _safe_text(value)
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        pass
    parts = text.split(":")
    if len(parts) == 3:
        try:
            return (float(parts[0]) * 3600.0) + (float(parts[1]) * 60.0) + float(parts[2])
        except Exception:
            return None
    return None


def _normalize_done_status(value: str) -> bool:
    norm = "".join(ch for ch in _safe_text(value).lower() if ch.isalpha())
    return norm in DONE_STATUS_VALUES


def _sanitize_filename(name: str) -> str:
    bad = '<>:"/\\|?*'
    out = "".join("_" if ch in bad else ch for ch in _safe_text(name))
    return out.strip().rstrip(". ")


def _split_ext(name: str, default_ext: str) -> tuple[str, str]:
    raw = _sanitize_filename(name)
    base, ext = os.path.splitext(raw)
    if ext:
        return base or raw, ext
    return raw, default_ext


def _format_hms(seconds: float) -> str:
    total = max(0, int(round(float(seconds))))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:02d}-{m:02d}-{s:02d}"


def _apply_path_maps(path: str, mappings: list[tuple[str, str]]) -> str:
    raw = str(path)
    norm_raw = os.path.normcase(os.path.normpath(raw))
    best: tuple[int, str] | None = None
    for src, dst in mappings:
        norm_src = os.path.normcase(os.path.normpath(src))
        if norm_raw == norm_src or norm_raw.startswith(norm_src + os.sep):
            suffix = raw[len(src):].lstrip("\\/")
            candidate = os.path.join(dst, suffix) if suffix else dst
            rank = len(norm_src)
            if best is None or rank > best[0]:
                best = (rank, candidate)
    return best[1] if best else raw


def _open_hash_store(cache_path: str | None) -> vd.VideoHashStore:
    desired = os.path.abspath(cache_path or vd.VIDEO_HASH_STORE_PATH)
    current = getattr(vd.VideoHashStore, "_inst", None)
    if current is not None and os.path.abspath(getattr(current, "path", "")) != desired:
        vd.VideoHashStore._inst = None
    return vd.VideoHashStore(desired)


def _iter_sheet_rows(ws) -> Iterable[tuple[int, dict[str, object]]]:
    headers = [_safe_text(c.value) for c in ws[1]]
    for row_idx in range(2, ws.max_row + 1):
        values = [ws.cell(row=row_idx, column=i + 1).value for i in range(len(headers))]
        row = dict(zip(headers, values))
        yield row_idx, row


def _collect_rename_actions(
    workbook_path: str,
    *,
    path_maps: list[tuple[str, str]],
) -> list[RenameAction]:
    wb = openpyxl.load_workbook(workbook_path, data_only=False)
    actions: list[RenameAction] = []
    for sheet_name in ("Rename_Queue", "Rename_Done"):
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]
        for row_idx, row in _iter_sheet_rows(ws):
            source_path = _safe_text(row.get("file_path"))
            proposed = _safe_text(row.get("proposed_new_name"))
            if not source_path or not proposed:
                continue
            workflow_status = _safe_text(row.get("workflow_status"))
            mapped_source = _apply_path_maps(source_path, path_maps)
            base_name, ext = _split_ext(proposed, Path(source_path).suffix or ".mkv")
            target_name = base_name + ext
            target_path = str(Path(mapped_source).with_name(target_name))
            actions.append(
                RenameAction(
                    sheet_name=sheet_name,
                    row_index=row_idx,
                    source_path=mapped_source,
                    target_path=target_path,
                    proposed_new_name=proposed,
                    workflow_status=workflow_status,
                )
            )
    return actions


def _derive_remux_output_name(source_path: str, row: dict[str, object], start_s: float, end_s: float) -> str:
    proposed = _safe_text(row.get("proposed_output_name"))
    if proposed:
        return proposed
    stem = Path(source_path).stem
    seg_idx = _safe_text(row.get("segment_index")) or "seg"
    return f"{stem}__seg{seg_idx}__{_format_hms(start_s)}__{_format_hms(end_s)}"


def _collect_remux_actions(
    workbook_path: str,
    *,
    path_maps: list[tuple[str, str]],
    output_root: str,
    include_short: bool,
) -> list[RemuxAction]:
    wb = openpyxl.load_workbook(workbook_path, data_only=False)
    actions: list[RemuxAction] = []
    sheet_names = ["Remux_Plan"] + (["Remux_Short"] if include_short else [])
    for sheet_name in sheet_names:
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]
        for row_idx, row in _iter_sheet_rows(ws):
            source_path = _safe_text(row.get("source_file_path"))
            start_s = _coerce_seconds(row.get("segment_start_s"))
            end_s = _coerce_seconds(row.get("segment_end_s"))
            if not source_path or start_s is None or end_s is None or end_s <= start_s:
                continue
            workflow_status = _safe_text(row.get("workflow_status"))
            mapped_source = _apply_path_maps(source_path, path_maps)
            output_name = _derive_remux_output_name(source_path, row, start_s, end_s)
            base_name, ext = _split_ext(output_name, ".mkv")
            output_path = str(Path(output_root) / f"{base_name}{ext or '.mkv'}")
            actions.append(
                RemuxAction(
                    sheet_name=sheet_name,
                    row_index=row_idx,
                    source_path=mapped_source,
                    output_path=output_path,
                    start_s=float(start_s),
                    end_s=float(end_s),
                    proposed_output_name=_safe_text(row.get("proposed_output_name")),
                    workflow_status=workflow_status,
                )
            )
    return actions


def execute_rename_actions(
    actions: list[RenameAction],
    *,
    cache_path: str | None,
    apply_changes: bool,
    overwrite: bool,
) -> list[dict[str, object]]:
    vhs = _open_hash_store(cache_path)
    results: list[dict[str, object]] = []
    for action in actions:
        status = "planned"
        message = ""
        source = Path(action.source_path)
        target = Path(action.target_path)
        if _normalize_done_status(action.workflow_status):
            message = "workflow_status already marked done; still eligible if source exists"
        if not source.exists():
            status = "missing_source"
            message = "source file does not exist"
        elif source.resolve() == target.resolve():
            status = "noop"
            message = "source already has target name"
        elif target.exists() and not overwrite:
            status = "target_exists"
            message = "target already exists"
        elif apply_changes:
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists() and overwrite:
                raise FileExistsError(f"Refusing to overwrite existing rename target: {target}")
            source.rename(target)
            moved = vhs.rename_path_key(str(source), str(target), overwrite_existing=False)
            status = "renamed"
            message = "cache entry moved" if moved else "no cache entry to move"
        results.append(
            {
                "sheet": action.sheet_name,
                "row": action.row_index,
                "source_path": str(source),
                "target_path": str(target),
                "status": status,
                "message": message,
            }
        )
    if apply_changes:
        vhs.save_if_dirty()
    return results


def execute_remux_actions(
    actions: list[RemuxAction],
    *,
    apply_changes: bool,
    overwrite: bool,
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    for action in actions:
        source = Path(action.source_path)
        target = Path(action.output_path)
        status = "planned"
        message = ""
        aligned: KeyframeAlignedClip | None = None
        if _normalize_done_status(action.workflow_status):
            message = "workflow_status already marked done; row still processed if requested"
        if not source.exists():
            status = "missing_source"
            message = "source file does not exist"
        elif target.exists() and not overwrite:
            status = "target_exists"
            message = "target already exists"
        elif apply_changes:
            aligned = remux_clip_lossless(
                str(source),
                str(target),
                start_s=action.start_s,
                end_s=action.end_s,
                align_to_keyframes=True,
                overwrite=overwrite,
            )
            status = "remuxed"
        results.append(
            {
                "sheet": action.sheet_name,
                "row": action.row_index,
                "source_path": str(source),
                "output_path": str(target),
                "start_s": action.start_s,
                "end_s": action.end_s,
                "status": status,
                "message": message,
                "aligned": {
                    "requested_start_s": aligned.requested_start_s,
                    "requested_end_s": aligned.requested_end_s,
                    "remux_start_s": aligned.remux_start_s,
                    "remux_end_s": aligned.remux_end_s,
                    "padded_head_s": aligned.padded_head_s,
                    "padded_tail_s": aligned.padded_tail_s,
                } if aligned else None,
            }
        )
    return results


def _parse_path_map(values: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for raw in values:
        if "=" not in raw:
            raise ValueError(f"Invalid --path-map value: {raw}")
        src, dst = raw.split("=", 1)
        src = src.strip().strip('"')
        dst = dst.strip().strip('"')
        if not src or not dst:
            raise ValueError(f"Invalid --path-map value: {raw}")
        out.append((src, dst))
    return out


def _write_manifest(path: str | None, payload: dict[str, object]) -> None:
    if not path:
        return
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


def run_workflow(
    *,
    workbook: str = str(Path("reports") / "dedupe_consolidated.xlsx"),
    mode: str = "all",
    apply: bool = False,
    overwrite: bool = False,
    include_short: bool = False,
    output_root: str = str(Path("Other") / "remux_output"),
    cache_path: str = vd.VIDEO_HASH_STORE_PATH,
    manifest: str | None = str(Path("reports") / "workflow_run_manifest.json"),
    path_map: list[str] | None = None,
) -> int:
    workbook = Path(workbook)
    if not workbook.exists():
        raise FileNotFoundError(f"Workbook not found: {workbook}")

    if mode not in {"rename", "remux", "all"}:
        raise ValueError(f"Unsupported mode: {mode}")

    path_maps = _parse_path_map(list(path_map or []))
    payload: dict[str, object] = {
        "workbook": str(workbook),
        "mode": mode,
        "apply": bool(apply),
        "path_maps": path_maps,
        "rename_results": [],
        "remux_results": [],
    }

    if mode in {"rename", "all"}:
        rename_actions = _collect_rename_actions(str(workbook), path_maps=path_maps)
        payload["rename_results"] = execute_rename_actions(
            rename_actions,
            cache_path=cache_path,
            apply_changes=bool(apply),
            overwrite=bool(overwrite),
        )

    if mode in {"remux", "all"}:
        remux_actions = _collect_remux_actions(
            str(workbook),
            path_maps=path_maps,
            output_root=output_root,
            include_short=bool(include_short),
        )
        payload["remux_results"] = execute_remux_actions(
            remux_actions,
            apply_changes=bool(apply),
            overwrite=bool(overwrite),
        )

    _write_manifest(manifest, payload)

    rename_done = sum(1 for row in payload["rename_results"] if row.get("status") == "renamed")
    remux_done = sum(1 for row in payload["remux_results"] if row.get("status") == "remuxed")
    print(
        f"[workflow] mode={mode} apply={'yes' if apply else 'no'} "
        f"rename_actions={len(payload['rename_results'])} renamed={rename_done} "
        f"remux_actions={len(payload['remux_results'])} remuxed={remux_done}",
        flush=True,
    )
    if manifest:
        print(f"[workflow] manifest: {manifest}", flush=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Execute workbook-driven rename and remux actions.")
    parser.add_argument("--workbook", default=str(Path("reports") / "dedupe_consolidated.xlsx"))
    parser.add_argument("--mode", choices=["rename", "remux", "all"], default="all")
    parser.add_argument("--apply", action="store_true", help="Perform filesystem changes. Default is dry-run.")
    parser.add_argument("--overwrite", action="store_true", help="Allow remux outputs to overwrite existing files.")
    parser.add_argument("--include-short", action="store_true", help="Include Remux_Short rows as well as Remux_Plan.")
    parser.add_argument("--output-root", default=str(Path("Other") / "remux_output"))
    parser.add_argument("--cache-path", default=vd.VIDEO_HASH_STORE_PATH)
    parser.add_argument("--manifest", default=str(Path("reports") / "workflow_run_manifest.json"))
    parser.add_argument("--path-map", action="append", default=[], help="Prefix remap OLD=NEW for safe test runs.")
    args = parser.parse_args()
    return run_workflow(
        workbook=args.workbook,
        mode=args.mode,
        apply=bool(args.apply),
        overwrite=bool(args.overwrite),
        include_short=bool(args.include_short),
        output_root=args.output_root,
        cache_path=args.cache_path,
        manifest=args.manifest,
        path_map=list(args.path_map),
    )


if __name__ == "__main__":
    print()
    # PyCharm-friendly run block. Edit values below, then Run this file directly.
    WORKFLOW_CONFIG = {
        "workbook": str(Path("reports") / "dedupe_consolidated.xlsx"),
        "mode": "all",  # "rename", "remux", or "all"
        "apply": False,  # set True to perform real renames/remuxes
        "overwrite": False,
        "include_short": False,
        "output_root": str(Path("Other") / "remux_output"),
        "cache_path": vd.VIDEO_HASH_STORE_PATH,
        "manifest": str(Path("reports") / "workflow_run_manifest.json"),
        # Example safe test remap:
        # "path_map": [
        #     r"D:\source_a=C:\temp\test\file_a",
        #     r"D:\source_b=C:\temp\test\file_b",
        # ],
        "path_map": [],
    }
    raise SystemExit(run_workflow(**WORKFLOW_CONFIG))
