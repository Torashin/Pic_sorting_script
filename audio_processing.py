"""
audio_processing.py

Fast audio silence detection for head/tail trimming.

Key functions:
- detect_head_tail_silence_ffmpeg: use FFmpeg 'silencedetect' on just the head and tail
- merge_close_regions: utility to merge tiny gaps
- (Optional) compute_audio_rms / detect_silent_regions are kept for fallback or debugging

Run directly in PyCharm by editing the 'filepath' variable at the bottom.
"""

from __future__ import annotations
import math
import re
import subprocess
import sys
from typing import List, Tuple

import numpy as np

# -------------------------
# FFmpeg helpers
# -------------------------

def _run_ffmpeg(cmd: list[str]) -> tuple[int, bytes, bytes]:
    try:
        proc = subprocess.run(
            cmd,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except FileNotFoundError:
        raise RuntimeError(
            "FFmpeg not found. Install it and ensure 'ffmpeg' is on your PATH."
        )


def ffprobe_duration_seconds(filepath: str) -> float:
    # Probe duration (format duration fallback if audio stream query fails)
    cmd = [
        "ffprobe", "-hide_banner",
        "-select_streams", "a:0",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        filepath,
    ]
    code, out, err = _run_ffmpeg(cmd)
    if code != 0:
        cmd = [
            "ffprobe", "-hide_banner",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            filepath,
        ]
        code, out, err = _run_ffmpeg(cmd)
    if code != 0:
        raise RuntimeError("ffprobe failed to read duration:\n" + err.decode("utf-8", "ignore"))
    try:
        return float(out.decode("utf-8", "ignore").strip())
    except ValueError:
        raise RuntimeError("Could not parse duration from ffprobe.")


# -------------------------
# FAST: head/tail silence via silencedetect
# -------------------------

_silence_start_re = re.compile(r"silence_start:\s*([0-9.]+)")
_silence_end_re   = re.compile(r"silence_end:\s*([0-9.]+)\s*\|\s*silence_duration:\s*([0-9.]+)")

def _parse_silencedetect(stderr_text: str, t0_offset: float = 0.0) -> List[Tuple[float, float]]:
    """Parse FFmpeg silencedetect output (stderr) into [(start_s, end_s)]."""
    regions: List[Tuple[float, float]] = []
    pending_start: float | None = None

    for line in stderr_text.splitlines():
        m_start = _silence_start_re.search(line)
        if m_start:
            pending_start = float(m_start.group(1)) + t0_offset
            continue
        m_end = _silence_end_re.search(line)
        if m_end:
            end = float(m_end.group(1)) + t0_offset
            dur = float(m_end.group(2))
            if pending_start is None:
                # If we began the window mid-silence, FFmpeg can emit an end without a start.
                pending_start = end - dur + t0_offset * 0.0  # explicit to show intent
            regions.append((pending_start, end))
            pending_start = None

    return regions


def detect_head_tail_silence_ffmpeg(
    filepath: str,
    *,
    noise_thresh_db: float = -55.0,
    min_silence_s: float = 1.5,
    head_scan_s: float = 90.0,
    tail_scan_s: float = 1800.0,  # 30 minutes; adjust if your tails can be longer
    downmix_mono: bool = True,
) -> dict:
    """
    Use FFmpeg 'silencedetect' to find silence only in the head and tail windows.

    Returns:
        {
          'duration_s': float,
          'head_window': (0.0, head_end),
          'tail_window': (tail_start, duration),
          'head_silences': [(s,e), ...]  # in absolute timeline seconds
          'tail_silences': [(s,e), ...]
        }
    """
    duration = ffprobe_duration_seconds(filepath)
    head_end = min(head_scan_s, duration)
    tail_start = max(0.0, duration - tail_scan_s)

    # Build common filter: optional downmix, then silencedetect
    # Note: keep verbosity high enough ('-v info') so silencedetect messages are emitted.
    adown = "pan=mono|c0=0.5*c0+0.5*c1," if downmix_mono else ""
    afilter = f"{adown}silencedetect=noise={noise_thresh_db}dB:d={min_silence_s}"

    # HEAD window (0 -> head_end)
    cmd_head = [
        "ffmpeg", "-hide_banner", "-nostdin",
        "-v", "info",
        "-ss", "0.0", "-t", f"{head_end:.3f}",
        "-i", filepath,
        "-vn", "-af", afilter,
        "-f", "null", "-"
    ]
    code_h, out_h, err_h = _run_ffmpeg(cmd_head)
    if code_h != 0:
        raise RuntimeError("FFmpeg silencedetect (head) failed:\n" + err_h.decode("utf-8", "ignore"))
    head_regions = _parse_silencedetect(err_h.decode("utf-8", "ignore"), t0_offset=0.0)

    # TAIL window (tail_start -> EOF). Use input-seek so we don't decode the whole file.
    cmd_tail = [
        "ffmpeg", "-hide_banner", "-nostdin",
        "-v", "info",
        "-ss", f"{tail_start:.3f}",
        "-i", filepath,
        "-vn", "-af", afilter,
        "-f", "null", "-"
    ]
    code_t, out_t, err_t = _run_ffmpeg(cmd_tail)
    if code_t != 0:
        raise RuntimeError("FFmpeg silencedetect (tail) failed:\n" + err_t.decode("utf-8", "ignore"))
    tail_regions = _parse_silencedetect(err_t.decode("utf-8", "ignore"), t0_offset=tail_start)

    return {
        "duration_s": round(duration, 1),
        "head_window": (round(0.0, 1), round(head_end, 1)),
        "tail_window": (round(tail_start, 1), round(duration, 1)),
        "head_silences": head_regions,
        "tail_silences": tail_regions,
    }


def merge_close_regions(
    regions: List[Tuple[float, float]],
    *,
    min_duration_s: float = 1.5,
    max_gap_s: float = 0.8,
) -> List[Tuple[float, float]]:
    if not regions:
        return []
    regions = sorted(regions, key=lambda r: r[0])
    merged: List[Tuple[float, float]] = []
    cur_s, cur_e = regions[0]
    for s, e in regions[1:]:
        if s - cur_e <= max_gap_s:
            cur_e = max(cur_e, e)
        else:
            if (cur_e - cur_s) >= min_duration_s:
                merged.append((round(cur_s, 1), round(cur_e, 1)))
            cur_s, cur_e = s, e
    if (cur_e - cur_s) >= min_duration_s:
        merged.append((round(cur_s, 1), round(cur_e, 1)))
    return merged


# -------------------------
# (Optional) PCM-based fallback: keep for reference / debugging
# -------------------------

def read_audio_ffmpeg_pcm(filepath: str, target_sr: int = 8000) -> tuple[np.ndarray, int]:
    """Decode to mono PCM s16le via FFmpeg at a low sample rate (fast)."""
    cmd = [
        "ffmpeg", "-hide_banner",
        "-i", filepath,
        "-vn", "-ac", "1", "-ar", str(target_sr),
        "-f", "s16le", "-acodec", "pcm_s16le", "pipe:1"
    ]
    code, out, err = _run_ffmpeg(cmd)
    if code != 0 or len(out) == 0:
        raise RuntimeError("FFmpeg PCM decode failed:\n" + err.decode("utf-8", "ignore"))
    y = np.frombuffer(out, dtype=np.int16).astype(np.float32) / 32768.0
    return y, target_sr


def compute_audio_rms(filepath: str, win_s: float = 0.5, hop_s: float = 0.2, target_sr: int = 8000) -> List[Tuple[float, float, float]]:
    y, sr = read_audio_ffmpeg_pcm(filepath, target_sr=target_sr)
    win = max(1, int(round(win_s * sr)))
    hop = max(1, int(round(hop_s * sr)))
    if y.size < win:
        rms = float(np.sqrt(np.mean(y * y) + 1e-12))
        db = 20.0 * math.log10(max(rms, 1e-12))
        return [(0.0, y.size / sr, db)]
    y2 = np.square(y, dtype=np.float32)
    kernel = np.ones(win, dtype=np.float32) / float(win)
    mov = np.convolve(y2, kernel, mode="valid")
    rms_vec = np.sqrt(mov + 1e-12)
    idx = np.arange(0, rms_vec.size, hop, dtype=np.int64)
    rms_vals = rms_vec[idx]
    starts = idx / sr
    ends = (idx + win) / sr
    db = 20.0 * np.log10(np.maximum(rms_vals, 1e-12))
    return [(float(s), float(e), float(d)) for s, e, d in zip(starts, ends, db)]


def detect_silent_regions(filepath: str, rms_win_s: float = 0.5, rms_hop_s: float = 0.2, rms_thresh_db: float = -55.0, min_silence_s: float = 1.5, target_sr: int = 8000) -> List[Tuple[float, float]]:
    frames = compute_audio_rms(filepath, win_s=rms_win_s, hop_s=rms_hop_s, target_sr=target_sr)
    regions: List[Tuple[float, float]] = []
    cur_start: float | None = None
    for s, e, db in frames:
        if db <= rms_thresh_db:
            if cur_start is None:
                cur_start = s
        else:
            if cur_start is not None and (s - cur_start) >= min_silence_s:
                regions.append((cur_start, s))
                cur_start = None
    if cur_start is not None:
        last_end = frames[-1][1]
        if (last_end - cur_start) >= min_silence_s:
            regions.append((cur_start, last_end))
    return regions


def print_silence_report(filepath: str, *, do_full_scan: bool = False) -> None:
    print("Fast head/tail silence via FFmpeg silencedetect…")
    try:
        result = detect_head_tail_silence_ffmpeg(
            filepath,
            noise_thresh_db=-55.0,
            min_silence_s=1.5,
            head_scan_s=90.0,
            tail_scan_s=1800.0,
            downmix_mono=True,
        )
    except RuntimeError as e:
        print(str(e))
        return

    dur = result["duration_s"]
    print(f"Duration: {dur:.1f}s")
    print(f"Head window: {result['head_window']}")
    print(f"Tail window: {result['tail_window']}")

    head = merge_close_regions(result["head_silences"], min_duration_s=1.5, max_gap_s=0.8)
    tail = merge_close_regions(result["tail_silences"], min_duration_s=1.5, max_gap_s=0.8)

    if head:
        print("Head silent regions:")
        for s, e in head:
            print(f"  {s:.1f} → {e:.1f}  (dur {e - s:.1f}s)")
    else:
        print("No head silence.")

    if tail:
        print("Tail silent regions:")
        for s, e in tail:
            print(f"  {s:.1f} → {e:.1f}  (dur {e - s:.1f}s)")
    else:
        print("No tail silence.")

    if do_full_scan:
        print("\nFull-scan (fallback) at 8 kHz to compare:")
        all_sil = detect_silent_regions(filepath, target_sr=8000)
        all_sil = merge_close_regions(all_sil, min_duration_s=1.5, max_gap_s=0.8)
        for s, e in all_sil:
            print(f"  {s:.1f} → {e:.1f}")


# -------------------------
# PyCharm-friendly test harness
# -------------------------

if __name__ == "__main__":
    print()
