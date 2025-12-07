# video_dedupe.py
#
# Deduplication pipeline with THREE selectable refine modes:
#   1) "anchors" – fast, subset-aware start/end anchors (audio-guided trim + FFmpeg streaming).
#                  No legacy hashes. No FAISS. Direct pairwise compare (with duration sanity gate).
#   2) "legacy"  – original sparse sampler + 60 s global aligner. Uses FAISS for shortlisting.
#                  No anchors.
#   3) "both"    – compute both; FAISS shortlist; refine with both; union matches in one report.
#
# Key improvements:
#   • Anchor sampling via FFmpeg streaming (1 Hz grayscale) – avoids random seeks.
#   • Per-file anchors cached alongside legacy hashes in ONE JSON at resources/cache/video_cache.json.
#   • Directional relation fields: relation_dir, full_clip, subset_clip, subset_start_mm:ss.
#   • Project tidy: reports saved to reports/, cache to resources/cache/.
#
# Tuning guidance (safe defaults for noisy analog captures):
#   • anchors mode:
#       ANCHOR_WINDOW_S = 300.0   # 5 min at start & end
#       ANCHOR_STEP_S   = 1.0     # 1 Hz; raise to 2.0 for speed, drop to 0.5 for accuracy
#       ANCHOR_HAMMING_THRESH = 14   # 16 if the tapes are very noisy
#       ANCHOR_MIN_FRACTION   = 0.40 # 40% of smaller set must match
#       ANCHOR_MAX_MAD_S      = 3.0  # robust spread of offsets
#       DURATION_RATIO_MIN    = 0.20 # skip pair if min(dur)/max(dur) < 0.20
#   • legacy/both modes:
#       FAISS_THRESHOLD  = 12 (for clean digital) → try 30–45 for analog so refine gets candidates
#       ALIGN_THRESHOLD  = 20.0  # mean Hamming over 60 s window
#       ALIGN_OFFSET_LIMIT_S = 60.0
#
# Requirements:
#   • FFmpeg on PATH (used by audio_processing.py and streaming sampler).
#   • OpenCV, numpy, pandas, faiss-cpu, imagehash, pillow, openpyxl, PySimpleGUI.

from __future__ import annotations

import os
import json
import atexit
import time
from pathlib import Path
from typing import List, Tuple, Dict
import concurrent.futures
import math
import subprocess
import platform

import numpy as np
import pandas as pd
import cv2
from PIL import Image
import imagehash
import faiss
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, numbers
from pandas import ExcelWriter
import PySimpleGUI as sg

# Audio helpers – estimate head/tail silence to trim dead air (fast).
from audio_processing import detect_head_tail_silence_ffmpeg, merge_close_regions


# ────────────────────────────────────────────────────────────────────
# Paths & layout
# ────────────────────────────────────────────────────────────────────

RESOURCES_DIR = os.path.join("resources")
CACHE_DIR     = os.path.join(RESOURCES_DIR, "cache")
REPORTS_DIR   = os.path.join("reports")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

# Unified cache file: legacy avg/seq AND anchor hashes live here.
VIDEO_HASH_STORE_PATH   = os.path.join(CACHE_DIR, "video_cache.json")
# Migration: old root-level cache (auto-imported once if present).
MIGRATE_OLD_CACHE_PATH  = "video_hashes.json"

DEFAULT_REPORT_PATH     = os.path.join(REPORTS_DIR, "video_duplicates.xlsx")


# ────────────────────────────────────────────────────────────────────
# Knobs – tweak here
# ────────────────────────────────────────────────────────────────────

# Legacy sparse sampler & FAISS coarse gate
MAX_SAMPLES          = 40    # max sparse frames per video (legacy)
MIN_SAMPLES          = 5     # min sparse frames per video (legacy)
FAISS_THRESHOLD      = 12    # L2 distance on 64-bit avg hash; try 30–45 for analog
ALIGN_THRESHOLD      = 20.0  # legacy: mean per-frame Hamming over 60 s
ALIGN_OFFSET_LIMIT_S = 60.0  # legacy: max allowed time offset (s)
TOP_K                = 5     # FAISS neighbours per video
EXTS                 = {'.mp4', '.mov', '.avi', '.m4v', '.mpg', '.mkv'}
SAVE_EVERY           = 5     # save cache every N new legacy entries
MAX_WORKERS          = 8     # workers for metadata/legacy

# Anchor mode (subset-aware & fast)
ANCHOR_WINDOW_S       = 300.0   # seconds sampled at start and at end (after trim)
ANCHOR_STEP_S         = 1.0     # seconds between frames (1.0 → 1 Hz)
ANCHOR_HAMMING_THRESH = 15      # per-frame phash Hamming threshold: larger values = less strict
ANCHOR_MIN_FRACTION   = 0.35    # min fraction of frames that must match
ANCHOR_MAX_MAD_S      = 3.0     # max median absolute deviation allowed (s)
DURATION_RATIO_MIN    = 0.20    # skip pair if min(dur)/max(dur) < 0.20

# FFmpeg settings (for anchors)
FFMPEG_THREADS        = 1       # 0 = auto-threading (let FFmpeg decide); use 1 for HDDs
ANCHOR_WORKERS        = 1       # concurrent FFmpeg readers for anchors; use 1 for HDDs

# Console progress counters (just for printing)
TOTAL_VIDEOS          = 0
PROCESSED_VIDEOS      = 0



# ────────────────────────────────────────────────────────────────────
# Small helpers (bit ops, formats)
# ────────────────────────────────────────────────────────────────────

def _hex_to_vec(hex_str: str) -> np.ndarray:
    """Convert a 64-bit hex string into a 64-dim 0/1 float32 vector (legacy FAISS embedding)."""
    ba   = bytes.fromhex(hex_str)
    bits = np.unpackbits(np.frombuffer(ba, dtype=np.uint8))
    return bits.astype("float32")


def _calculate_target_samples(duration: float) -> int:
    """Legacy: choose sparse sample count via log curve; clamp to [MIN_SAMPLES, MAX_SAMPLES]."""
    if duration <= 0:
        return MIN_SAMPLES
    samples = int(math.log(duration + 1, 10) * 12)
    return max(MIN_SAMPLES, min(samples, MAX_SAMPLES))


def _average_hex(pairs: List[Tuple[str, float]]) -> str:
    """Legacy: average many 64-bit pHashes (hex) into one 64-bit hex by bit-wise majority."""
    hexes = [h for (h, _) in pairs]
    if not hexes:
        return "0" * 16
    bits = np.stack([_hex_to_vec(h) for h in hexes])
    avg_bits = (bits.mean(axis=0) >= 0.5).astype(np.uint8)
    return np.packbits(avg_bits).tobytes().hex()


def _hamming_hex(a: str, b: str) -> int:
    """Legacy: Hamming distance between two 64-bit pHashes stored as hex strings."""
    return bin(int(a, 16) ^ int(b, 16)).count("1")


def _format_mmss(seconds: float | None) -> str | None:
    """Pretty mm:ss for subset offsets; returns None if value is None/NaN/inf."""
    if seconds is None or not math.isfinite(seconds):
        return None
    s = int(round(seconds))
    m, s = divmod(abs(s), 60)
    sign = "-" if seconds < 0 else ""
    return f"{sign}{m:d}:{s:02d}"


def _order_pair(i: int, j: int, folder_ids: list[int], paths: list[str], prefer_folder: int | None) -> tuple[int, int]:
    """
    Return (a_idx, b_idx) in a consistent order.
    - If prefer_folder is set, try to put that folder on the A side when present.
    - Otherwise order by (folder_id, path) to make it stable across runs.
    """
    fi, fj = folder_ids[i], folder_ids[j]
    if prefer_folder is not None:
        if fi == prefer_folder and fj != prefer_folder:
            return i, j
        if fj == prefer_folder and fi != prefer_folder:
            return j, i
    # fallback: stable ordering by (folder, path)
    return (i, j) if (fi, paths[i]) <= (fj, paths[j]) else (j, i)


# ────────────────────────────────────────────────────────────────────
# Legacy per-video sampler (kept for "legacy" / "both")
# ────────────────────────────────────────────────────────────────────

def _sample_hashes_with_times(path: str) -> List[Tuple[str, float]]:
    """
    Legacy sampler: sparsely sample frames across the whole video and pHash them.
    Returns: [(hash_hex, timestamp_seconds), ...] up to ~MAX_SAMPLES elements.
    """
    cap = cv2.VideoCapture(path)
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video: {path}")

    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT)) or 1
    duration = frame_count / fps

    k = _calculate_target_samples(duration)
    step = max(1, int(frame_count / k))
    out: List[Tuple[str, float]] = []

    for frame_idx in range(0, frame_count, step):
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
        ok, frame = cap.read()
        if not ok:
            break
        timestamp = frame_idx / fps
        img = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
        out.append((str(imagehash.phash(img)), timestamp))
        if len(out) >= k:
            break

    cap.release()
    return out


# ────────────────────────────────────────────────────────────────────
# Legacy pair aligner
# ────────────────────────────────────────────────────────────────────

def _aligned_distance_and_time_limited(
    seq_a: List[Tuple[str, float]],
    seq_b: List[Tuple[str, float]],
    max_shift_samples: int,
    offset_limit_s: float
) -> Tuple[float, float]:
    """
    Try small temporal shifts (±max_shift_samples) and compute the mean Hamming
    of overlapping sparse samples, but only if their timestamps are within offset_limit_s.
    Returns: (best_mean_hamming, average_time_shift_seconds)
    """
    if not seq_a or not seq_b:
        return 64.0, 0.0

    best_dist = 64.0
    best_time_shift = 0.0
    L1, L2 = len(seq_a), len(seq_b)
    min_shift = max(-(L2 - 1), -max_shift_samples)
    max_shift = min(L1 - 1, max_shift_samples)

    for shift in range(min_shift, max_shift + 1):
        dists, ts_list = [], []
        for i, (ha, ta) in enumerate(seq_a):
            j = i - shift
            if 0 <= j < L2:
                hb, tb = seq_b[j]
                ts = tb - ta
                if abs(ts) <= offset_limit_s:
                    dists.append(_hamming_hex(ha, hb))
                    ts_list.append(ts)
        if dists:
            mean_d = sum(dists) / len(dists)
            if mean_d < best_dist:
                best_dist = mean_d
                best_time_shift = sum(ts_list) / len(ts_list)

    return best_dist, best_time_shift


# ────────────────────────────────────────────────────────────────────
# Anchor-mode helpers (audio-guided trim + fast frame streaming)
# ────────────────────────────────────────────────────────────────────

def estimate_trim_bounds(
    path: str,
    *,
    head_scan_s: float = 90.0,     # scan first N seconds for silence
    tail_scan_s: float = 1800.0,   # scan last N seconds for silence (30 min)
    noise_thresh_db: float = -55.0,
    min_silence_s: float = 1.5,
    merge_min_duration_s: float = 1.5,
    merge_max_gap_s: float = 0.8,
    guard_head_max_s: float = 60.0,
    guard_tail_max_s: float = 1800.0,
) -> tuple[float, float]:
    """
    Use audio to find head/tail silence and return (trim_head_s, trim_tail_s).
    If audio analysis fails (e.g. no audio stream), fall back to (0.0, 0.0).
    Times are rounded to 1 dp.
    """
    # Try audio-guided trim first
    try:
        res = detect_head_tail_silence_ffmpeg(
            path,
            noise_thresh_db=noise_thresh_db,
            min_silence_s=min_silence_s,
            head_scan_s=head_scan_s,
            tail_scan_s=tail_scan_s,
            downmix_mono=True,
        )
        duration = float(res.get("duration_s", 0.0))
        head_regions = merge_close_regions(
            res.get("head_silences", []),
            min_duration_s=merge_min_duration_s,
            max_gap_s=merge_max_gap_s,
        )
        tail_regions = merge_close_regions(
            res.get("tail_silences", []),
            min_duration_s=merge_min_duration_s,
            max_gap_s=merge_max_gap_s,
        )
    except Exception as e:
        # No audio stream or silencedetect failure — fall back gracefully
        # Get duration via OpenCV so tail logic can still clamp properly.
        try:
            cap = cv2.VideoCapture(path)
            fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
            cnt = cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0.0
            duration = float(cnt / max(fps, 1e-6))
            cap.release()
        except Exception:
            duration = 0.0
        head_regions, tail_regions = [], []
        try:
            fname = Path(path).name
        except Exception:
            fname = str(path)
        print(f"[audio] silencedetect unavailable for {fname} — assuming no trim. Reason: {e}", flush=True)

    # Compute trims from (possibly empty) regions
    trim_head = 0.0
    if head_regions:
        s0, e0 = head_regions[0]
        if s0 <= 0.3:  # only if silence starts ~at 0 s
            trim_head = min(e0, guard_head_max_s)

    trim_tail = 0.0
    if tail_regions and duration > 0.0:
        s_last, e_last = tail_regions[-1]
        if (duration - e_last) <= 1.0:  # ends ~at EOF
            trim_tail = min(duration - s_last, guard_tail_max_s)

    trim_head = round(max(0.0, min(trim_head, guard_head_max_s)), 1)
    trim_tail = round(max(0.0, min(trim_tail, guard_tail_max_s)), 1)
    return trim_head, trim_tail


def _stream_gray_frames_ffmpeg(
    path: str,
    start_s: float,
    duration_s: float,
    *,
    fps: float = 1.0,
    w: int = 32,
    h: int = 32,
    ffmpeg_path: str = "ffmpeg",
):
    """
    Stream grayscale frames at a fixed fps over [start_s, start_s + duration_s].
    FFmpeg does the decode + scale to 32×32 gray. We silence logs and send stderr to DEVNULL
    to prevent pipe blocking on noisy sources.
    """
    start_s = max(0.0, float(start_s))
    duration_s = max(0.0, float(duration_s))
    if duration_s <= 0.0:
        return

    vf = f"fps={fps},scale={w}:{h}:flags=bicubic,format=gray"

    cmd = [
        ffmpeg_path,
        "-hide_banner", "-nostdin",
        "-loglevel", "quiet", "-nostats",  # hard-silence logs
        "-ss", f"{start_s:.3f}",
        "-t",  f"{duration_s:.3f}",
        "-i",  path,
        "-vf", vf,
        "-an", "-sn",
        "-f", "rawvideo", "-vcodec", "rawvideo",
        "-threads", str(FFMPEG_THREADS),
        "pipe:1",
    ]

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,   # <- avoid deadlock on stderr
        stdin=subprocess.DEVNULL,    # <- be explicit
        bufsize=0,
    )
    frame_size = w * h
    idx = 0
    try:
        while True:
            buf = proc.stdout.read(frame_size)
            if not buf or len(buf) < frame_size:
                break
            frame = np.frombuffer(buf, dtype=np.uint8).reshape(h, w)
            t = start_s + idx / fps
            yield (round(t, 1), frame)
            idx += 1
    finally:
        try:
            if proc.stdout:
                proc.stdout.close()
        except Exception:
            pass
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()


def sample_anchor_hashes(
    path: str,
    *,
    window_s: float = ANCHOR_WINDOW_S,
    step_s: float = ANCHOR_STEP_S,   # 1.0 → 1 Hz. For speed use 2.0; for accuracy 0.5.
    trim: tuple[float, float] | None = None,
) -> dict[str, list[tuple[float, bytes]]]:
    """
    Sample 64-bit DCT pHash at fixed rate in the first/last window_s seconds, after trimming.
    Frames are streamed via FFmpeg (sequential reads) – much faster than random seeks.

    Returns: {"start": [(t_s, 8-byte_hash), ...], "end": [...]}
    """
    head_trim, tail_trim = (0.0, 0.0) if trim is None else trim
    fps = 1.0 / max(step_s, 1e-6)

    # Need total duration to position the tail window.
    cap = cv2.VideoCapture(path)
    if not cap.isOpened():
        raise RuntimeError(f"Could not open video: {path}")
    vfps = cap.get(cv2.CAP_PROP_FPS) or 25.0
    dur = float(cap.get(cv2.CAP_PROP_FRAME_COUNT) / max(vfps, 1e-6))
    cap.release()

    start_t0 = head_trim
    start_dur = max(0.0, window_s)

    end_t1 = dur - tail_trim
    end_t0 = max(head_trim, end_t1 - window_s)
    end_dur = max(0.0, end_t1 - end_t0)

    def _phash64_from_gray(gray: np.ndarray) -> bytes:
        # Input is already 32×32 gray from FFmpeg
        g32 = gray.astype(np.float32)
        dct = cv2.dct(g32)
        low = dct[:8, :8]
        med = float(np.median(low))
        bits = (low > med).astype(np.uint8).flatten()
        out = bytearray()
        byte = 0
        for i, b in enumerate(bits):
            byte = (byte << 1) | int(b)
            if (i % 8) == 7:
                out.append(byte & 0xFF)
                byte = 0
        return bytes(out)

    start_samples: list[tuple[float, bytes]] = []
    for t, gray in _stream_gray_frames_ffmpeg(path, start_t0, start_dur, fps=fps):
        start_samples.append((t, _phash64_from_gray(gray)))

    end_samples: list[tuple[float, bytes]] = []
    for t, gray in _stream_gray_frames_ffmpeg(path, end_t0, end_dur, fps=fps):
        end_samples.append((t, _phash64_from_gray(gray)))

    return {"start": start_samples, "end": end_samples}


def _hamming64(a: bytes, b: bytes) -> int:
    """Hamming distance between two 8-byte pHashes (64 bits)."""
    if len(a) != 8 or len(b) != 8:
        raise ValueError("Expected 8-byte hashes")
    return (int.from_bytes(a, "big") ^ int.from_bytes(b, "big")).bit_count()


def match_anchor_sets(
    a: list[tuple[float, bytes]],
    b: list[tuple[float, bytes]],
    *,
    hamming_thresh: int = ANCHOR_HAMMING_THRESH,
) -> tuple[float, float, float]:
    """
    For each element of the smaller set, find nearest neighbour in the larger set by Hamming.
    Keep pairs with Hamming ≤ hamming_thresh and collect time offsets.
    Returns: (match_fraction_wrt_smaller, median_offset_s, MAD_offset_s)
    """
    if not a or not b:
        return 0.0, 0.0, float("inf")

    queries, targets = (a, b) if len(a) <= len(b) else (b, a)
    t_hashes = [h for _, h in targets]

    offsets: list[float] = []
    num_good = 0
    for t_s, qh in queries:
        best = 65
        best_idx = -1
        for idx, th in enumerate(t_hashes):
            d = _hamming64(qh, th)
            if d < best:
                best = d
                best_idx = idx
                if best == 0:
                    break
        if best_idx >= 0 and best <= hamming_thresh:
            num_good += 1
            ref_s = targets[best_idx][0]
            offsets.append(t_s - ref_s)

    if num_good == 0:
        return 0.0, 0.0, float("inf")

    offs = np.array(offsets, dtype=np.float64)
    med = float(np.median(offs))
    mad = float(np.median(np.abs(offs - med)))
    return num_good / float(len(queries)), med, mad


def compare_anchors(
    anchorsA: dict[str, list[tuple[float, bytes]]],
    anchorsB: dict[str, list[tuple[float, bytes]]],
    *,
    hamming_thresh: int = ANCHOR_HAMMING_THRESH,
) -> dict:
    """Compare start↔start and end↔end anchor sets and return fractions + robust offsets."""
    f1, o1, m1 = match_anchor_sets(anchorsA.get("start", []), anchorsB.get("start", []), hamming_thresh=hamming_thresh)
    f2, o2, m2 = match_anchor_sets(anchorsA.get("end",   []), anchorsB.get("end",   []), hamming_thresh=hamming_thresh)
    return {
        "start_fraction": f1, "start_offset_s": o1, "start_mad_s": m1,
        "end_fraction":   f2, "end_offset_s":   o2, "end_mad_s":   m2,
    }


def _median_offset_ordered(
    a_list: list[tuple[float, bytes]],
    b_list: list[tuple[float, bytes]],
    *,
    hamming_thresh: int = ANCHOR_HAMMING_THRESH,
) -> tuple[float, float, float]:
    """
    Compute offsets in a fixed order: for each (t_a, h_a) in A, find nearest in B.
    Returns (median_offset_seconds = t_a - t_b, MAD_seconds, match_fraction_wrt_A).
    """
    if not a_list or not b_list:
        return 0.0, float("inf"), 0.0
    offsets: list[float] = []
    good = 0
    b_hashes = [h for _, h in b_list]
    for t_a, h_a in a_list:
        best = 65
        best_idx = -1
        for idx, h_b in enumerate(b_hashes):
            d = _hamming64(h_a, h_b)
            if d < best:
                best = d
                best_idx = idx
                if best == 0:
                    break
        if best_idx >= 0 and best <= hamming_thresh:
            good += 1
            t_b = b_list[best_idx][0]
            offsets.append(t_a - t_b)
    if good == 0:
        return 0.0, float("inf"), 0.0
    offs = np.array(offsets, dtype=np.float64)
    med = float(np.median(offs))
    mad = float(np.median(np.abs(offs - med)))
    frac = good / float(len(a_list))
    return med, mad, frac


def decide_subset_match(
    stats: dict,
    *,
    min_fraction: float = ANCHOR_MIN_FRACTION,
    max_mad_s: float = ANCHOR_MAX_MAD_S,
) -> tuple[bool, str]:
    """
    Decide the relation label based on match strength at start and end.
    Labels: {"full↔full", "first_part↔full", "second_part↔full", "ambiguous"}.
    """
    sf, sm = stats["start_fraction"], stats["start_mad_s"]
    ef, em = stats["end_fraction"],   stats["end_mad_s"]

    start_strong = (sf >= min_fraction) and (sm <= max_mad_s)
    end_strong   = (ef >= min_fraction) and (em <= max_mad_s)

    if start_strong and end_strong:
        return True, "full↔full"
    if start_strong and not end_strong:
        return True, "first_part↔full"
    if end_strong and not start_strong:
        return True, "second_part↔full"
    return False, "ambiguous"


def refine_prefix_suffix(
    pathA: str,
    pathB: str,
    *,
    window_s: float = ANCHOR_WINDOW_S,
    step_s: float = ANCHOR_STEP_S,
    hamming_thresh: int = ANCHOR_HAMMING_THRESH,
    min_fraction: float = ANCHOR_MIN_FRACTION,
    max_mad_s: float = ANCHOR_MAX_MAD_S,
    precomputed: tuple[dict, dict] | None = None,
) -> dict:
    """
    Run the anchor refine for A and B. If precomputed=(ancA, ancB) is provided, it uses that.
    Returns a dict with: ok, relation, start_* / end_* stats, subset_start_in_full_s.
    """
    if precomputed is not None:
        ancA, ancB = precomputed
    else:
        trA = estimate_trim_bounds(pathA)
        trB = estimate_trim_bounds(pathB)
        ancA = sample_anchor_hashes(pathA, window_s=window_s, step_s=step_s, trim=trA)
        ancB = sample_anchor_hashes(pathB, window_s=window_s, step_s=step_s, trim=trB)

    stats = compare_anchors(ancA, ancB, hamming_thresh=hamming_thresh)
    ok, relation = decide_subset_match(stats, min_fraction=min_fraction, max_mad_s=max_mad_s)

    # Directional roles by duration (±5% tolerance = ambiguous)
    subset_start_in_full_s = None
    relation_dir = relation
    full_clip = None
    subset_clip = None

    if ok and relation in ("first_part↔full", "second_part↔full"):
        # decide roles by duration
        # (use external durations in caller to set full/subset columns consistently)
        pass  # roles are assigned in the main loop where durations are known

    return {
        "ok": ok,
        "relation": relation,
        "start_fraction": stats["start_fraction"],
        "start_offset_s": stats["start_offset_s"],
        "start_mad_s": stats["start_mad_s"],
        "end_fraction": stats["end_fraction"],
        "end_offset_s": stats["end_offset_s"],
        "end_mad_s": stats["end_mad_s"],
        "subset_start_in_full_s": subset_start_in_full_s,  # caller fills when roles resolved
        "relation_dir": relation_dir,                      # caller fills when roles resolved
        "full_clip": full_clip,                            # caller fills
        "subset_clip": subset_clip,                        # caller fills
    }


# ────────────────────────────────────────────────────────────────────
# JSON encode/decode helpers for anchors (bytes ↔ hex strings)
# ────────────────────────────────────────────────────────────────────

def _hash_bytes_to_hex_list(seq: list[tuple[float, bytes]]) -> list[tuple[float, str]]:
    """JSON can’t store bytes; store each 8-byte pHash as hex. Timestamps already rounded to 0.1 s."""
    return [(float(t), h.hex()) for t, h in seq]


def _hash_hex_to_bytes_list(seq: list[tuple[float, str]]) -> list[tuple[float, bytes]]:
    """Decode JSON-stored [(t, 'hex'), …] to [(t, bytes), …] for runtime matching."""
    return [(float(t), bytes.fromhex(hx)) for t, hx in seq]


# ────────────────────────────────────────────────────────────────────
# Persistent cache: legacy hashes + NEW anchors in one JSON
# ────────────────────────────────────────────────────────────────────

class VideoHashStore:
    """
    Caches per-file:
      • legacy 'avg' (64-bit hex) and 'seq' (sparse [(hex, t), ...])
      • anchors: {'trim':(head, tail), 'data':{'start':[(t,hex)], 'end':[...]} , 'params', 'mtime'}
    Key: absolute file path.
    """
    _inst = None

    def __new__(cls, path=VIDEO_HASH_STORE_PATH):
        if cls._inst is None:
            cls._inst = super().__new__(cls)
            cls._inst._init(path)
        return cls._inst

    def _init(self, path: str):
        self.path = path
        self._data: Dict[str, Dict] = {}
        self._dirty = False
        self._new_count = 0

        # Load new cache, or migrate old root-level cache if present.
        if os.path.exists(path):
            with open(path) as f:
                self._data = json.load(f)
        elif os.path.exists(MIGRATE_OLD_CACHE_PATH):
            with open(MIGRATE_OLD_CACHE_PATH) as f:
                self._data = json.load(f)
            self._dirty = True  # save into new location on exit

        atexit.register(self.save_if_dirty)

    # —— Legacy getters (avg + sparse seq) ——
    def get(self, filepath: str) -> Tuple[str, List[Tuple[str, float]]]:
        """Fetch or compute legacy data for one file. Returns (avg_hex, sparse_seq[(hex, t), …])."""
        global PROCESSED_VIDEOS, TOTAL_VIDEOS
        mtime = os.path.getmtime(filepath)
        entry = self._data.get(filepath)

        if not entry or entry.get("mtime") != mtime or "avg" not in entry or "seq" not in entry:
            seq_pairs = _sample_hashes_with_times(filepath)
            avg_hex   = _average_hex(seq_pairs)
            self._data[filepath] = {"mtime": mtime, "avg": avg_hex, "seq": seq_pairs}
            self._dirty = True
            self._new_count += 1
            print(f"[HashStore] NEW {Path(filepath).name} ({len(seq_pairs)} samples)", flush=True)
            if self._new_count >= SAVE_EVERY:
                self.save_if_dirty()
                self._new_count = 0
                pct = (PROCESSED_VIDEOS / TOTAL_VIDEOS * 100) if TOTAL_VIDEOS else 0
                print(f"[find_video_duplicates] Saved {PROCESSED_VIDEOS}/{TOTAL_VIDEOS} videos to JSON ({pct:.1f}%)")
            PROCESSED_VIDEOS += 1
            return avg_hex, seq_pairs

        PROCESSED_VIDEOS += 1
        print(f"[HashStore] CACHE {Path(filepath).name}", flush=True)
        return entry["avg"], entry["seq"]

    # —— Anchors ——
    def get_anchors(
        self,
        path: str,
        *,
        window_s: float = ANCHOR_WINDOW_S,
        step_s: float = ANCHOR_STEP_S,
        force: bool = False,
    ) -> tuple[dict, tuple[float, float]]:
        """
        Fetch (or compute & cache) start/end anchor hashes for a file.
        Returns: (anchors_dict, trim_tuple)
        """
        mtime = os.path.getmtime(path)
        entry = self._data.get(path)
        if entry is None:
            entry = {}
            self._data[path] = entry

        params = {"version": 1, "window_s": float(window_s), "step_s": float(step_s)}
        anc = entry.get("anchors")
        valid = False
        if anc and not force:
            try:
                valid = (
                    isinstance(anc, dict)
                    and anc.get("params") == params
                    and float(anc.get("mtime", 0.0)) == float(mtime)
                    and "data" in anc and "trim" in anc
                )
            except Exception:
                valid = False

        if not valid:
            trim = estimate_trim_bounds(path)
            data = sample_anchor_hashes(path, window_s=window_s, step_s=step_s, trim=trim)
            anc = {
                "mtime": mtime,
                "params": params,
                "trim": (round(float(trim[0]), 1), round(float(trim[1]), 1)),
                "data": {
                    "start": _hash_bytes_to_hex_list(data.get("start", [])),
                    "end":   _hash_bytes_to_hex_list(data.get("end",   [])),
                },
            }
            entry["anchors"] = anc
            self._data[path] = entry
            self._dirty = True

        runtime_data = {
            "start": _hash_hex_to_bytes_list(anc["data"].get("start", [])),
            "end":   _hash_hex_to_bytes_list(anc["data"].get("end",   [])),
        }
        trim_tuple = (float(anc["trim"][0]), float(anc["trim"][1]))
        return runtime_data, trim_tuple

    def save_if_dirty(self):
        """Persist cache to disk if changed."""
        if self._dirty:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            with open(self.path, "w") as f:
                json.dump(self._data, f, indent=2)
            self._dirty = False
            print(f"[HashStore] cache saved → {self.path} ({len(self._data)} items)", flush=True)


# ────────────────────────────────────────────────────────────────────
# Worker for metadata + optional legacy hashes (parallelised)
# ────────────────────────────────────────────────────────────────────

def _process_video(path_fid, *, need_legacy: bool):
    """
    Threadpool worker:
      • Get fps & frame count (for duration).
      • If need_legacy=True: compute legacy avg+seq via the cache (slow).
      • Else: skip legacy hashes entirely (anchors mode is faster).
      • Return metadata tuple.
    """
    path, fid = path_fid
    try:
        cap = cv2.VideoCapture(path)
        if not cap.isOpened():
            return None
        fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
        cnt = cap.get(cv2.CAP_PROP_FRAME_COUNT) or 1
        cap.release()

        file_size = os.path.getsize(path)
        duration = cnt / fps

        if need_legacy:
            avg_hex, seq = VideoHashStore().get(path)
        else:
            avg_hex, seq = None, None

        return (path, fid, fps, cnt, avg_hex, seq, file_size, duration)
    except Exception as e:
        print(f"[find_video_duplicates] ERROR processing {path}: {e}", flush=True)
        return None


# ────────────────────────────────────────────────────────────────────
# Main dedupe pipeline
# ────────────────────────────────────────────────────────────────────

def find_video_duplicates(
    directories: List[str],
    faiss_threshold: float      = FAISS_THRESHOLD,
    align_threshold: float      = ALIGN_THRESHOLD,
    align_offset_limit_s: float = ALIGN_OFFSET_LIMIT_S,
    top_k: int                  = TOP_K,
    self_compare: bool          = False,
    use_gpu: bool               = True,
    report_path: str            = DEFAULT_REPORT_PATH,
    refine_mode: str            = "anchors",   # "anchors" | "legacy" | "both"
) -> pd.DataFrame:
    """
    Run dedupe over one or more directories.

    Pipeline:
      1) Walk files (recursive), filter by extensions.
      2) Parallel: gather metadata + legacy hashes only if mode needs them.
      3) If anchors are needed: precompute anchors for all files (parallel, cached).
      4) Build candidate pairs:
           - anchors: direct all-pairs (with duration sanity gate).
           - legacy/both: FAISS shortlist on legacy avg hash.
      5) Evaluate pairs with chosen refine(s).
      6) Export a mode-specific report and open it.
    """
    t0 = time.time()
    print(f"[find_video_duplicates] Start {time.strftime('%H:%M:%S')} (mode={refine_mode})", flush=True)
    print(f"[mode] legacy={'yes' if refine_mode in ('legacy','both') else 'no'}, "
          f"anchors={'yes' if refine_mode in ('anchors','both') else 'no'}, "
          f"faiss={'no' if refine_mode == 'anchors' else 'yes'}")

    from funcs import get_list_of_files  # local import avoids circularity

    # ── 1) Gather files (recursive) — build a single grand total
    all_tasks = []
    grand_total = 0
    for fid, folder in enumerate(directories):
        all_files = get_list_of_files(folder)
        print(f"[find_video_duplicates] Folder {fid}: {folder} → {len(all_files)} files", flush=True)
        video_paths = [f for f in all_files
                       if Path(f).suffix.lower() in EXTS
                       and '_gsdata_' not in f
                       and not Path(f).name.startswith("._")]
        print(f"[find_video_duplicates]   ↳ {len(video_paths)} video files", flush=True)
        all_tasks += [(p, fid) for p in video_paths]
        grand_total += len(video_paths)

    global TOTAL_VIDEOS
    TOTAL_VIDEOS = grand_total
    if grand_total == 0:
        print("⚠️  No readable videos found.", flush=True)
        return pd.DataFrame()
    print(f"[totals] {grand_total} videos across {len(directories)} folder(s)", flush=True)

    # If the user passed exactly two folders, put folder 0 on the A side.
    prefer_folder = 0 if len(directories) == 2 else None

    # ── 2) Metadata (+ legacy hashes only if needed)
    need_legacy = (refine_mode in ("legacy", "both"))
    results = []
    total_jobs = len(all_tasks)
    tick = max(1, total_jobs // 10)  # ~10 updates
    print(f"[stage] metadata scan — need_legacy={need_legacy} — {total_jobs} file(s)")
    meta_t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
        futures = {exec.submit(_process_video, t, need_legacy=need_legacy): t for t in all_tasks}
        for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            res = fut.result()
            if res:
                results.append(res)
            if (i % tick == 0) or (i == total_jobs):
                pct = int(round(100 * i / total_jobs))
                elapsed = time.time() - meta_t0
                print(f"[progress] metadata {i}/{total_jobs} ({pct}%) — {elapsed:.1f}s elapsed", flush=True)

    # Unpack basics
    paths, folder_ids, durations, sizes = [], [], [], []
    for path, fid, fps, cnt, avg_hex, seq, file_size, duration in results:
        paths.append(path)
        folder_ids.append(fid)
        durations.append(duration)
        sizes.append(file_size)

    # ── 3) Anchors precompute (parallel) if needed
    anchors_cache: dict[int, dict] = {}
    if refine_mode in ("anchors", "both"):
        vhs = VideoHashStore()
        total_jobs = len(paths)
        print(f"[stage] anchors precompute — {total_jobs} file(s)")
        anc_t0 = time.time()

        def _build(idx_path):
            idx, p = idx_path
            anc, _ = vhs.get_anchors(
                p,
                window_s=ANCHOR_WINDOW_S,
                step_s=ANCHOR_STEP_S,
            )
            return idx, anc

        with concurrent.futures.ThreadPoolExecutor(max_workers=ANCHOR_WORKERS) as exec:
            futures = {exec.submit(_build, (idx, p)): idx for idx, p in enumerate(paths)}
            done_count = 0
            last_print = 0.0
            pending = set(futures.keys())

            while pending:
                # wait up to 10s to either get completions or emit a heartbeat
                done, pending = concurrent.futures.wait(
                    pending,
                    timeout=10.0,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )

                # collect finished tasks
                for fut in done:
                    idx, anc = fut.result()
                    anchors_cache[idx] = anc
                    done_count += 1

                # heartbeat every 10s (or on completion)
                now = time.time()
                if (now - last_print) >= 10.0 or not pending:
                    pct = int(round(100 * done_count / total_jobs)) if total_jobs else 100
                    elapsed = now - anc_t0
                    avg = (elapsed / done_count) if done_count else 0.0
                    eta = max(0.0, (total_jobs - done_count) * avg) if done_count else 0.0
                    print(f"[progress] anchors {done_count}/{total_jobs} ({pct}%) — "
                          f"{elapsed:.1f}s elapsed — ~{avg:.2f}s/file — ETA {eta:.1f}s",
                          flush=True)
                    last_print = now

    # ── 4) Candidate pairs
    if refine_mode == "anchors":
        # Direct all-pairs (unique), with an ultra-cheap duration sanity gate.
        raw_pairs = set()
        N = len(paths)
        for i in range(N):
            for j in range(i + 1, N):
                if paths[i] == paths[j]:
                    continue  # skip self-pairs even if the same file appears twice
                if not self_compare and folder_ids[i] == folder_ids[j]:
                    continue
                if max(durations[i], durations[j]) <= 0:
                    continue
                ratio = min(durations[i], durations[j]) / max(durations[i], durations[j])
                if ratio < DURATION_RATIO_MIN:
                    continue
                a, b = _order_pair(i, j, folder_ids, paths, prefer_folder)
                raw_pairs.add((a, b))
        print(f"[find_video_duplicates] (anchors) {len(raw_pairs)} candidate pairs (no FAISS)", flush=True)
        faiss_dist = {}  # not used in anchors mode

    else:
        # legacy / both: FAISS shortlist on legacy avg hash
        print(f"[stage] FAISS shortlist (legacy avg hash) — {len(paths)} vector(s)")
        avg_vecs = []
        seqs = []  # legacy sparse sequences for aligner
        for (_, _, _, _, avg_hex, seq, _, _) in results:
            avg_vecs.append(_hex_to_vec(avg_hex))
            seqs.append(seq)

        mat = np.stack(avg_vecs).astype("float32")
        index = faiss.IndexFlatL2(mat.shape[1])
        if use_gpu:
            res = faiss.StandardGpuResources()
            index = faiss.index_cpu_to_gpu(res, 0, index)
        index.add(mat)
        D, I = index.search(mat, top_k + 1)
        print(f"[find_video_duplicates] FAISS search done", flush=True)

        raw_pairs = set()
        for i, (drow, idxrow) in enumerate(zip(D, I)):
            for dist, j in zip(drow, idxrow):
                if paths[i] == paths[j]:
                    continue  # skip self-pairs even if the same file appears twice
                if i == j or dist > faiss_threshold:
                    continue
                if not self_compare and folder_ids[i] == folder_ids[j]:
                    continue
                a, b = _order_pair(i, j, folder_ids, paths, prefer_folder)
                raw_pairs.add((a, b))
        print(f"[find_video_duplicates] {len(raw_pairs)} candidate pairs", flush=True)

        # Distance lookup for report
        faiss_dist = {(i, j): float(D[i][k])
                      for i, (drow, idxrow) in enumerate(zip(D, I))
                      for k, j in enumerate(idxrow) if i != j}

    # ── 5) Evaluate candidates
    results_rows = []
    eval_total = len(raw_pairs)
    eval_tick = max(1, eval_total // 10)
    print(f"[stage] evaluating candidates — {eval_total} pair(s)")
    eval_t0 = time.time()
    seen_pairs: set[tuple[str, str]] = set()

    for n, (i, j) in enumerate(raw_pairs, 1):
        row = {
            "file_a": paths[i],
            "file_b": paths[j],
            "size_a (MB)": round(sizes[i] / (1024 * 1024), 2),
            "size_b (MB)": round(sizes[j] / (1024 * 1024), 2),
            "duration_a (s)": round(durations[i], 1),
            "duration_b (s)": round(durations[j], 1),
        }
        # Drop exact duplicates across modes/passes (path-level)
        pair_key = (row["file_a"], row["file_b"])
        if pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)

        if refine_mode != "anchors":
            # Only legacy/both have meaningful FAISS distances to display
            row["avg_frame_diff (0–64)"] = faiss_dist.get((i, j), faiss_dist.get((j, i)))

        legacy_ok = False
        anchors_ok = False

        # Legacy refine (only if mode needs it)
        if refine_mode in ("legacy", "both"):
            seq_i = results[i][5]
            seq_j = results[j][5]
            sec_per = (durations[i] / len(seq_i)) if seq_i else float('inf')
            max_shift = int(align_offset_limit_s / sec_per) if math.isfinite(sec_per) and sec_per > 0 else 0
            best_h, best_ts = _aligned_distance_and_time_limited(
                seq_i, seq_j, max_shift, align_offset_limit_s
            )
            legacy_ok = (best_h <= align_threshold)
            row.update({
                "legacy_best_aligned_diff (0–64)": best_h,
                "legacy_time_shift_s": round(best_ts, 1),
            })

        # Anchors refine (only if mode needs it)
        if refine_mode in ("anchors", "both"):
            ancA = anchors_cache[i]
            ancB = anchors_cache[j]
            stats = compare_anchors(ancA, ancB, hamming_thresh=ANCHOR_HAMMING_THRESH)
            ok, relation = decide_subset_match(
                stats,
                min_fraction=ANCHOR_MIN_FRACTION,
                max_mad_s=ANCHOR_MAX_MAD_S
            )
            anchors_ok = ok

            # Directional roles by duration (±5% tolerance = ambiguous)
            relation_dir = relation
            full_clip = None
            subset_clip = None
            subset_start_in_full_s = None

            if ok and relation in ("first_part↔full", "second_part↔full"):
                durA, durB = durations[i], durations[j]
                if durA > 1.05 * durB:
                    full_clip, subset_clip = "A", "B"
                elif durB > 1.05 * durA:
                    full_clip, subset_clip = "B", "A"

                if full_clip and subset_clip:
                    if relation == "first_part↔full":
                        # START↔START offsets, ordered subset→full
                        if subset_clip == "A":
                            med, _mad, _ = _median_offset_ordered(ancA["start"], ancB["start"],
                                                                  hamming_thresh=ANCHOR_HAMMING_THRESH)
                        else:
                            med, _mad, _ = _median_offset_ordered(ancB["start"], ancA["start"],
                                                                  hamming_thresh=ANCHOR_HAMMING_THRESH)
                        subset_start_in_full_s = round(-med, 1)
                        relation_dir = f"{full_clip}=full, {subset_clip}=first_part"

                    elif relation == "second_part↔full":
                        # END↔END offsets, ordered subset→full
                        if subset_clip == "A":
                            med, _mad, _ = _median_offset_ordered(ancA["end"], ancB["end"],
                                                                  hamming_thresh=ANCHOR_HAMMING_THRESH)
                        else:
                            med, _mad, _ = _median_offset_ordered(ancB["end"], ancA["end"],
                                                                  hamming_thresh=ANCHOR_HAMMING_THRESH)
                        subset_start_in_full_s = round(-med, 1)
                        relation_dir = f"{full_clip}=full, {subset_clip}=second_part"

            row.update({
                "relation": relation,
                "relation_dir": relation_dir,
                "full_clip": full_clip,
                "subset_clip": subset_clip,
                "subset_start_in_full_s": subset_start_in_full_s,
                "subset_start_mm:ss": _format_mmss(subset_start_in_full_s),
                # symmetric quality stats for transparency
                "start_match_fraction": round(stats["start_fraction"], 3),
                "start_offset_s": round(stats["start_offset_s"], 1),
                "start_offset_mad_s": (round(stats["start_mad_s"], 1)
                                       if math.isfinite(stats["start_mad_s"]) else None),
                "end_match_fraction": round(stats["end_fraction"], 3),
                "end_offset_s": round(stats["end_offset_s"], 1),
                "end_offset_mad_s": (round(stats["end_mad_s"], 1)
                                     if math.isfinite(stats["end_mad_s"]) else None),
            })

        keep = ((refine_mode == "legacy"  and legacy_ok) or
                (refine_mode == "anchors" and anchors_ok) or
                (refine_mode == "both"    and (legacy_ok or anchors_ok)))
        if keep:
            results_rows.append(row)

        if (n % eval_tick == 0) or (n == eval_total):
            pct = int(round(100 * n / eval_total)) if eval_total else 100
            elapsed = time.time() - eval_t0
            print(f"[progress] evaluate {n}/{eval_total} ({pct}%) — {elapsed:.1f}s elapsed", flush=True)

    # ── 6) Export
    if refine_mode == "legacy":
        columns = [
            "file_a", "file_b",
            "duration_a (s)", "duration_b (s)",
            "size_a (MB)", "size_b (MB)",
            "avg_frame_diff (0–64)",
            "legacy_best_aligned_diff (0–64)",
            "legacy_time_shift_s",
        ]
    elif refine_mode == "anchors":
        columns = [
            "file_a", "file_b",
            "duration_a (s)", "duration_b (s)",
            "size_a (MB)", "size_b (MB)",
            "relation", "relation_dir", "full_clip", "subset_clip",
            "subset_start_in_full_s", "subset_start_mm:ss",
            "start_match_fraction", "start_offset_s", "start_offset_mad_s",
            "end_match_fraction",   "end_offset_s",   "end_offset_mad_s",
        ]
    else:  # both
        columns = [
            "file_a", "file_b",
            "duration_a (s)", "duration_b (s)",
            "size_a (MB)", "size_b (MB)",
            "avg_frame_diff (0–64)",
            # legacy
            "legacy_best_aligned_diff (0–64)",
            "legacy_time_shift_s",
            # anchors
            "relation", "relation_dir", "full_clip", "subset_clip",
            "subset_start_in_full_s", "subset_start_mm:ss",
            "start_match_fraction", "start_offset_s", "start_offset_mad_s",
            "end_match_fraction",   "end_offset_s",   "end_offset_mad_s",
        ]

    filtered = [{k: r.get(k, None) for k in columns} for r in results_rows]
    df = pd.DataFrame(filtered, columns=columns)

    export_excel(df, report_path)
    print(f"[find_video_duplicates] Done in {time.time()-t0:.1f}s — {len(results_rows)} pairs saved", flush=True)
    open_excel_file(report_path)
    return df


# ────────────────────────────────────────────────────────────────────
# Excel export helpers
# ────────────────────────────────────────────────────────────────────

def export_excel(df: pd.DataFrame, path: str):
    """
    Write a single-sheet Excel file with bold headers, frozen top row,
    sane column widths, and simple number formatting.
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    while True:
        try:
            with ExcelWriter(path, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Duplicates")
                ws = writer.sheets["Duplicates"]

                # Header styling
                for cell in ws[1]:
                    cell.font = Font(bold=True)
                ws.freeze_panes = "A2"

                # Column widths + number formats
                for idx, col in enumerate(df.columns, start=1):
                    max_len = df[col].astype(str).map(len).max() if not df.empty else 0
                    width = max(len(col), max_len) + 2
                    ws.column_dimensions[get_column_letter(idx)].width = width

                    if "file_a" in col.lower() or "file_b" in col.lower():
                        fmt = numbers.FORMAT_GENERAL
                    elif "%" in col:
                        fmt = numbers.FORMAT_PERCENTAGE_00
                    else:
                        fmt = numbers.FORMAT_NUMBER_00
                    for cell in ws[get_column_letter(idx)][1:]:
                        cell.number_format = fmt
            break
        except PermissionError:
            choice = sg.popup_yes_no(
                f"Can't write to file:\n{path}\n\nIt might be open in Excel.\n\nRetry?",
                title="Export Failed",
                keep_on_top=True
            )
            if choice != 'Yes':
                print(f"Export to {path} aborted by user.")
                break
        except Exception as e:
            sg.popup_error(f"Unexpected error while exporting:\n{e}", title="Export Failed", keep_on_top=True)
            break


def open_excel_file(path):
    """Open the Excel file with the OS default application (best-effort)."""
    try:
        if platform.system() == "Windows":
            os.startfile(path)  # type: ignore
        elif platform.system() == "Darwin":
            subprocess.call(["open", path])
        elif platform.system() == "Linux":
            subprocess.call(["xdg-open", path])
    except Exception as e:
        print(f"Could not open Excel file: {e}")


# ────────────────────────────────────────────────────────────────────
# PyCharm-friendly harness (edit paths + mode, then Run)
# ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print()
