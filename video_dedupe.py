# video_dedupe.py

import os
import json
import atexit
import time
from pathlib import Path
from typing import List, Tuple, Dict

import numpy as np
import pandas as pd
import cv2
from PIL import Image
import imagehash
import faiss
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, numbers
from pandas import ExcelWriter

# ────────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────────
VIDEO_HASH_STORE_PATH     = "video_hashes.json"
VIDEO_DEDUPE_REPORT_PATH  = "video_duplicates.xlsx"

MAX_SAMPLES              = 20      # key‑frame sample count per video
FAISS_THRESHOLD          = 12     # raw L2 on 64‑bit super‑hash (0–64)
ALIGN_THRESHOLD          = 10.0    # raw mean Hamming (0–64)
ALIGN_OFFSET_LIMIT_S     = 60.0    # max time offset for alignment (seconds)
TOP_K                    = 5
EXTS                     = {'.mp4', '.mov', '.avi', '.m4v', '.mpg', '.mkv'}


# ────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────
def _hex_to_vec(hex_str: str) -> np.ndarray:
    """Convert a 64‑bit hex into a 64‑dim 0/1 float32 vector."""
    ba   = bytes.fromhex(hex_str)
    bits = np.unpackbits(np.frombuffer(ba, dtype=np.uint8))
    return bits.astype("float32")


def _sample_hashes_with_times(path: str, k: int = MAX_SAMPLES) -> List[Tuple[str, float]]:
    """
    Sample up to k frame pHashes evenly, returning (hex, time_s).
    """
    cap = cv2.VideoCapture(path)
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video: {path}")

    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT)) or 1
    step = max(1, frame_count // k)
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


def _average_hex(pairs: List[Tuple[str, float]]) -> str:
    """Average the hex hashes (ignore times)."""
    hexes = [h for (h, _) in pairs]
    if not hexes:
        return "0" * 16
    bits = np.stack([_hex_to_vec(h) for h in hexes])
    avg_bits = (bits.mean(axis=0) >= 0.5).astype(np.uint8)
    packed = np.packbits(avg_bits)
    return packed.tobytes().hex()


def _hamming(a: str, b: str) -> int:
    """Hamming distance between two 64‑bit hex strings."""
    return bin(int(a, 16) ^ int(b, 16)).count("1")


def _aligned_distance_and_time_limited(
    seq_a: List[Tuple[str, float]],
    seq_b: List[Tuple[str, float]],
    max_shift_samples: int,
    offset_limit_s: float
) -> Tuple[float, float]:
    """
    Slide seq_a over seq_b within ±max_shift_samples (in sample indices),
    compute mean Hamming and corresponding mean time shift,
    but only consider pairs whose time shift ≤ offset_limit_s.
    Returns (best_mean_hamming, best_time_shift_s).
    """
    if not seq_a or not seq_b:
        return 64.0, 0.0

    L1, L2 = len(seq_a), len(seq_b)
    best_dist = 64.0
    best_time_shift = 0.0

    min_shift = max(-(L2 - 1), -max_shift_samples)
    max_shift = min(L1 - 1, max_shift_samples)

    for shift in range(min_shift, max_shift + 1):
        dists = []
        time_shifts = []
        for i, (ha, ta) in enumerate(seq_a):
            j = i - shift
            if 0 <= j < L2:
                hb, tb = seq_b[j]
                ts = tb - ta
                if abs(ts) <= offset_limit_s:
                    dists.append(_hamming(ha, hb))
                    time_shifts.append(ts)
        if dists:
            mean_dist = sum(dists) / len(dists)
            if mean_dist < best_dist:
                best_dist = mean_dist
                best_time_shift = sum(time_shifts) / len(time_shifts)
    return best_dist, best_time_shift


# ────────────────────────────────────────────────────────────────────
# Persistent cache
# ────────────────────────────────────────────────────────────────────
class VideoHashStore:
    """
    Caches per-file:
      • mtime
      • avg: hex of super‑hash
      • seq: list of (hex, time_s)
    Persists to VIDEO_HASH_STORE_PATH.
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
        if os.path.exists(path):
            with open(path) as f:
                self._data = json.load(f)
        atexit.register(self.save_if_dirty)

    def get(self, filepath: str) -> Tuple[str, List[Tuple[str, float]]]:
        """
        Return (avg_hex, seq_pairs), computing if missing or file changed.
        seq_pairs is List of (hash_hex, time_s).
        """
        mtime = os.path.getmtime(filepath)
        entry = self._data.get(filepath)
        if not entry or entry.get("mtime") != mtime:
            seq_pairs = _sample_hashes_with_times(filepath)
            avg_hex    = _average_hex(seq_pairs)
            self._data[filepath] = {"mtime": mtime, "avg": avg_hex, "seq": seq_pairs}
            self._dirty = True
            print(f"[HashStore] NEW {Path(filepath).name} ({len(seq_pairs)} samples)")
            return avg_hex, seq_pairs
        print(f"[HashStore] CACHE {Path(filepath).name}")
        return entry["avg"], entry["seq"]

    def save_if_dirty(self):
        if self._dirty:
            with open(self.path, "w") as f:
                json.dump(self._data, f, indent=2)
            self._dirty = False


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
    use_gpu: bool               = False,
    report_path: str            = VIDEO_DEDUPE_REPORT_PATH
) -> pd.DataFrame:
    """
    1) Build 64‑bit super‑hash vectors.
    2) FAISS coarse filter (L2 ≤ faiss_threshold).
    3) Refine via limited alignment:
       • best_aligned_diff (mean Hamming)
       • time_shift_s
       only within ±align_offset_limit_s.
    4) Export columns:
       file_a, file_b,
       avg_frame_diff (0–8),
       best_aligned_diff (0–64),
       time_shift_s,
       aligned_pct_diff (0–100%).
    """
    t0 = time.time()
    print(f"[find_video_duplicates] Start {time.strftime('%H:%M:%S')}")

    from funcs import get_list_of_files

    paths, folder_ids, durations = [], [], []
    store = VideoHashStore()

    # gather files + durations
    for fid, folder in enumerate(directories):
        for p in get_list_of_files(folder):
            q = Path(p)
            if q.suffix.lower() in EXTS and not q.name.startswith("._"):
                paths.append(str(q))
                folder_ids.append(fid)
                cap = cv2.VideoCapture(str(q))
                fps  = cap.get(cv2.CAP_PROP_FPS) or 30.0
                cnt  = cap.get(cv2.CAP_PROP_FRAME_COUNT) or MAX_SAMPLES
                durations.append(cnt / fps)
                cap.release()

    # build hashes & sequences
    vecs, seqs = [], []
    for p in paths:
        avg_hex, seq_pairs = store.get(p)
        vecs.append(_hex_to_vec(avg_hex))
        seqs.append(seq_pairs)

    if not vecs:
        print("⚠️  No videos found.")
        return pd.DataFrame()

    mat = np.stack(vecs).astype("float32")
    index = faiss.IndexFlatL2(mat.shape[1])
    if use_gpu:
        res   = faiss.StandardGpuResources()
        index = faiss.index_cpu_to_gpu(res, 0, index)
    index.add(mat)

    D, I = index.search(mat, top_k + 1)

    # stage 1: coarse candidate pairs
    raw_pairs = set()
    for i, (drow, idxrow) in enumerate(zip(D, I)):
        for dist, j in zip(drow, idxrow):
            if i == j or dist > faiss_threshold:
                continue
            if not self_compare and folder_ids[i] == folder_ids[j]:
                continue
            raw_pairs.add(tuple(sorted((i, j))))

    # stage 2: refine with limited alignment
    results = []
    for i, j in raw_pairs:
        sec_per_sample  = durations[i] / len(seqs[i]) if seqs[i] else float('inf')
        max_shift_samps = int(align_offset_limit_s / sec_per_sample)

        best_ham, best_ts = _aligned_distance_and_time_limited(
            seqs[i], seqs[j],
            max_shift_samps,
            align_offset_limit_s
        )
        if best_ham > align_threshold:
            continue

        idx = np.where(I[i] == j)[0]
        if idx.size:
            avg_diff = float(D[i][idx[0]])
        else:
            avg_diff = float(((vecs[i] - vecs[j]) ** 2).sum())

        aligned_pct = best_ham / 64.0

        results.append({
            "file_a":                    paths[i],
            "file_b":                    paths[j],
            "avg_frame_diff (0–8)":      avg_diff,
            "best_aligned_diff (0–64)":  best_ham,
            "time_shift_s":              best_ts,
            "aligned_pct_diff (0–100%)": aligned_pct
        })

    df = pd.DataFrame(results, columns=[
        "file_a",
        "file_b",
        "avg_frame_diff (0–8)",
        "best_aligned_diff (0–64)",
        "time_shift_s",
        "aligned_pct_diff (0–100%)"
    ])

    _export_excel(df, report_path)
    print(f"[find_video_duplicates] Done in {time.time() - t0:.1f}s — {len(df)} pairs saved")
    return df


# ────────────────────────────────────────────────────────────────────
# Excel export helper
# ────────────────────────────────────────────────────────────────────
def _export_excel(df: pd.DataFrame, path: str):
    with ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Duplicates")
        ws = writer.sheets["Duplicates"]

        # Bold header row & freeze
        for cell in ws[1]:
            cell.font = Font(bold=True)
        ws.freeze_panes = "A2"

        # Auto‑fit & format
        for idx, col in enumerate(df.columns, start=1):
            max_len = df[col].astype(str).map(len).max() if not df.empty else 0
            width = max(len(col), max_len) + 2
            ws.column_dimensions[get_column_letter(idx)].width = width

            if col == "aligned_pct_diff (0–100%)":
                for cell in ws[get_column_letter(idx)][1:]:
                    cell.number_format = numbers.FORMAT_PERCENTAGE_00
            elif col in ("avg_frame_diff (0–8)",
                         "best_aligned_diff (0–64)",
                         "time_shift_s"):
                for cell in ws[get_column_letter(idx)][1:]:
                    cell.number_format = numbers.FORMAT_NUMBER_00
