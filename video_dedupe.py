# video_dedupe.py
import os
import json
import atexit
import shutil
import time
from pathlib import Path
from typing import List, Dict, Tuple

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
VIDEO_HASH_STORE_PATH    = "video_hashes.json"
VIDEO_DEDUPE_REPORT_PATH = "video_duplicates.xlsx"

MAX_SAMPLES       = 20    # max key‑frame hashes per video
FAISS_THRESHOLD   = 12.0  # coarse distance cut‑off (L2 on 0/1 bit‑vector)
ALIGN_THRESHOLD   = 10.0  # fine distance cut‑off (average Hamming)
TOP_K             = 5     # how many neighbours FAISS returns
EXTS              = {'.mp4', '.mov', '.avi', '.m4v', '.mpg', '.mkv'}


# ────────────────────────────────────────────────────────────────────
# Helper – 64‑bit hex → float vector of 0/1 (for FAISS)
# ────────────────────────────────────────────────────────────────────
def _hex_to_vector(hex_str: str) -> np.ndarray:
    ba   = bytes.fromhex(hex_str)
    bits = np.unpackbits(np.frombuffer(ba, dtype=np.uint8))
    return bits.astype("float32")


# ────────────────────────────────────────────────────────────────────
# Key‑frame hashing
# ────────────────────────────────────────────────────────────────────
def _sample_keyframe_hashes(path: str, k: int = MAX_SAMPLES) -> List[str]:
    """Return ≤k 64‑bit pHash hex strings sampled evenly from the video."""
    cap = cv2.VideoCapture(path)
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video: {path}")

    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    if frame_count == 0:
        cap.release()
        raise RuntimeError(f"No frames in video: {path}")

    step = max(1, frame_count // k)
    hashes = []
    for idx in range(0, frame_count, step):
        cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ok, frame = cap.read()
        if not ok:
            break
        img = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
        hashes.append(str(imagehash.phash(img)))
        if len(hashes) >= k:
            break
    cap.release()
    return hashes


def _average_hex_hash(hex_list: List[str]) -> str:
    """Average bit‑wise over many 64‑bit hashes -> new 64‑bit hex."""
    if not hex_list:
        return "0" * 16
    bits = np.stack([_hex_to_vector(h) for h in hex_list])
    mean = bits.mean(axis=0)
    avg_bits = (mean >= 0.5).astype(np.uint8)
    packed = np.packbits(avg_bits)
    return packed.tobytes().hex()


def _hamming_hex(a: str, b: str) -> int:
    return bin(int(a, 16) ^ int(b, 16)).count("1")


def _aligned_distance(seq_a: List[str], seq_b: List[str]) -> float:
    """Slide the shorter over the longer; return best average Hamming."""
    if not seq_a or not seq_b:
        return 64.0
    L1, L2 = len(seq_a), len(seq_b)
    best = 64.0
    for shift in range(-(L2 - 1), L1):
        dists = []
        for i, h1 in enumerate(seq_a):
            j = i - shift
            if 0 <= j < L2:
                dists.append(_hamming_hex(h1, seq_b[j]))
        if dists:
            best = min(best, sum(dists) / len(dists))
    return best


# ────────────────────────────────────────────────────────────────────
# Persistent store
# ────────────────────────────────────────────────────────────────────
class VideoHashStore:
    """Caches {filepath: {mtime, avg_hex, seq_hexes}} to JSON."""
    _inst = None

    def __new__(cls, path=VIDEO_HASH_STORE_PATH):
        if cls._inst is None:
            cls._inst = super().__new__(cls)
            cls._inst._init(path)
        return cls._inst

    def _init(self, path):
        self.path   = path
        self._data: Dict[str, Dict] = {}
        self._dirty = False
        if os.path.exists(path):
            with open(path) as f:
                self._data = json.load(f)
        atexit.register(self.save_if_dirty)

    def get(self, filepath: str) -> Tuple[str, List[str]]:
        """
        Return (avg_hex, seq_hexes). Computes and caches if missing or file changed.
        """
        mtime = os.path.getmtime(filepath)
        entry = self._data.get(filepath)

        if not entry or entry.get("mtime") != mtime:
            seq = _sample_keyframe_hashes(filepath, MAX_SAMPLES)
            avg = _average_hex_hash(seq)
            self._data[filepath] = {"mtime": mtime, "avg": avg, "seq": seq}
            self._dirty = True
            print(f"[VideoHashStore] NEW hash for {filepath} ({len(seq)} samples)")
            return avg, seq

        avg, seq = entry["avg"], entry["seq"]
        print(f"[VideoHashStore] Cached hash for {filepath}")
        return avg, seq

    def save_if_dirty(self):
        if self._dirty:
            with open(self.path, "w") as f:
                json.dump(self._data, f, indent=2)
            self._dirty = False


# ────────────────────────────────────────────────────────────────────
# Main pipeline
# ────────────────────────────────────────────────────────────────────
def find_video_duplicates(
    directories: List[str],
    faiss_threshold: float = FAISS_THRESHOLD,
    align_threshold: float = ALIGN_THRESHOLD,
    top_k: int = TOP_K,
    self_compare: bool = False,
    use_gpu: bool = False,
    report_path: str = VIDEO_DEDUPE_REPORT_PATH
) -> pd.DataFrame:
    t0 = time.time()
    print(f"[find_video_duplicates] Starting at {time.strftime('%H:%M:%S')}")

    # Defer funcs import to avoid circularity
    from funcs import FileManager, get_list_of_files

    paths, folderID = [], []
    store = VideoHashStore()

    # gather video files
    for idx, folder in enumerate(directories):
        for p in get_list_of_files(folder):
            q = Path(p)
            if q.suffix.lower() in EXTS and not q.name.startswith("._"):
                paths.append(str(q))
                folderID.append(idx)

    # build vectors
    avgs, seqs = [], []
    for p in paths:
        avg_hex, seq_hexes = store.get(p)
        avgs.append(_hex_to_vector(avg_hex))
        seqs.append(seq_hexes)

    if not avgs:
        print("⚠️  No video files found.")
        return pd.DataFrame()

    mat = np.stack(avgs).astype("float32")
    index = faiss.IndexFlatL2(mat.shape[1])
    if use_gpu:
        res   = faiss.StandardGpuResources()
        index = faiss.index_cpu_to_gpu(res, 0, index)
    index.add(mat)

    D, I = index.search(mat, top_k + 1)

    # stage‑1 candidates
    raw_pairs = set()
    for i, (drow, idxrow) in enumerate(zip(D, I)):
        for dist, j in zip(drow, idxrow):
            if i == j or dist > faiss_threshold:
                continue
            if not self_compare and folderID[i] == folderID[j]:
                continue
            raw_pairs.add(tuple(sorted((i, j))))

    # stage‑2 refinement
    results = []
    for i, j in raw_pairs:
        align_dist = _aligned_distance(seqs[i], seqs[j])
        if align_dist <= align_threshold:
            avg_dist = float(D[i][np.where(I[i] == j)][0])
            results.append({
                "file_a":            paths[i],
                "file_b":            paths[j],
                "avg_distance":      avg_dist,
                "aligned_distance":  align_dist
            })

    # to DataFrame & export
    df = pd.DataFrame(results, columns=[
        "file_a", "file_b", "avg_distance", "aligned_distance"
    ])
    _export_excel(df, report_path)

    t1 = time.time()
    print(f"[find_video_duplicates] Finished in {t1 - t0:.1f} seconds")
    print(f"✅ Saved {len(df)} duplicate pairs to {report_path}")
    return df


# ────────────────────────────────────────────────────────────────────
# Excel export helper
# ────────────────────────────────────────────────────────────────────
def _export_excel(df: pd.DataFrame, path: str):
    with ExcelWriter(path, engine="openpyxl") as writer:
        sheet = "Duplicates"
        # write with 2 decimal float format
        df.to_excel(writer, index=False, sheet_name=sheet, float_format="%.2f")
        ws = writer.sheets[sheet]
        # bold header + freeze
        for cell in ws[1]:
            cell.font = Font(bold=True)
        ws.freeze_panes = "A2"
        # auto‑width + enforce number format
        for idx, col in enumerate(df.columns, 1):
            width = max(
                df[col].astype(str).map(len).max() if not df.empty else 0,
                len(col)
            ) + 2
            ws.column_dimensions[get_column_letter(idx)].width = width
            if col in ("avg_distance", "aligned_distance"):
                for cell in ws[get_column_letter(idx)][1:]:
                    cell.number_format = numbers.FORMAT_NUMBER_00
