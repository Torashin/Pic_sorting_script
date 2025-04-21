# video_dedupe.py

import os
import json
import atexit
import time
from pathlib import Path
from typing import List, Tuple, Dict
import concurrent.futures
import math
import numpy as np
import pandas as pd
import cv2
from PIL import Image
import imagehash
import faiss
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, numbers
from pandas import ExcelWriter
import platform
import subprocess
import PySimpleGUI as sg

# ────────────────────────────────────────────────────────────────────
# Progress Counters (shared between hashing and duplicate finder)
# ────────────────────────────────────────────────────────────────────
TOTAL_VIDEOS = 0
PROCESSED_VIDEOS = 0

# ────────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────────
VIDEO_HASH_STORE_PATH     = "video_hashes.json"
VIDEO_DEDUPE_REPORT_PATH  = "video_duplicates.xlsx"

MAX_SAMPLES               = 40      # max frame sample count per video
MIN_SAMPLES               = 5       # min frame sample count per video
FAISS_THRESHOLD           = 12      # raw L2 on 64‑bit super‑hash
ALIGN_THRESHOLD           = 10.0    # raw mean Hamming
ALIGN_OFFSET_LIMIT_S      = 60.0    # max time offset for alignment (seconds)
TOP_K                     = 5
EXTS                      = {'.mp4', '.mov', '.avi', '.m4v', '.mpg', '.mkv'}
SAVE_EVERY                = 5       # save cache every N new entries
MAX_WORKERS               = 4       # threads for hashing/metadata

# ────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────
def _hex_to_vec(hex_str: str) -> np.ndarray:
    """Convert a 64‑bit hex into a 64‑dim 0/1 float32 vector."""
    ba   = bytes.fromhex(hex_str)
    bits = np.unpackbits(np.frombuffer(ba, dtype=np.uint8))
    return bits.astype("float32")


def _calculate_target_samples(duration: float) -> int:
    """
    Calculate the number of target frames to sample based on video duration (in seconds),
    using a logarithmic scaling that works well from ~2 seconds to 2 hours.
    The result is clamped between 5 and 40.
    """
    if duration <= 0:
        return 5
    samples = int(math.log(duration + 1, 10) * 12)
    return max(MIN_SAMPLES, min(samples, MAX_SAMPLES))


def _sample_hashes_with_times(path: str) -> List[Tuple[str, float]]:
    cap = cv2.VideoCapture(path)
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video: {path}")

    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT)) or 1
    duration = frame_count / fps

    k = _calculate_target_samples(duration)  # calculate target number of samples based on duration

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



def _average_hex(pairs: List[Tuple[str, float]]) -> str:
    hexes = [h for (h, _) in pairs]
    if not hexes:
        return "0" * 16
    bits = np.stack([_hex_to_vec(h) for h in hexes])
    avg_bits = (bits.mean(axis=0) >= 0.5).astype(np.uint8)
    packed = np.packbits(avg_bits)
    return packed.tobytes().hex()


def _hamming(a: str, b: str) -> int:
    return bin(int(a, 16) ^ int(b, 16)).count("1")


def _aligned_distance_and_time_limited(
    seq_a: List[Tuple[str, float]],
    seq_b: List[Tuple[str, float]],
    max_shift_samples: int,
    offset_limit_s: float
) -> Tuple[float, float]:
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
                    dists.append(_hamming(ha, hb))
                    ts_list.append(ts)
        if dists:
            mean_d = sum(dists) / len(dists)
            if mean_d < best_dist:
                best_dist = mean_d
                best_time_shift = sum(ts_list) / len(ts_list)
    return best_dist, best_time_shift

# ────────────────────────────────────────────────────────────────────
# Persistent cache
# ────────────────────────────────────────────────────────────────────
class VideoHashStore:
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
        if os.path.exists(path):
            with open(path) as f:
                self._data = json.load(f)
        atexit.register(self.save_if_dirty)

    def get(self, filepath: str) -> Tuple[str, List[Tuple[str, float]]]:
        global PROCESSED_VIDEOS, TOTAL_VIDEOS
        mtime = os.path.getmtime(filepath)
        entry = self._data.get(filepath)
        if not entry or entry.get("mtime") != mtime:
            seq_pairs = _sample_hashes_with_times(filepath)
            avg_hex    = _average_hex(seq_pairs)
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

    def save_if_dirty(self):
        if self._dirty:
            with open(self.path, "w") as f:
                json.dump(self._data, f, indent=2)
            self._dirty = False
            print(f"[HashStore] cache saved ({len(self._data)} items)", flush=True)

# ────────────────────────────────────────────────────────────────────
# Worker for metadata + hashing
# ────────────────────────────────────────────────────────────────────
def _process_video(path_fid):
    path, fid = path_fid
    try:
        cap = cv2.VideoCapture(path)
        if not cap.isOpened():
            return None
        fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
        cnt = cap.get(cv2.CAP_PROP_FRAME_COUNT) or 1
        cap.release()
        avg_hex, seq = VideoHashStore().get(path)
        file_size = os.path.getsize(path)  # Add this
        duration = cnt / fps
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
    use_gpu: bool               = False,
    report_path: str            = VIDEO_DEDUPE_REPORT_PATH
) -> pd.DataFrame:
    t0 = time.time()
    print(f"[find_video_duplicates] Start {time.strftime('%H:%M:%S')}", flush=True)
    from funcs import get_list_of_files

    # gather and filter files
    all_tasks = []
    for fid, folder in enumerate(directories):
        all_files = get_list_of_files(folder)
        print(f"[find_video_duplicates] Folder {fid}: {folder} → {len(all_files)} files", flush=True)
        video_paths = [f for f in all_files
                       if Path(f).suffix.lower() in EXTS
                       and '_gsdata_' not in f
                       and not Path(f).name.startswith("._")]
        total_videos = len(video_paths)
        global TOTAL_VIDEOS
        TOTAL_VIDEOS = total_videos
        print(f"[find_video_duplicates] → {total_videos} video files", flush=True)
        all_tasks += [(p, fid) for p in video_paths]

    # parallel processing
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
        futures = {exec.submit(_process_video, t): t for t in all_tasks}
        for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            res = fut.result()
            if res:
                results.append(res)
            if i % 100 == 0 or i==len(futures):
                print(f"[find_video_duplicates] Processed {i}/{len(futures)} videos", flush=True)

    if not results:
        print("⚠️  No readable videos found.", flush=True)
        return pd.DataFrame()

    # unpack metadata
    paths, folder_ids, durations, vecs, seqs, sizes = [], [], [], [], [], []
    for path, fid, fps, cnt, avg_hex, seq, file_size, duration in results:
        paths.append(path)
        folder_ids.append(fid)
        durations.append(cnt / fps)
        vecs.append(_hex_to_vec(avg_hex))
        seqs.append(seq)
        durations.append(duration)
        sizes.append(file_size)

    # FAISS index
    print(f"[find_video_duplicates] {len(vecs)} videos hashed, building FAISS index...", flush=True)
    mat = np.stack(vecs).astype("float32")
    index = faiss.IndexFlatL2(mat.shape[1])
    if use_gpu:
        res   = faiss.StandardGpuResources()
        index = faiss.index_cpu_to_gpu(res, 0, index)
    index.add(mat)
    D, I = index.search(mat, top_k+1)
    print(f"[find_video_duplicates] FAISS search done", flush=True)

    # refine pairs
    raw_pairs = set()
    for i, (drow, idxrow) in enumerate(zip(D, I)):
        for dist, j in zip(drow, idxrow):
            if i==j or dist>faiss_threshold: continue
            if not self_compare and folder_ids[i]==folder_ids[j]: continue
            raw_pairs.add(tuple(sorted((i,j))))
    print(f"[find_video_duplicates] {len(raw_pairs)} candidate pairs", flush=True)

    results = []
    for i,j in raw_pairs:
        sec_per = durations[i]/len(seqs[i]) if seqs[i] else float('inf')
        max_shift = int(align_offset_limit_s/sec_per)
        best_h, best_ts = _aligned_distance_and_time_limited(
            seqs[i], seqs[j], max_shift, align_offset_limit_s
        )
        if best_h>align_threshold: continue
        idx = np.where(I[i]==j)[0]
        avg_d = float(D[i][idx[0]]) if idx.size else float(((vecs[i]-vecs[j])**2).sum())
        aligned_pct_diff = best_h/64.0
        temporal_pct_diff = abs(best_ts) / durations[i]
        length_pct_diff = length_pct_diff = abs(durations[i] - durations[j]) / max(durations[i], durations[j])
        results.append({
            "file_a":paths[i],
            "size_a (MB)": round(sizes[i] / (1024 * 1024), 2),
            "duration_a (s)": durations[i],
            "file_b":paths[j],
            "size_b (MB)": round(sizes[j] / (1024 * 1024), 2),
            "duration_b (s)": durations[j],
            "avg_frame_diff (0–64)":avg_d,
            "best_aligned_diff (0–64)":best_h,
            "time_shift_s":best_ts,
            "aligned_pct_diff (0–100%)": aligned_pct_diff,
            "temporal_pct_diff (0–100%)": temporal_pct_diff,
            "length_pct_diff (0–100%)": length_pct_diff,
            "overall_difference (0–100%)": (aligned_pct_diff + temporal_pct_diff + length_pct_diff) / 3
        })
    print(f"[find_video_duplicates] {len(results)} duplicates found, exporting...", flush=True)

    df = pd.DataFrame(results)
    export_excel(df, report_path)
    print(f"[find_video_duplicates] Done in {time.time()-t0:.1f}s — {len(results)} pairs saved", flush=True)
    open_excel_file(VIDEO_DEDUPE_REPORT_PATH)
    return df

# ────────────────────────────────────────────────────────────────────
# Excel export helper
# ────────────────────────────────────────────────────────────────────
import PySimpleGUI as sg

def export_excel(df: pd.DataFrame, path: str):
    while True:
        try:
            with ExcelWriter(path, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Duplicates")
                ws = writer.sheets["Duplicates"]
                for cell in ws[1]:
                    cell.font = Font(bold=True)
                ws.freeze_panes = "A2"
                for idx, col in enumerate(df.columns, start=1):
                    max_len = df[col].astype(str).map(len).max() if not df.empty else 0
                    width = max(len(col), max_len) + 2
                    ws.column_dimensions[get_column_letter(idx)].width = width

                    if "%" in col in col:
                        fmt = numbers.FORMAT_PERCENTAGE_00
                    elif "duration" in col.lower() or "time" in col.lower():
                        fmt = numbers.FORMAT_NUMBER_00
                    elif "size" in col.lower() and "mb" in col.lower():
                        fmt = numbers.FORMAT_NUMBER_00
                    else:
                        fmt = numbers.FORMAT_GENERAL

                    for cell in ws[get_column_letter(idx)][1:]:
                        cell.number_format = fmt
            break  # Success, exit loop

        except PermissionError:
            choice = sg.popup_yes_no(
                f"Can't write to file:\n{path}\n\nIt might be open in Excel.\n\nWould you like to retry?",
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
    try:
        if platform.system() == "Windows":
            os.startfile(path)
        elif platform.system() == "Darwin":
            subprocess.call(["open", path])
        elif platform.system() == "Linux":
            subprocess.call(["xdg-open", path])
    except Exception as e:
        print(f"Could not open Excel file: {e}")


#   TODO: Add to GUI
#   TODO: Make use of FileManager to get file properties like file size, vid length, and maybe also hashes (instead of accessing directly from hashstore)

