import datetime as dt
import os
import re
import subprocess
import tempfile
from typing import Any

import cv2


MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def _winrt_ocr_text(image_path: str) -> str:
    """
    OCR via built-in Windows Runtime OCR engine.
    Returns empty string on failure.
    """
    ps_path = image_path.replace("'", "''")
    ps = f"""
$ErrorActionPreference='Stop'
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$path='{ps_path}'
Add-Type -AssemblyName System.Runtime.WindowsRuntime
$null = [Windows.Storage.StorageFile,Windows.Storage,ContentType=WindowsRuntime]
$null = [Windows.Storage.Streams.IRandomAccessStream,Windows.Storage.Streams,ContentType=WindowsRuntime]
$null = [Windows.Media.Ocr.OcrEngine,Windows.Foundation,ContentType=WindowsRuntime]
$null = [Windows.Media.Ocr.OcrResult,Windows.Foundation,ContentType=WindowsRuntime]
$null = [Windows.Graphics.Imaging.BitmapDecoder,Windows.Graphics,ContentType=WindowsRuntime]
$null = [Windows.Graphics.Imaging.SoftwareBitmap,Windows.Graphics,ContentType=WindowsRuntime]
function Await([object]$WinRtTask, [Type]$ResultType) {{
    $asTaskGeneric = [System.WindowsRuntimeSystemExtensions].GetMethods() |
        Where-Object {{ $_.Name -eq 'AsTask' -and $_.IsGenericMethod -and $_.GetParameters().Count -eq 1 }} |
        Select-Object -First 1
    $asTask = $asTaskGeneric.MakeGenericMethod($ResultType)
    $netTask = $asTask.Invoke($null, @($WinRtTask))
    $netTask.Wait()
    return $netTask.Result
}}
$file = Await ([Windows.Storage.StorageFile]::GetFileFromPathAsync($path)) ([Windows.Storage.StorageFile])
$stream = Await ($file.OpenAsync([Windows.Storage.FileAccessMode]::Read)) ([Windows.Storage.Streams.IRandomAccessStream])
$decoder = Await ([Windows.Graphics.Imaging.BitmapDecoder]::CreateAsync($stream)) ([Windows.Graphics.Imaging.BitmapDecoder])
$bitmap = Await ($decoder.GetSoftwareBitmapAsync()) ([Windows.Graphics.Imaging.SoftwareBitmap])
$engine = [Windows.Media.Ocr.OcrEngine]::TryCreateFromUserProfileLanguages()
$result = Await ($engine.RecognizeAsync($bitmap)) ([Windows.Media.Ocr.OcrResult])
Write-Output $result.Text
"""
    try:
        out = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps],
            capture_output=True,
            text=False,
            check=False,
            timeout=30,
        )
        if out.returncode == 0:
            raw = out.stdout or b""
            try:
                text = raw.decode("utf-8", errors="replace")
            except Exception:
                text = raw.decode(errors="replace")
        else:
            text = ""
    except Exception:
        text = ""
    return " ".join((text or "").replace("\r", "\n").split()).strip()


def _timestamp_roi_tophat(image_bgr) -> Any:
    """
    Tuned preprocessor for Hi8 camcorder OSD text in lower-right corner.
    Fixed ROI -> upscale -> grayscale -> local contrast -> mild sharpen.
    Keep this grayscale rather than hard-thresholding; WinRT OCR copes better
    with bright footage when the white stamp is still embedded in scene detail.
    """
    h, w = image_bgr.shape[:2]
    # Tight bottom-right crop around the two timestamp lines.
    # The earlier wide crop worked on dark scenes but admitted too much bright
    # background clutter, causing OCR misses later in clips.
    y0 = int(round(0.760 * h))
    y1 = int(round(0.970 * h))
    x0 = int(round(0.520 * w))
    roi = image_bgr[y0:y1, x0:w]
    if roi.size == 0:
        return image_bgr
    up = cv2.resize(roi, None, fx=4.0, fy=4.0, interpolation=cv2.INTER_CUBIC)
    gray = cv2.cvtColor(up, cv2.COLOR_BGR2GRAY)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)).apply(gray)
    sharp = cv2.addWeighted(clahe, 1.5, cv2.GaussianBlur(clahe, (0, 0), 1.2), -0.5, 0)
    return sharp


def _roi_likely_has_timestamp_text(proc_roi) -> bool:
    """
    Cheap text-likeness heuristic for the preprocessed timestamp ROI.
    Used to trigger local burst rescans even when OCR on the coarse frame
    misses the actual characters.
    """
    if proc_roi is None:
        return False
    arr = proc_roi
    if getattr(arr, "ndim", 0) == 3:
        arr = cv2.cvtColor(arr, cv2.COLOR_BGR2GRAY)
    if arr is None or arr.size == 0:
        return False
    h = int(arr.shape[0])
    focus = arr[int(round(0.45 * h)):, :]
    if focus.size == 0:
        focus = arr

    edges = cv2.Canny(focus, 80, 180)
    edge_count = int((edges > 0).sum())
    bright_count = int((focus >= 210).sum())

    _, th = cv2.threshold(focus, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    _n, _labels, stats, _centroids = cv2.connectedComponentsWithStats(th, 8)
    small_components = 0
    for row in stats[1:]:
        area = int(row[cv2.CC_STAT_AREA])
        w = int(row[cv2.CC_STAT_WIDTH])
        h_box = int(row[cv2.CC_STAT_HEIGHT])
        if 8 <= area <= 800 and 4 <= w <= 120 and 8 <= h_box <= 120:
            small_components += 1

    return bool(
        (edge_count >= 1800 and bright_count >= 500 and small_components >= 6)
        or (edge_count >= 3000 and small_components >= 4)
    )


def _normalize_ocr_token_digits(text: str) -> str:
    """
    Normalize common OCR confusions inside mostly-numeric tokens.
    """
    trans = str.maketrans({
        "I": "1",
        "L": "1",
        "|": "1",
        "S": "9",
        "O": "0",
        "Q": "0",
        "D": "0",
        "B": "8",
        "Z": "2",
    })
    return text.translate(trans)


def _extract_month(text: str) -> int | None:
    m = re.search(r"\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\b", text.upper())
    if not m:
        return None
    return MONTHS.get(m.group(1))


def _extract_day(text: str) -> int | None:
    up = text.upper()
    month_match = re.search(r"\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\b", up)
    if not month_match:
        return None
    # Use the final numeric token before the month label.
    # This avoids mixing the HH:MM token into the day extraction.
    prefix = up[:month_match.start()]
    nums = re.findall(r"\d+", prefix)
    if not nums:
        return None
    digits = nums[-1]
    if not digits:
        return None
    if len(digits) == 1:
        d = int(digits)
        return d if 1 <= d <= 31 else None
    if len(digits) == 2:
        d = int(digits)
        return d if 1 <= d <= 31 else None
    if len(digits) >= 3:
        # OCR often inserts one spurious digit in day; e.g. "241" -> 21.
        cands = []
        if len(digits) >= 3:
            tail3 = digits[-3:]
            cands.append(int(tail3[0] + tail3[2]))
        cands.append(int(digits[:2]))
        cands.append(int(digits[-2:]))
        for d in cands:
            if 1 <= d <= 31:
                return d
    return None


def _extract_year(text: str) -> int | None:
    up = text.upper()
    # Prefer explicit 4-digit year.
    m4 = re.search(r"\b(19\d{2}|20\d{2})\b", up)
    if m4:
        return int(m4.group(1))
    # Handle common OCR overrun: 5 digits like "20011" -> 2001.
    m5 = re.search(r"\b(19\d{3}|20\d{3})\b", up)
    if m5:
        s = m5.group(1)
        y1 = int(s[:4])
        y2 = int(s[-4:])
        if 1900 <= y1 <= 2099:
            return y1
        if 1900 <= y2 <= 2099:
            return y2
    # Retry after normalizing common OCR digit confusions, e.g. "1S94" -> "1994".
    norm = _normalize_ocr_token_digits(up)
    m4n = re.search(r"\b(19\d{2}|20\d{2})\b", norm)
    if m4n:
        return int(m4n.group(1))
    m5n = re.search(r"\b(19\d{3}|20\d{3})\b", norm)
    if m5n:
        s = m5n.group(1)
        y1 = int(s[:4])
        y2 = int(s[-4:])
        if 1900 <= y1 <= 2099:
            return y1
        if 1900 <= y2 <= 2099:
            return y2
    return None


def _extract_time(text: str) -> str | None:
    up = text.upper()
    # Typical OSD has "HH:MM AM/PM"; OCR may return separators as '-' or '.'.
    m = re.search(r"\b([0-2]?\d)\s*[:.\-]\s*([0-5]\d)\s*(AM|PM)?\b", up)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if hh > 23:
        return None
    ampm = m.group(3)
    if ampm:
        return f"{hh:02d}:{mm:02d} {ampm}"
    return f"{hh:02d}:{mm:02d}"


def extract_camcorder_date_from_image(image_path: str) -> dict[str, Any]:
    """
    Isolated demo extractor for camcorder OSD date/time in a single image.
    Single-pass OCR: preprocess fixed bottom-right ROI once, OCR once.
    Returns structured fields plus OCR debug text.
    """
    img = cv2.imread(image_path)
    if img is None:
        raise FileNotFoundError(f"Could not read image: {image_path}")

    with tempfile.TemporaryDirectory() as td:
        proc_path = os.path.join(td, "timestamp_roi_tophat.png")
        cv2.imwrite(proc_path, _timestamp_roi_tophat(img))
        ocr_text = _winrt_ocr_text(proc_path)

    month = _extract_month(ocr_text)
    day = _extract_day(ocr_text)
    year = _extract_year(ocr_text)
    time_text = _extract_time(ocr_text)

    date_iso = None
    if year and month and day:
        try:
            date_iso = dt.date(year, month, day).isoformat()
        except ValueError:
            date_iso = None

    confidence = "low"
    if date_iso:
        confidence = "high" if time_text else "medium"
    elif (month is not None) and (year is not None):
        confidence = "medium"

    return {
        "date_iso": date_iso,
        "year": year,
        "month": month,
        "day": day,
        "time_text": time_text,
        "confidence": confidence,
        # Kept for backward compatibility with prior debug keys.
        "ocr_full_text": "",
        "ocr_roi_text": ocr_text,
        "ocr_merged_text": ocr_text,
        "ocr_text": ocr_text,
    }


if __name__ == "__main__":
    print()
    TEST_IMAGE_PATH = r""  # Set to a local image path before running this file directly.
    if not TEST_IMAGE_PATH:
        print("Set TEST_IMAGE_PATH in __main__ before running this file directly.")
    else:
        result = extract_camcorder_date_from_image(TEST_IMAGE_PATH)
        print(result)
