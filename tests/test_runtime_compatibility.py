"""Integration checks for the Python 3.14 environment, using temporary media."""

import importlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import faiss
import imagehash
import numpy as np
import pandas as pd
from PIL import Image
from pillow_heif import register_heif_opener

import audio_processing
import funcs
import main
import video_dedupe as vd


class RuntimeCompatibilityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pic_sorting_test_")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def make_photo(self):
        source = self.root / "source"
        source.mkdir()
        photo = source / "2024-02-19.jpg"
        exif = Image.Exif()
        exif[36867] = "2024:02:19 12:34:56"
        Image.new("RGB", (64, 64), "red").save(photo, exif=exif)
        return photo

    def test_all_entry_points_import(self):
        for module in ("main", "video_dedupe", "video_rename_remux",
                       "audio_processing", "camcorder_timestamp_ocr"):
            with self.subTest(module=module):
                importlib.import_module(module)

    def test_gui_window_and_event_loop(self):
        app = main.PicSortingScriptGUI()
        self.addCleanup(app.window.close)
        app.window.hide()
        app.window.read(timeout=20)
        app.update_status("Compatibility check")
        self.assertTrue(app.window["-copy-"].get())
        self.assertEqual(app.status, "Compatibility check")

    @unittest.skipUnless(os.name == "nt", "Uses the bundled Windows ExifTool")
    def test_metadata_read_and_edit_without_exiftool_on_path(self):
        photo = self.make_photo()
        previous_cwd = Path.cwd()
        try:
            os.chdir(self.root)
            metadata = funcs.get_metadata(str(photo))
            self.assertEqual(metadata[0]["EXIF:DateTimeOriginal"], "2024:02:19 12:34:56")
            funcs.edit_metadata(str(photo), {"DateTimeOriginal": "2024:02:20 01:02:03"})
            metadata = funcs.get_metadata(str(photo))
            self.assertEqual(metadata[0]["EXIF:DateTimeOriginal"], "2024:02:20 01:02:03")
        finally:
            os.chdir(previous_cwd)

    @unittest.skipUnless(os.name == "nt", "Uses the bundled Windows ExifTool")
    def test_sorting_copies_photo_into_metadata_month(self):
        photo = self.make_photo()
        destination = self.root / "destination"
        destination.mkdir()
        funcs.bulkprocess(str(photo.parent), str(destination), movefiles=False)
        copied = destination / "2024-02" / photo.name
        self.assertTrue(photo.exists())
        self.assertEqual(copied.read_bytes(), photo.read_bytes())
        self.assertEqual(len(list(destination.rglob("*.jpg"))), 1)

    def test_heif_roundtrip_and_image_hashing(self):
        register_heif_opener()
        path = self.root / "sample.heic"
        original = Image.new("RGB", (64, 64), "red")
        original.save(path, format="HEIF")
        with Image.open(path) as restored:
            self.assertEqual(restored.size, original.size)
            self.assertEqual(imagehash.phash(restored), imagehash.phash(original))

    def test_faiss_search_with_numpy_2(self):
        vectors = np.stack([vd._hex_to_vec("0000000000000000"),
                            vd._hex_to_vec("ffffffffffffffff")]).astype(np.float32)
        index = faiss.IndexFlatL2(vectors.shape[1])
        index.add(vectors)
        distances, ids = index.search(vectors, 1)
        self.assertEqual(ids[:, 0].tolist(), [0, 1])
        np.testing.assert_allclose(distances, 0)

    def test_project_excel_export_and_read(self):
        path = self.root / "report.xlsx"
        frame = pd.DataFrame({"path": ["sample.mp4"], "duration_s": [12.5]})
        vd.export_excel(frame, str(path))
        restored = pd.read_excel(path)
        self.assertEqual(restored["path"].tolist(), ["sample.mp4"])
        # The exporter formats seconds as Excel elapsed time.
        self.assertAlmostEqual(restored.loc[0, "duration_s"].total_seconds(), 12.5)

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"),
                         "Requires FFmpeg and ffprobe on PATH")
    def test_video_sampling_and_audio_fingerprints(self):
        path = self.root / "sample.mp4"
        subprocess.run([
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
            "-f", "lavfi", "-i", "testsrc2=size=160x120:rate=10:duration=3",
            "-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=8000:duration=3",
            "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", str(path),
        ], check=True, capture_output=True, timeout=30)
        self.assertAlmostEqual(audio_processing.ffprobe_duration_seconds(str(path)), 3, delta=0.2)
        self.assertTrue(vd._sample_hashes_with_times(str(path)))
        hashes = vd.sample_timeline_hashes(str(path), trim=(0, 0), min_bitcount=0, max_bitcount=64)
        self.assertTrue(hashes)
        self.assertTrue(all(len(hash_bytes) == 8 for _, hash_bytes in hashes))
        self.assertTrue(audio_processing.compute_audio_fingerprints(str(path)))


if __name__ == "__main__":
    unittest.main()
