# Pic Sorting Script

## Overview
This project started as a personal media-library tool and now contains two substantial workflows:

1. Photo/video sorting and renaming based on metadata, filesystem dates, and folder/date hints.
2. Home-video deduplication, timestamp extraction, review reporting, and lossless remux planning.

It is still a pragmatic working tool rather than a polished end-user application. The code is useful, but it assumes the user is comfortable editing Python run blocks, reviewing generated Excel workbooks, and making backups before applying changes.

## Main components

### `main.py`
PySimpleGUI desktop front-end for the original sorting workflow in `funcs.py`.

### `funcs.py`
Core photo/video sorting, metadata handling, copy/move logic, and assorted file-management helpers.

### `video_dedupe.py`
The main dedupe pipeline. It can:

- compare two folders of videos
- run anchor/audio/timeline matching
- scan camcorder on-screen timestamps
- infer coverage/unique-content relationships
- build review workbooks in `reports/`
- maintain a unified cache in `resources/cache/video_cache.json`

### `video_rename_remux.py`
Workbook-driven execution runner for:

- renaming files based on `Rename_Queue` / `Rename_Done`
- losslessly remuxing segments from `Remux_Plan` and optionally `Remux_Short`
- migrating cache keys when files are renamed

## Typical dedupe workflow
1. Run `video_dedupe.py` to refresh scans and rebuild `reports/dedupe_consolidated.xlsx`.
2. Review the workbook:
   - rename decisions in `Rename_Queue`
   - remux decisions in `Remux_Plan`
   - optional short segments in `Remux_Short`
3. Run `video_rename_remux.py` in dry-run mode first.
4. Run `video_rename_remux.py` with `apply=True` once satisfied.

## Requirements

Use Python 3.14 with Tcl/Tk installed. The pinned Windows package versions
are in `requirements.txt`. With PyCharm using the project's `.venv`, install
from the repository root using PowerShell:

```powershell
& .\.venv\Scripts\python.exe -m pip install --only-binary=:all: -r requirements.txt
& .\.venv\Scripts\python.exe -m pip check
& .\.venv\Scripts\python.exe main.py
```

The environment uses NumPy 2 and includes pandas, FAISS, OpenCV, and HEIF
support. pandas remains on 2.3 and OpenCV on 4.x to retain the existing APIs.
Python 3.15 is not supported by this setup because FAISS has no compatible
wheel as of 18 October 2026.

Run the compatibility checks from the repository root (the GUI check briefly
opens a window; sorting checks use temporary sample files):

```powershell
& .\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

External tools used by the project:

- `ffmpeg`
- `ffprobe`
- `ExifTool`

This repo keeps `resources/exiftool.exe` and `resources/exiftool` in-tree.
On Windows, `funcs.py` explicitly passes the bundled executable's absolute
path to PyExifTool, so ExifTool does not need to be on PATH. FFmpeg and ffprobe
must be on PATH for video/audio processing.

## PySimpleGUI note
The repository includes:

- `Other/PySimpleGUI-4.60.5-main/PySimpleGUI-4.60.5-py3-none-any.whl`

This wheel is retained for historical use. The current requirements install
PySimpleGUI 6.3 from PyPI, whose window creation works with Tcl/Tk 9. The old
4.60.5 wheel uses a removed Tcl command and must not be installed in this
environment.

## Generated files
These are runtime artifacts and should not normally be committed:

- `reports/`
- `resources/cache/`
- `resources/timestamp_debug/`
- local workflow test outputs under `Other/`

The root `.gitignore` excludes them.

## Local and public branches

`master` is the public branch and uses generic example paths. Keep personal
paths and run settings on the local `private` branch. Transfer shared fixes
with selective cherry-picks rather than merging private history into master.
Push the public branch explicitly with `git push origin master`; avoid
`--all` and `--mirror`. Local `.env` files and `local_settings.json` are ignored.

## Utility scripts in `Other/`
`Other/` contains small standalone helpers that were useful during development and are kept as generic utilities:

- `Other/folder_renaming.py`
- `Other/list_files_in_folder.py`

They are not part of the core dedupe pipeline.

## Status / caveats
- The project is powerful but not simplified.
- The dedupe workflow is designed around spreadsheet review.
- Some run blocks still assume the user will edit local paths before running.
- Backups are strongly recommended before any rename/move/remux operation.

## License
This project is licensed under the GNU General Public License v3.0. See `COPYING`.
