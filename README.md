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

Python packages are listed in `requirements.txt`.

External tools used by the project:

- `ffmpeg`
- `ffprobe`
- `ExifTool`

This repo keeps `resources/exiftool.exe` and `resources/exiftool` in-tree so the original sorting workflow still works in environments where ExifTool is not already installed.

## PySimpleGUI note
The repository includes:

- `Other/PySimpleGUI-4.60.5-main/PySimpleGUI-4.60.5-py3-none-any.whl`

This is intentional. Older free PySimpleGUI versions became difficult to obtain reliably, and this project still depends on that package for the original GUI workflow.

## Generated files
These are runtime artifacts and should not normally be committed:

- `reports/`
- `resources/cache/`
- `resources/timestamp_debug/`
- local workflow test outputs under `Other/`

The root `.gitignore` excludes them.

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
