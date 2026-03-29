import os
import argparse
from pathlib import Path

from openpyxl import Workbook

DEFAULT_VIDEO_EXTS = {".mp4", ".mov", ".avi", ".m4v", ".mpg", ".mkv"}


def list_video_files(folder: str, extensions: set[str] | None = None) -> list[str]:
    exts = {ext.lower() for ext in (extensions or DEFAULT_VIDEO_EXTS)}
    files = []
    for root, _dirs, filenames in os.walk(folder):
        for name in filenames:
            if Path(name).suffix.lower() in exts:
                files.append(os.path.normpath(os.path.join(root, name)))
    return files


def write_video_list_report(
    folder: str,
    report_path: str | None = None,
    extensions: set[str] | None = None,
) -> str:
    if report_path is None:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        report_path = os.path.join(project_root, "reports", "video_files.xlsx")

    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    videos = list_video_files(folder, extensions=extensions)

    wb = Workbook()
    ws = wb.active
    ws.title = "Videos"
    ws.append(["video_path"])
    for path in videos:
        ws.append([path])

    max_len = max((len(p) for p in videos), default=len("video_path"))
    ws.column_dimensions["A"].width = min(max_len + 2, 120)
    wb.save(report_path)
    return report_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Write an Excel report listing all video files under a folder.")
    parser.add_argument("folder", help="Folder to scan recursively")
    parser.add_argument("--report", default=None, help="Output workbook path (default: reports/video_files.xlsx)")
    args = parser.parse_args()

    output = write_video_list_report(args.folder, report_path=args.report)
    print(f"Wrote report to {output}")
