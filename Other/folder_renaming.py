import argparse
import os
from pathlib import Path


def generate_unique_directory_name(base_name: str, existing_names: set[str]) -> str:
    if not existing_names:
        return base_name
    n = 2
    new_name = base_name
    while any(new_name.lower() == name.lower() for name in existing_names):
        new_name = f"{base_name} ({n})"
        n += 1
    return new_name


def rename_directories(directory: str, *, dry_run: bool = False) -> list[tuple[str, str]]:
    renamed: list[tuple[str, str]] = []
    existing_names = set(os.listdir(directory))
    for dir_name in os.listdir(directory):
        old_path = os.path.join(directory, dir_name)
        if not os.path.isdir(old_path) or "," not in dir_name:
            continue
        parts = dir_name.rsplit(",", 1)
        new_name = parts[1].strip() if len(parts) == 2 else dir_name
        new_name = generate_unique_directory_name(new_name, existing_names)
        if new_name == dir_name:
            continue
        existing_names.remove(dir_name)
        existing_names.add(new_name)
        new_path = os.path.join(directory, new_name)
        if not dry_run:
            os.rename(old_path, new_path)
        renamed.append((dir_name, new_name))
    return renamed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rename exported photo-library subfolders by keeping only the suffix after the last comma."
    )
    parser.add_argument("directory", help="Directory whose child folders should be renamed")
    parser.add_argument("--dry-run", action="store_true", help="Show planned renames without changing anything")
    args = parser.parse_args()

    directory = Path(args.directory)
    if not directory.exists():
        raise FileNotFoundError(f"Directory does not exist: {directory}")
    if not directory.is_dir():
        raise NotADirectoryError(f"Not a directory: {directory}")

    renamed = rename_directories(str(directory), dry_run=bool(args.dry_run))
    for old_name, new_name in renamed:
        prefix = "Would rename" if args.dry_run else "Renamed"
        print(f"{prefix}: {old_name} -> {new_name}")
    print(f"Updated {len(renamed)} folder(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
