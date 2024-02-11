import os

# Written to format names of folders created by Apple Photos during library export. Numbers in the location
# description can confuse the function that tries to get a date from folder names.

# Specify the directory where you want to perform the renaming
base_directory = r"C:\Users\james\Desktop\2"

def generate_unique_directory_name(base_name, existing_names):
    if not existing_names:
        return base_name

    n = 2
    new_name = base_name
    while any(new_name.lower() == name.lower() for name in existing_names):
        new_name = f"{base_name} ({n})"
        n += 1
    return new_name


def rename_directories(directory):
    existing_names = set(os.listdir(directory))

    for dir_name in os.listdir(directory):
        old_path = os.path.join(directory, dir_name)

        if ',' in dir_name:
            parts = dir_name.rsplit(',', 1)  # Split based on the last comma
            if len(parts) == 2:
                new_name = parts[1].strip()
            else:
                new_name = dir_name  # If no comma, keep the original name

            new_name = generate_unique_directory_name(new_name, existing_names)
            existing_names.remove(dir_name)
            existing_names.add(new_name)

            new_path = os.path.join(directory, new_name)
            os.rename(old_path, new_path)
            print(f"Renamed: {dir_name} -> {new_name}")


if os.path.exists(base_directory):
    if not os.path.isdir(base_directory):
        print(f"'{base_directory}' is not a directory.")
    else:
        rename_directories(base_directory)
else:
    print(f"'{base_directory}' does not exist.")

