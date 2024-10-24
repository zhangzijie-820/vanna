import os


def check_directory_exists(directory_path) -> bool:
    return os.path.exists(directory_path) and os.path.isdir(directory_path)
