from pathlib import Path

def get_relative_path(relative_path: str | Path) -> Path:
    """Gets a path relative to the current file.

    Args:
        relative_path: The path relative to the current file.

    Returns:
        A pathlib.Path object representing the absolute path.
    """
    return (Path(__file__).resolve().parent / relative_path).resolve()