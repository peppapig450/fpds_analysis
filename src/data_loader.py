import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any

from fpds import fpdsRequest

from config import Config

CHUNK_SIZE = 4096


class DataLoader:
    """Handles all data loading operations for contract data."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.data_directory = self.get_relative_path(self.config.data_directory)
        self.processed_files_path = self.data_directory / "processed_hashes.txt"

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculates the SHA256 hash of a file."""
        hasher = hashlib.sha256()
        with file_path.open("rb") as f:
            for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    @staticmethod
    def get_relative_path(relative_path: str | Path) -> Path:
        """Gets a path relative to the current file.

        Args:
            relative_path: The path relative to the current file.

        Returns:
            A pathlib.Path object representing the absolute path.
        """
        return (Path(__file__).resolve().parent / relative_path).resolve()

    def _read_processed_files(self) -> set:
        """Reads the set of processed file hashes from a file."""
        if self.processed_files_path.exists():
            try:
                return set(
                    self.processed_files_path.read_text(encoding="utf-8").splitlines()
                )
            except Exception as e:
                print(f"Error reading processed files: {e}")
                return set()
        return set()

    def _write_processed_files(self, processed_hashes: set):
        """Write the set of a processed file hashes to a file."""
        try:
            self.processed_files_path.write_text(
                "\n".join(sorted(processed_hashes)), encoding="utf-8"
            )
        except Exception as e:
            print(f"Error writing processed files: {e}")

    async def fetch_live_data(self):
        """Fetch current FPDS data from the API."""
        today = date.today().strftime(self.config.date_format)
        date_range = f"[{today}, {today}]"
        request = fpdsRequest(LAST_MOD_DATE=date_range, cli_run=True)
        return await request.data()

    def load_json_files(self) -> list[dict[Any, Any]]:
        """Load FPDS data from JSON files in the configured directory."""
        data: list[dict[Any, Any]] = []
        if not self.data_directory.is_dir():
            raise FileNotFoundError(f"Directory '{self.data_directory}' not found")

        processed_hashes: set[str] = self._read_processed_files()
        json_files = sorted(self.data_directory.glob("*.json"))

        if not json_files:
            raise FileNotFoundError(f"No JSON files found in '{self.data_directory}'")

        files_to_process = [
            (file_path, file_hash)
            for file_path in json_files
            if (file_hash := self._calculate_file_hash(file_path))
            not in processed_hashes
        ]

        if not files_to_process:
            print("No new JSON files based on content hashes.")
            return data

        newly_processed_hashes: set[str] = set()

        for file_path, file_hash in files_to_process:
            file_hash = self._calculate_file_hash(file_path)
            try:
                file_data = json.loads(file_path.read_text(encoding="utf-8"))
                if isinstance(file_data, list):
                    data.extend(file_data)
                elif isinstance(file_data, dict):
                    data.append(file_data)
                else:
                    print(f"Unexpected data format in {file_path}: {type(file_data)}")
                newly_processed_hashes.add(file_hash)
            except json.JSONDecodeError as e:
                print(f"Error decoding {file_path}: {e}")
                continue

        if newly_processed_hashes:
            self._write_processed_files(processed_hashes.union(newly_processed_hashes))

        return data
