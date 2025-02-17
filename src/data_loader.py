import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any

from fpds import fpdsRequest

from config import Config


class DataLoader:
    """Handles all data loading operations for contract data."""

    def __init__(self, config: Config):
        self.config = config
        self.data_directory = self.get_relative_path(self.config.data_directory)
        self.processed_files_path = (
            self.data_directory / "processed_hashes.txt"
        )

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculates the SHA256 hash of a file."""
        hasher = hashlib.sha256()

        with file_path.open("rb") as file:
            while True:
                chunk = file.read(4096)
                if not chunk:
                    break
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
        current_file_path = Path(__file__).resolve()
        current_dir = current_file_path.parent
        target_path = current_dir / relative_path
        return target_path.resolve()

    def _read_processed_files(self) -> set:
        """Reads the set of processed file hashes from a file."""
        if self.processed_files_path.exists():
            try:
                with self.processed_files_path.open("r") as file:
                    return {line.strip() for line in file}
            except Exception as e:
                print(f"Error reading processed files: {e}")
                return set()
        return set()

    def _write_processed_files(self, processed_hashes: set):
        """Write the set of a processed file hashes to a file."""
        try:
            with self.processed_files_path.open("w") as file:
                file.write("\n".join(sorted(processed_hashes)))
        except Exception as e:
            print(f"Error writing processed files: {e}")

    async def fetch_live_data(self):
        """Fetch current FPDS data from the API."""
        # TODO: add config setting for setting the dates
        today = date.today().strftime(self.config.date_format)
        date_range = f"[{today}, {today}]"
        request = fpdsRequest(LAST_MOD_DATE=date_range, cli_run=True)
        return await request.data()

    def load_json_files(self) -> list[dict[Any, Any]]:
        """Load FPDS data from JSON files in the configured directory."""
        data: list[dict[Any, Any]] = []
        data_path = self.data_directory

        if not data_path.is_dir():
            raise FileNotFoundError(f"Directory '{data_path}' not found")

        processed_hashes = self._read_processed_files()
        json_files = sorted(data_path.glob("*.json"))
        newly_processed_hashes: set[str] = set()

        if not json_files:
            raise FileNotFoundError(f"No JSON files found in '{data_path}'")

        files_to_process = []

        for file_path in json_files:
            file_hash = self._calculate_file_hash(file_path)

            if file_hash not in processed_hashes:
                files_to_process.append(file_path)

        if not files_to_process:
            print("No new JSON files to process based on content hashes.")
            return data

        for file_path in files_to_process:
            file_hash = self._calculate_file_hash(file_path)
            try:
                with open(file_path, "r", encoding="utf-8") as file:
                    file_data = json.load(file)
                    if isinstance(file_data, list):
                        data.extend(file_data)
                    elif isinstance(file_data, dict):
                        data.append(file_data)
                    newly_processed_hashes.add(file_hash)
            except json.JSONDecodeError as e:
                print(f"Error decoding {file_path}: {e}")
                continue

        if newly_processed_hashes:
            update_processed_hashes = processed_hashes.union(newly_processed_hashes)
            self._write_processed_files(update_processed_hashes)

        return data
