import json
from pathlib import Path
import asyncio
from typing import Any
import pandas as pd
from datetime import date
from fpds import fpdsRequest
from config import Config

class DataLaoder:
    """Handles all data loading operations for contract data."""
    def __init__(self, config: Config):
        self.config = config
        
    async def fetch_live_data(self):
        """Fetch current FPDS data from the API."""
        # TODO: add config setting for setting the dates
        today = date.today().strftime(self.config.date_format)
        date_range = f"[{today}, {today}]"
        request = fpdsRequest(LAST_MOD_DATE=date_range, cli_run=True)
        return await request.data()
    
    def load_json_files(self):
        """Load FPDS data from JSON files in the configured directory."""
        data = []
        data_path = self.config.data_directory
        
        if not data_path.is_dir():
            raise FileNotFoundError(f"Directory '{data_path}' not found")
        
        json_files = sorted(data_path.glob("*.json"))
        if not json_files:
            raise FileNotFoundError(f"No JSON files found in '{data_path}'")
        
        for file_path in json_files:
            try:
                with open(file_path, "r", encoding="utf-8") as file:
                    file_data = json.load(file)
                    if isinstance(file_data, list):
                        data.extend(file_data)
                    elif isinstance(file_data, dict):
                        data.append(file_data)
            except json.JSONDecodeError as e:
                print(f"Error decoding {file_path}: {e}")
                continue
        
        return data