
import os
import requests
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class SlimsClient:
    def __init__(self, url: str, user: str, password: str):
        self.url = url.rstrip('/')
        self.auth = (user, password)
        self.headers = {'Content-Type': 'application/json'}

    def _get(self, endpoint: str, **kwargs) -> Dict:
        full_url = f"{self.url}/rest/{endpoint}"
        logger.debug(f"GET {full_url} with kwargs {kwargs}")
        response = requests.get(full_url, auth=self.auth, headers=self.headers, **kwargs)
        response.raise_for_status()
        return response.json()

    def _post(self, endpoint: str, data: Dict) -> Dict:
        full_url = f"{self.url}/rest/{endpoint}"
        logger.debug(f"POST {full_url} with data {data}")
        response = requests.post(full_url, auth=self.auth, headers=self.headers, json=data)
        response.raise_for_status()
        return response.json()

    def fetch_queued_runs(self) -> List[Dict]:
        """
        Fetch runs that are ready to start.
        Assuming 'ExperimentRun' table and a status field.
        Adjust criteria based on actual SLIMS configuration.
        """
        # This is a best-guess criteria based on typical SLIMS setups.
        # We might need to adjust the field names (e.g., 'exp_status', 'exp_name').
        criteria = {
            "criteria": {
                "fieldName": "xprn_status", 
                "operator": "equals", 
                "value": "Ready" 
            }
        }
        # In a real scenario, we might need to search by a different status or table.
        # For the prototype, we'll try to fetch from 'ExperimentRun'.
        try:
            response = self._get("ExperimentRun/advanced", json=criteria)
            return response.get('entities', [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch runs: {e}")
            return []

    def fetch_run_details(self, run_pk: int) -> Dict:
        """
        Fetch details for a specific run, including linked content (samples).
        """
        # Fetch the run record itself
        run_record = self._get(f"ExperimentRun/{run_pk}")
        
        # Fetch linked content (input samples)
        # Usage 'input' is typical for samples going into a run.
        inputs = self._get(f"eln/content/input/{run_pk}")
        
        return {
            "run": run_record,
            "inputs": inputs.get('entities', [])
        }

    def fetch_content_by_pk(self, pks: List[str]) -> List[Dict]:
        """
        Fetch content records by primary keys.
        """
        if not pks:
            return []
        pks_str = ",".join(map(str, pks))
        response = self._get(f"Content/{pks_str}")
        return response.get('entities', [])
