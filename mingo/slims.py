
import os
import requests
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class SlimsClient:
    def __init__(self, url: str, user: str, password: str):
        self.url = url.rstrip('/')
        if self.url.endswith('/rest'):
            self.url = self.url[:-5]
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

    def _flatten_entity(self, entity: Dict) -> Dict:
        """
        Convert SLIMS entity with 'columns' list into a flat dictionary.
        Includes display values for foreign keys with a '_display' suffix.
        """
        flat = {'pk': entity.get('pk')}
        for col in entity.get('columns', []):
            name = col['name']
            flat[name] = col.get('value')
            if 'displayValue' in col and col['displayValue'] is not None:
                flat[f"{name}_display"] = col['displayValue']
        return flat

    def fetch_queued_runs(self) -> List[Dict]:
        """
        Fetch runs that are ready to start and belong to the 'ONT Sequencing' protocol.
        """
        # 1. Find the "ONT Sequencing" templates
        template_criteria = {
            "criteria": {
                "fieldName": "xptm_name",
                "operator": "equals",
                "value": "ONT Sequencing"
            }
        }
        try:
            templates = self._get("ExperimentTemplate/advanced", json=template_criteria).get('entities', [])
            template_pks = [t.get('pk') for t in templates]
            
            if not template_pks:
                logger.warning("No 'ONT Sequencing' templates found.")
                return []

            # 2. Fetch runs for these templates that are 'Ready'
            template_criteria = []
            # 2. Find runs that are not completed/cancelled for these templates
            # Note: We use the 'or' structure for multiple templates to be safe
            template_filters = [{"fieldName": "xprn_fk_experimentTemplate", "operator": "equals", "value": tpk} for tpk in template_pks]
            
            run_criteria = {
                "operator": "and",
                "criteria": [
                    {"fieldName": "xprn_completed", "operator": "equals", "value": False},
                    {"fieldName": "xprn_cancelled", "operator": "equals", "value": False},
                    {"operator": "or", "criteria": template_filters}
                ]
            }
            
            runs_resp = self._get("ExperimentRun/advanced", json={"criteria": run_criteria})
            all_potential_runs = [self._flatten_entity(e) for e in runs_resp.get('entities', [])]
            
            if not all_potential_runs:
                return []
                
            # 3. Filter for runs that have at least one non-DONE step
            # We can do this by fetching all steps for these runs and checking statuses
            # For efficiency in a real environment, we'd batch this.
            queued_runs = []
            for run in all_potential_runs:
                step_criteria = {
                    "fieldName": "xprs_fk_experimentRun",
                    "operator": "equals",
                    "value": run['pk']
                }
                steps_resp = self._get("ExperimentRunStep/advanced", json={"criteria": step_criteria})
                steps = [self._flatten_entity(s) for s in steps_resp.get('entities', [])]
                
                # If there are no steps, it's probably new/queued
                # If there are steps, at least one must be NOT DONE
                if not steps or any(s.get('xprs_status') != 'DONE' for s in steps):
                    queued_runs.append(run)
            
            # Sort by name/create date (newest first)
            queued_runs.sort(key=lambda x: x.get('xprn_createdOn', 0), reverse=True)
            return queued_runs
            
        except Exception as e:
            print(f"Error fetching runs from SLIMS: {e}")
            return []

    def _trace_ingredients(self, content_pk: int, depth: int = 0, metadata: Dict = None) -> List[Dict]:
        """
        Recursively trace ingredients of a content item via ContentRelation.
        Stops at 'DNA' or 'Pure strain' or 'Strain aliquot' types or after max depth.
        Metadata from intermediate steps (like barcodes from DNA Library) is passed down.
        """
        if metadata is None:
            metadata = {}
            
        if depth > 5: # Safety break
            return []
            
        try:
            content_resp = self._get(f"Content/{content_pk}")
            entities = content_resp.get('entities', [])
            if not entities:
                return []
            content = self._flatten_entity(entities[0])
        except Exception:
            return []

        cntp = content.get('cntp_name')
        
        # If this is a DNA Library, it might have barcode info
        if cntp == 'DNA Library':
            # Capture barcode info to pass down
            if content.get('cntn_cf_fk_barcode_i7_display'):
                metadata['barcode_i7'] = content['cntn_cf_fk_barcode_i7_display']
            if content.get('cntn_cf_barcodeAdapterSet'):
                metadata['barcode_adapter'] = content['cntn_cf_barcodeAdapterSet']

        # Target types that we consider "Original Content"
        if cntp in ['DNA', 'Pure strain', 'Strain aliquot', 'DNA samples']:
            # Merge collected metadata into the original content record
            content.update(metadata)
            return [content]
            
        # Otherwise, find relations (ingredients)
        criteria = {
            "criteria": {
                "fieldName": "corl_fk_to",
                "operator": "equals",
                "value": content_pk
            }
        }
        try:
            rels_resp = self._get("ContentRelation/advanced", json=criteria)
            rels = rels_resp.get('entities', [])
            
            ingredients = []
            for r_entity in rels:
                r = self._flatten_entity(r_entity)
                from_pk = r.get('corl_fk_from')
                if from_pk:
                    # Pass a copy of metadata to avoid cross-contamination
                    ingredients.extend(self._trace_ingredients(from_pk, depth + 1, metadata.copy()))
            
            # If it's a library but no ingredients found, just return the library itself as fallback
            if not ingredients and cntp in ['DNA Library', 'Library pool']:
                content.update(metadata)
                return [content]
            
            return ingredients
        except Exception:
            content.update(metadata)
            return [content]

    def fetch_run_details(self, run_pk: int) -> Dict:
        """
        Fetch details for a specific run, including linked original content (samples).
        """
        # Fetch the run record itself
        run_record = self._flatten_entity(self._get(f"ExperimentRun/{run_pk}").get('entities', [])[0])
        
        # 1. Find the steps for this run
        step_criteria = {
            "criteria": {
                "fieldName": "xprs_fk_experimentRun",
                "operator": "equals",
                "value": run_pk
            }
        }
        
        original_samples = []
        seen_pks = set()
        
        try:
            steps_resp = self._get("ExperimentRunStep/advanced", json=step_criteria)
            steps = steps_resp.get('entities', [])
            
            for step in steps:
                step_pk = step.get('pk')
                # 2. Fetch linked content (input samples) for each step
                inputs_resp = self._get(f"eln/content/input/{step_pk}")
                for input_entity in inputs_resp.get('entities', []):
                    input_flat = self._flatten_entity(input_entity)
                    # 3. Recursively trace ingredients for each input
                    ingredients = self._trace_ingredients(input_flat['pk'])
                    for ing in ingredients:
                        if ing['pk'] not in seen_pks:
                            original_samples.append(ing)
                            seen_pks.add(ing['pk'])
                    
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch steps/samples for run {run_pk}: {e}")
        
        return {
            "run": run_record,
            "inputs": original_samples
        }

    def fetch_content_by_pk(self, pks: List[int]) -> List[Dict]:
        """
        Fetch content records by primary keys.
        """
        if not pks:
            return []
        pks_str = ",".join(map(str, pks))
        response = self._get(f"Content/{pks_str}")
        return [self._flatten_entity(e) for e in response.get('entities', [])]
