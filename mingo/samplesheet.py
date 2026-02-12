
import csv
import io
from typing import List, Dict

class SampleSheetGenerator:
    def __init__(self):
        self.headers = [
            "flow_cell_id", "position_id", "sample_id", "experiment_id",
            "flow_cell_product_code", "kit", "alias", "type", "barcode",
            "cntn_cf_fk_barcode_i7", "cntn_id", "cntn_cf_taxon",
            "cntn_cf_genomeSizeMb", "cntn_cf_gcContent", "cntn_cf_orderName",
            "cntn_cf_stockConcentration", "cntn_cf_stockConcentration_unit",
            "cntn_cf_isUrgent", "cntn_cf_lowMaterial"
        ]

    def generate(self, 
                 run_metadata: Dict, 
                 samples: List[Dict], 
                 flow_cell_id: str, 
                 position_id: str, 
                 kit: str, 
                 flow_cell_product_code: str = "FLO-PRO114") -> str:
        """
        Generate a CSV sample sheet string.
        """
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=self.headers)
        writer.writeheader()

        experiment_id = run_metadata.get('xprn_name', 'Unknown_Run')

        for sample in samples:
            # Map SLIMS Content fields to Sample Sheet columns
            # This mapping assumes 'columns' in the entity response or direct fields.
            # SLIMS entities usually have a 'columns' list or key-value pairs depending on the fetch.
            # We'll assume the entity dict represents the record values.
            
            # Helper to safely get value from nested structures if needed, 
            # but usually 'entity' is a flat dict of fields in simplified views, 
            # or we need to extract from 'columns'.
            # Based on slims.py, we expect a dictionary of field_name -> value.
            
            # Note: sample_id is often left blank in the example, alias is populated.
            
            # Extract barcode info collected during tracing
            barcode_i7 = sample.get('barcode_i7', '')
            barcode_name = ''
            if barcode_i7.startswith('NB'):
                try:
                    idx = int(barcode_i7[2:])
                    barcode_name = f"barcode{idx:02d}"
                except ValueError:
                    barcode_name = barcode_i7
            elif barcode_i7.startswith('BC'):
                try:
                    idx = int(barcode_i7[2:])
                    barcode_name = f"barcode{idx:02d}"
                except ValueError:
                    barcode_name = barcode_i7
            else:
                barcode_name = sample.get('cntn_barCode', '')

            row = {
                "flow_cell_id": flow_cell_id,
                "position_id": position_id,
                "sample_id": "", # Intentionally empty as per example
                "experiment_id": experiment_id,
                "flow_cell_product_code": flow_cell_product_code,
                "kit": kit,
                "alias": sample.get('cntn_id', ''), 
                "type": "test_sample", 
                "barcode": barcode_name,
                "cntn_cf_fk_barcode_i7": barcode_i7,
                "cntn_id": sample.get('cntn_id', ''),
                "cntn_cf_taxon": sample.get('cntn_cf_taxon', ''),
                "cntn_cf_genomeSizeMb": sample.get('cntn_cf_genomeSizeMb', ''),
                "cntn_cf_gcContent": sample.get('cntn_cf_gcContent', ''),
                "cntn_cf_orderName": sample.get('cntn_cf_orderName', ''),
                "cntn_cf_stockConcentration": sample.get('cntn_cf_stockConcentration', ''),
                "cntn_cf_stockConcentration_unit": sample.get('cntn_cf_stockConcentration_unit', ''),
                "cntn_cf_isUrgent": str(sample.get('cntn_cf_isUrgent', 'false')).lower(),
                "cntn_cf_lowMaterial": str(sample.get('cntn_cf_lowMaterial', 'false')).lower(),
            }
            writer.writerow(row)

        return output.getvalue()
