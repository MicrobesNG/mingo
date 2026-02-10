
import unittest
import csv
import io
from mingo.samplesheet import SampleSheetGenerator

class TestSampleSheetGenerator(unittest.TestCase):
    def setUp(self):
        self.generator = SampleSheetGenerator()

    def test_generate(self):
        run_metadata = {"xprn_name": "TEST_RUN_01"}
        samples = [
            {
                "cntn_id": "SAMPLE_01",
                "cntn_cf_barcode": "barcode01",
                "cntn_cf_fk_barcode_i7": "NB01",
                "cntn_cf_taxon": "E. coli",
                "cntn_cf_isUrgent": True
            }
        ]
        
        csv_output = self.generator.generate(
            run_metadata, samples, "FLOWCELL_123", "P1", "KIT_123"
        )
        
        # Parse back specifically checking headers and content
        reader = csv.DictReader(io.StringIO(csv_output))
        rows = list(reader)
        
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row['experiment_id'], "TEST_RUN_01")
        self.assertEqual(row['flow_cell_id'], "FLOWCELL_123")
        self.assertEqual(row['alias'], "SAMPLE_01")
        self.assertEqual(row['cntn_cf_isUrgent'], "true")
        self.assertEqual(row['cntn_cf_taxon'], "E. coli")

if __name__ == '__main__':
    unittest.main()
