
import unittest
from unittest.mock import MagicMock, patch
from mingo.slims import SlimsClient

class TestSlimsClient(unittest.TestCase):
    def setUp(self):
        self.client = SlimsClient("http://slims.example.com", "user", "pass")

    @patch('mingo.slims.requests.get')
    def test_fetch_queued_runs(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "entities": [{"pk": 1, "xprn_status": "Ready", "xprn_name": "TestRun"}]
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        runs = self.client.fetch_queued_runs()
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]['xprn_name'], "TestRun")
        
    @patch('mingo.slims.requests.get')
    def test_fetch_run_details(self, mock_get):
        # Mocking multiple calls: one for run info, one for inputs
        mock_run_response = MagicMock()
        mock_run_response.json.return_value = {"pk": 1, "xprn_name": "TestRun"}
        
        mock_inputs_response = MagicMock()
        mock_inputs_response.json.return_value = {
            "entities": [{"pk": 101, "cntn_id": "Sample1"}]
        }
        
        mock_get.side_effect = [mock_run_response, mock_inputs_response]

        details = self.client.fetch_run_details(1)
        self.assertEqual(details['run']['xprn_name'], "TestRun")
        self.assertEqual(len(details['inputs']), 1)
        self.assertEqual(details['inputs'][0]['cntn_id'], "Sample1")

if __name__ == '__main__':
    unittest.main()
