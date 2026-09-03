import os
import unittest
from unittest.mock import MagicMock, patch
from mingo.watch import MinKNOWWatcher, resolve_display_hostname, is_remote_host
from minknow_api import protocol_pb2

class TestWatch(unittest.TestCase):
    def test_hostname_resolution(self):
        with patch('socket.gethostname', return_value='sequencer1.local'):
            self.assertEqual(resolve_display_hostname('localhost'), 'sequencer1')
            self.assertEqual(resolve_display_hostname('127.0.0.1'), 'sequencer1')
            self.assertEqual(resolve_display_hostname(None), 'sequencer1')
        
        self.assertEqual(resolve_display_hostname('pancake.lan'), 'pancake')
        self.assertEqual(resolve_display_hostname('pancake'), 'pancake')
        self.assertEqual(resolve_display_hostname('192.168.1.111'), '192.168.1.111')

    def test_is_remote_host(self):
        with patch('socket.gethostname', return_value='gridion-1'):
            with patch('socket.getfqdn', return_value='gridion-1.local'):
                self.assertFalse(is_remote_host('localhost'))
                self.assertFalse(is_remote_host('127.0.0.1'))
                self.assertFalse(is_remote_host('gridion-1'))
                self.assertTrue(is_remote_host('192.168.1.200'))

    @patch('mingo.watch.Manager')
    def test_mid_run_attachment(self, mock_manager):
        watcher = MinKNOWWatcher(host="sequencer-a", level="normal")
        watcher.send_slack_notification = MagicMock()
        watcher._start_directory_watcher = MagicMock()
        watcher._start_coverage_watcher = MagicMock()

        # Mock position and connection
        mock_pos = MagicMock()
        mock_pos.name = "1A"
        mock_conn = MagicMock()
        mock_pos.connect.return_value.__enter__.return_value = mock_conn

        # Mock message representing an in-progress run on connect
        mock_msg = MagicMock()
        mock_msg.run_id = "run_123"
        mock_msg.state = protocol_pb2.PROTOCOL_RUNNING
        def mock_stream():
            yield mock_msg
            watcher._stop_event.set()

        mock_conn.protocol.watch_current_protocol_run.side_effect = mock_stream

        # Mock run_info
        mock_run_info = MagicMock()
        mock_run_info.user_info.protocol_group_id.value = "NSR_TEST_GROUP"
        mock_run_info.protocol_id = "sequencing/5000bp"
        mock_run_info.output_path = "/data/output"
        mock_conn.protocol.get_run_info.return_value = mock_run_info

        # Run watch position (which runs one loop over the stream)
        watcher._watch_position(mock_pos)

        # Assert mid-run attachment started directory & coverage watchers
        watcher._start_directory_watcher.assert_called_once_with("1A", "/data/output")
        watcher._start_coverage_watcher.assert_called_once_with("1A", "/data/output")

        # Assert notification sent with 'attached' phase and hostname included
        watcher.send_slack_notification.assert_called_once()
        phase, msg = watcher.send_slack_notification.call_args[0]
        self.assertEqual(phase, "attached")
        self.assertIn("sequencer-a", msg)
        self.assertIn("1A", msg)
        self.assertIn("attached to in-progress run", msg)

    @patch('mingo.watch.Manager')
    @patch('mingo.watch.os.path.exists', return_value=False)
    def test_remote_path_warning_skips_watcher(self, mock_exists, mock_manager):
        watcher = MinKNOWWatcher(host="192.168.1.200", level="normal")
        with patch('mingo.watch.logger.warning') as mock_warn:
            watcher._start_directory_watcher("1A", "/data/runs/test")
            mock_warn.assert_called_once()
            self.assertIn("remote", mock_warn.call_args[0][0])
            self.assertNotIn("1A", watcher.file_observers)

    @patch('mingo.watch.Manager')
    def test_build_slack_blocks(self, mock_manager):
        watcher = MinKNOWWatcher(host="pancake", level="normal")
        
        # Test Run Started Blocks
        color, blocks = watcher._build_slack_blocks(
            "starting",
            "*NSR_01* (`pancake` | `1A` | seq_proto) started.",
            experiment_id="NSR_01",
            pos_name="1A",
            protocol_id="seq_proto"
        )
        self.assertEqual(color, "#2EB67D")
        self.assertEqual(blocks[0]["type"], "header")
        self.assertIn("Started", blocks[0]["text"]["text"])
        self.assertEqual(blocks[1]["type"], "section")
        field_texts = [f["text"] for f in blocks[1]["fields"]]
        self.assertTrue(any("NSR_01" in t for t in field_texts))
        self.assertTrue(any("pancake" in t for t in field_texts))

        # Test Error Blocks
        color_err, blocks_err = watcher._build_slack_blocks(
            "error",
            "Run error",
            experiment_id="NSR_01",
            pos_name="1A",
            error_detail="Flow cell disconnected"
        )
        self.assertEqual(color_err, "#E01E5A")
        self.assertIn("Error", blocks_err[0]["text"]["text"])
        field_texts_err = [f["text"] for f in blocks_err[1]["fields"]]
        self.assertTrue(any("Flow cell disconnected" in t for t in field_texts_err))

    def test_make_progress_bar(self):
        from mingo.watch import make_progress_bar
        bar_50 = make_progress_bar(27.5, 55.0, length=10)
        self.assertIn("50.0%", bar_50)
        self.assertIn("▰▰▰▰▰▱▱▱▱▱", bar_50)

        bar_100 = make_progress_bar(55.0, 55.0, length=10)
        self.assertIn("100.0%", bar_100)
        self.assertIn("▰▰▰▰▰▰▰▰▰▰", bar_100)

    def test_resolve_run_dirs_and_coverage_inputs(self):
        from mingo.coverage import resolve_run_dirs, find_coverage_inputs, run_coverage_analysis
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        exp_dir = os.path.join(project_root, 'NSR_test')
        
        # Test resolving from experiment_dir
        run_dirs = resolve_run_dirs(exp_dir)
        self.assertEqual(len(run_dirs), 1)
        leaf_dir = run_dirs[0]
        self.assertTrue(leaf_dir.endswith("20260220_1403_P2S-01064-B_PBI35250_84b0eb00"))

        # Test resolving from leaf run_dir
        self.assertEqual(resolve_run_dirs(leaf_dir), [leaf_dir])

        # Test discovery and analysis
        csv_path, summary_path, json_path = find_coverage_inputs(leaf_dir)
        self.assertTrue(os.path.exists(csv_path))
        results = run_coverage_analysis(csv_path, json_path=json_path, quiet=True)
        self.assertEqual(len(results), 63)
        self.assertIn('coverage_float', results[0])

if __name__ == '__main__':
    unittest.main()
