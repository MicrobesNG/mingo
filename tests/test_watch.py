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

if __name__ == '__main__':
    unittest.main()
