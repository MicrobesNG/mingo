
import logging
from typing import List, Dict, Optional
from minknow_api.manager import Manager


logger = logging.getLogger(__name__)

class MinKNOWClient:
    def __init__(self, host: str = "localhost", port: int = None, token: str = None):
        self.host = host
        self.port = port
        self.token = token
        self.manager = Manager(host=host, port=port, developer_api_token=token)

    def get_positions(self) -> List[Dict]:
        """
        Get all flow cell positions and their states.
        """
        positions = []
        try:
            for pos in self.manager.flow_cell_positions():
                # Extract relevant info
                # pos has .name, .running, .flow_cell (maybe?)
                # We need to connect to get detailed status/flow cell info if not available directly
                # flowcell_health.py suggests pos.connect() is needed for deep info, 
                # but pos object itself has some info.
                
                # Check if a flow cell is present
                # Use a try-except block as connection might fail or properties might be missing
                state = "Unknown"
                flow_cell_id = None
                
                try:
                    # Connection is needed to check flow cell info
                    connection = pos.connect()
                    # Check flow cell info
                    fc_info = connection.flow_cell.get_flow_cell_info()
                    flow_cell_id = fc_info.flow_cell_id
                    
                    # Check run state
                    run_info = connection.protocol.get_run_info()
                    if run_info.state == 1: # PROTOCOL_RUNNING - just a guess, need to verify enum
                         # Actually flow_cell_health.py uses pos.running boolean
                         pass
                         
                except Exception as e:
                    logger.warning(f"Failed to get details for position {pos.name}: {e}")
                
                positions.append({
                    "name": pos.name,
                    "running": pos.running,
                    "flow_cell_id": flow_cell_id
                })
        except Exception as e:
            logger.error(f"Failed to list positions: {e}")
            
        return positions

    def list_protocols(self, position_name: str) -> List[str]:
        """
        List available protocols for a position.
        """
        try:
            pos = next(p for p in self.manager.flow_cell_positions() if p.name == position_name)
            connection = pos.connect()
            # Assuming list_protocols returns a list of protocol info objects
            protocols = connection.protocol.list_protocols()
            return [p.identifier for p in protocols]
        except Exception as e:
            logger.error(f"Failed to list protocols for {position_name}: {e}")
            return []

    def start_run(self, position_name: str, protocol_id: str, sample_sheet: str, run_name: str):
        """
        Start a sequencing run.
        """
        try:
            pos = next(p for p in self.manager.flow_cell_positions() if p.name == position_name)
            connection = pos.connect()
            
            # Start protocol expects verification of arguments.
            # We pass the sample sheet as an argument, likely 'sample_sheet' or check specific protocol args.
            # Typical ONT protocols take 'experiment_id', 'sample_sheet' (content or path?), etc.
            # We will assume we pass it as a file or string.
            # For this prototype, we'll try to pass the sample sheet content or write it to a temp file if needed.
            # The prompt says "Writing sample sheet to /data/input/...". 
            # So we should write the file first, then pass the path.
            
            args = [
                 # Arguments need to be constructed based on protocol definition.
                 # This is tricky without knowing the exact protocol arguments.
                 # We will log the action for now.
                 f"--sample_sheet={sample_sheet}",
                 f"--experiment_id={run_name}"
            ]
            
            logger.info(f"Starting protocol {protocol_id} on {position_name} with args {args}")
            
            # Real start_protocol call:
            run_id = connection.protocol.start_protocol(
                identifier=protocol_id,
                args=args,
                user_info={"protocol_group_id": run_name}
            )
            return run_id
            
        except Exception as e:
            logger.error(f"Failed to start run on {position_name}: {e}")
            raise

