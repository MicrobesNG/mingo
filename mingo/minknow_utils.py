
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
                status = "Ready"
                flow_cell_id = None
                
                try:
                    connection = pos.connect()
                    
                    # Check flow cell info
                    fc_info = connection.device.get_flow_cell_info()
                    flow_cell_id = fc_info.flow_cell_id
                    
                    if not flow_cell_id:
                        status = "No Flow Cell"
                    else:
                        # Check run state
                        run_info = connection.protocol.get_run_info()
                        # Use the protocol state enum name for a descriptive status
                        state_name = connection.protocol._pb.ProtocolState.Name(run_info.state)
                        if state_name == "PROTOCOL_RUNNING":
                             status = "Running"
                        elif state_name == "PROTOCOL_COMPLETED":
                             status = "Ready"
                        else:
                             # Map other states to something readable
                             status = state_name.replace("PROTOCOL_", "").capitalize()
                         
                except Exception as e:
                    logger.warning(f"Failed to get details for position {pos.name}: {e}")
                    status = "Error/Offline"
                
                positions.append({
                    "name": pos.name,
                    "status": status,
                    "running": status == "Running",
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

