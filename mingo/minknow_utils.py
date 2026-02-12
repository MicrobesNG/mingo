
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
            # Use the .protocols field from the response
            response = connection.protocol.list_protocols()
            return [p.identifier for p in response.protocols]
        except Exception as e:
            logger.error(f"Failed to list protocols for {position_name}: {e}")
            return []

    def start_run(self, position_name: str, protocol_id: str, sample_sheet_path: str, run_name: str, settings: Dict = None):
        """
        Start a sequencing run with comprehensive parameters from a settings template.
        """
        from minknow_api.tools import protocols
        
        if settings is None:
            settings = {}
            
        try:
            pos = next(p for p in self.manager.flow_cell_positions() if p.name == position_name)
            connection = pos.connect()
            
            # 1. Basecalling & Barcoding & Alignment
            barcoding_args = None
            if settings.get("barcodingEnabled"):
                barcoding_args = protocols.BarcodingArgs(
                    kits=settings.get("barcodingExpansionKits", []),
                    trim_barcodes=settings.get("trimBarcodesEnabled", False),
                    barcodes_both_ends=settings.get("requireBarcodesBothEnds", False)
                )

            alignment_args = None
            if settings.get("alignmentEnabled") and settings.get("alignmentRefFile"):
                alignment_args = protocols.AlignmentArgs(
                    reference_files=[settings["alignmentRefFile"]],
                    bed_file=settings.get("alignmentBedFile")
                )

            basecalling_args = None
            if settings.get("basecallingEnabled"):
                basecalling_args = protocols.BasecallingArgs(
                    simplex_model=settings.get("basecallModel"),
                    modified_models=settings.get("modifiedBasecallingModels"),
                    stereo_model=None, # Default to None if not specified in template
                    barcoding=barcoding_args,
                    alignment=alignment_args,
                    min_qscore=settings.get("readFilteringMinQscore")
                )

            # 2. Output Arguments
            fastq_args = None
            if settings.get("fastQEnabled"):
                # Use None for reads_per_file and batch_duration to use MinKNOW defaults
                fastq_args = protocols.OutputArgs(None, None)

            pod5_args = None
            if settings.get("pod5Enabled"):
                pod5_args = protocols.OutputArgs(None, None)

            bam_args = None
            if settings.get("bamEnabled"):
                bam_args = protocols.OutputArgs(None, None)

            # 3. Read Until (Adaptive Sampling)
            read_until_args = None
            if settings.get("adaptiveSamplingEnabled"):
                read_until_args = protocols.ReadUntilArgs(
                    filter_type="enrich" if settings.get("shouldEnrichAdaptiveSamplingRef") else "deplete",
                    reference_files=[settings.get("enrichDepleteAdaptiveSamplingRefFile")] if settings.get("enrichDepleteAdaptiveSamplingRefFile") else [],
                    bed_file=settings.get("enrichDepleteAdaptiveSamplingBedFile")
                )

            # 4. Stop Criteria
            duration_hours = settings.get("runLengthHours", 72)
            stop_criteria = protocols.CriteriaValues(
                runtime=int(duration_hours * 3600)
            )

            # 5. Extra Arguments (like sample sheet and experiment ID)
            extra_args = [f"--sample_sheet={sample_sheet_path}"]
            if run_name:
                extra_args.append(f"--experiment_id={run_name}")

            # 6. Simulation Path (needs to be a Path object)
            from pathlib import Path
            sim_path = settings.get("simulatedPlaybackFilePath")
            if sim_path:
                sim_path = Path(sim_path)

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("--- Protocol Parameters ---")
                logger.debug(f"Position: {position_name}")
                logger.debug(f"Protocol ID: {protocol_id}")
                logger.debug(f"Run Name: {run_name}")
                logger.debug(f"Basecalling: {basecalling_args}")
                logger.debug(f"Barcoding: {barcoding_args}")
                logger.debug(f"Alignment: {alignment_args}")
                logger.debug(f"Read Until: {read_until_args}")
                logger.debug(f"Output: FASTQ={fastq_args}, POD5={pod5_args}, BAM={bam_args}")
                logger.debug(f"Duration: {duration_hours}h")
                logger.debug(f"Extra Args: {extra_args}")
                logger.debug(f"Simulation Path: {sim_path}")
                logger.debug("---------------------------")

            logger.info(f"Starting protocol {protocol_id} on {position_name} using standard tools")
            
            run_id = protocols.start_protocol(
                connection,
                identifier=protocol_id,
                sample_id="", # This goes into user_info
                experiment_group=run_name, # This goes into user_info
                barcode_info=None,
                basecalling=basecalling_args,
                read_until=read_until_args,
                fastq_arguments=fastq_args,
                pod5_arguments=pod5_args,
                bam_arguments=bam_args,
                mux_scan_period=settings.get("muxScanPeriod", 1.5),
                stop_criteria=stop_criteria,
                simulation_path=sim_path,
                args=extra_args
            )
            
            return run_id
            
        except Exception as e:
            logger.error(f"Failed to start run on {position_name}: {e}")
            raise

