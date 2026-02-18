
import logging
from typing import List, Dict, Optional, Sequence
from minknow_api.manager import Manager
from minknow_api.protocol_pb2 import BarcodeUserData
from minknow_api.protocol_settings_pb2 import ProtocolSetting


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
                fc_info = None
                
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
                
                # Extract product code
                product_code = "UNKNOWN"
                if fc_info:
                    product_code = fc_info.product_code or fc_info.user_specified_product_code or "UNKNOWN"

                positions.append({
                    "name": pos.name,
                    "status": status,
                    "running": status == "Running",
                    "flow_cell_id": flow_cell_id,
                    "product_code": product_code  
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

    def start_run(self, position_name: str, protocol_id: str, sample_sheet_path: str, run_name: str = None, settings: dict = None, samples: list = None, kit: str = None):
        """
        Start a protocol on a specific position.
        
        Args:
            position_name: Name of the position (e.g., "1A")
            protocol_id: The ID of the protocol to run
            sample_sheet_path: Path to the sample sheet CSV
            run_name: Optional name for the run
            settings: Dictionary of run settings/parameters
            samples: List of sample dictionaries from SLIMS
            kit: Explicitly selected barcoding kit (e.g. SQK-RBK114-96)
        """
        from minknow_api.tools import protocols
        
        if not settings:
            settings = {}
            
        logging.info(f"Connecting to position {position_name}...")
        
        if settings is None:
            settings = {}
            
        barcode_user_info = []
        if samples:
            logger.info(f"Mapping {len(samples)} samples to barcode info")
            for sample in samples:
                user_data = BarcodeUserData()
                user_data.alias = sample.get('cntn_id', '')
                user_data.type = BarcodeUserData.SampleType.test_sample
                
                barcode_i7 = sample.get('barcode_i7', '')
                if barcode_i7:
                    # Map NB01/BC01 to barcode01 etc.
                    if barcode_i7[:2] in ['NB', 'BC']:
                        try:
                            idx = int(barcode_i7[2:])
                            user_data.barcode_name = f"barcode{idx:02d}"
                        except ValueError:
                            user_data.barcode_name = barcode_i7
                    else:
                        user_data.barcode_name = barcode_i7
                
                barcode_user_info.append(user_data)
                
        try:
            pos = next(p for p in self.manager.flow_cell_positions() if p.name == position_name)
            connection = pos.connect()
            
            # 1. Basecalling & Barcoding & Alignment
            barcoding_args = None
            if settings.get("barcodingEnabled"):
                # Extract kit from samples if available, otherwise fallback to settings
                barcoding_kits = settings.get("barcodingExpansionKits", [])
                
                # Priority 1: Explicitly selected kit (from CLI)
                if kit:
                     logger.info(f"Using explicitly selected barcoding kit: {kit}")
                     barcoding_kits = [kit]
                else:
                    # Priority 2: Extract from samples (SLIMS data)
                    if samples and len(samples) > 0:
                        # In SLIMS, the kit is often in 'kit' or 'cntn_cf_kit'
                        # We'll check the first sample as all should be the same for a run position
                        first_sample = samples[0]
                        if logger.isEnabledFor(logging.DEBUG):
                            pass # Kept structure if needed, or just remove.
                            # logger.debug(f"DEBUG: First sample keys: ...") 
                        
                        # Attempt to find kit in likely fields
                        sample_kit = first_sample.get('kit') or first_sample.get('cntn_cf_kit')
                        if sample_kit:
                            logger.info(f"Using barcoding kit from sample sheet: {sample_kit}")
                            barcoding_kits = [sample_kit]
                    
                    # Priority 3: Fallback to template tags
                    if not barcoding_kits:
                         script_kit = settings.get("script", {}).get("tags", {}).get("kit")
                         if script_kit:
                             logger.info(f"Using barcoding kit from template script tags: {script_kit}")
                             barcoding_kits = [script_kit]

                barcoding_args = protocols.BarcodingArgs(
                    kits=barcoding_kits,
                    trim_barcodes=settings.get("trimBarcodesEnabled", False),
                    barcodes_both_ends=settings.get("requireBarcodesBothEnds", False),
                    ignore_unspecified_barcodes=True
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
                    stereo_model=None, # Required positional argument
                    barcoding=barcoding_args,
                    alignment=alignment_args,
                    min_qscore=settings.get("readFilteringMinQscore"),
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

            # 4. Stop Criteria
            duration_hours = settings.get("runLengthHours", 72)
            stop_criteria = protocols.CriteriaValues(
                runtime=int(duration_hours * 3600)
            )

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
                logger.debug(f"Read Until: None")
                logger.debug(f"Output: FASTQ={fastq_args}, POD5={pod5_args}, BAM={bam_args}")
                logger.debug(f"Duration: {duration_hours}h")
                logger.debug(f"Extra Args: None")
                logger.debug(f"Simulation Path: {sim_path}")
                if barcode_user_info:
                    logger.debug(f"Barcode Info Map ({len(barcode_user_info)} barcodes):")
                    for b in barcode_user_info:
                        logger.debug(f"  - {b.alias} -> {b.barcode_name}")
                logger.debug("---------------------------")

            logger.info(f"Starting protocol {protocol_id} on {position_name} using standard tools")
            
            run_id = protocols.start_protocol(
                connection,
                identifier=protocol_id,
                sample_id="",
                experiment_group=run_name,
                barcode_info=barcode_user_info,
                basecalling=basecalling_args,
                fastq_arguments=fastq_args,
                pod5_arguments=pod5_args,
                fast5_arguments=None,
                bam_arguments=None,
                disable_active_channel_selection=False,
                stop_criteria=stop_criteria,
                simulation_path=sim_path,
                offload_location_info=None,
                args=["--split_files_by_barcode=on",
                    "--split_pod5_files_by_barcode=on",
                    "--generate_bulk_file=off",
                    "--fastq_batch_duration=3600",
                    "--pore_reserve=off",
                    "--poly_a_tail_length_estimation=off",
                    ]
            )
            
            return run_id
            
        except Exception as e:
            logger.error(f"Failed to start run on {position_name}: {e}")
            raise


def to_protocol_setting_value(value):
    """Converts a Python value to a ProtocolSettingValue message.

    Args:
        value (bool, int, float, str): The value to convert.

    Returns:
        minknow_api.protocol_settings_pb2.ProtocolSetting.ProtocolSettingValue: The converted value.
    """
    val = ProtocolSetting.ProtocolSettingValue()
    if isinstance(value, bool):
        val.bool_value = value
    elif isinstance(value, int):
        val.integer_value = value
    elif isinstance(value, float):
        val.float_value = value
    elif isinstance(value, str):
        val.string_value = value
    else:
        raise ValueError(f"Unsupported protocol setting value type: {type(value)}")
    return val


def to_protocol_settings(settings_dict):
    """Converts a dictionary of settings to a map of ProtocolSettingValue messages.

    Args:
        settings_dict (dict): A dictionary where keys are setting identifiers and values are
            Python values (bool, int, float, str).

    Returns:
        dict: A dictionary suitable for use in BeginProtocolRequest.settings.
    """
    return {key: to_protocol_setting_value(value) for key, value in settings_dict.items()}
