from openquake.commonlib.datastore import read

from reia.config.settings import get_settings
from reia.io.results import (extract_risk_from_datastore,
                             prepare_risk_data_for_storage)
from reia.repositories.asset import AggregationTagRepository
from reia.repositories.lossvalue import RiskValueRepository
from reia.repositories.types import SessionType
from reia.schemas.calculation_schemas import CalculationBranch
from reia.schemas.enums import ERiskType
from reia.services.logger import LoggerService
from reia.services.oq_api import OQCalculationAPI


class ResultsService:
    """Service for handling OpenQuake calculation results."""

    def __init__(self,
                 session: SessionType,
                 api_client: OQCalculationAPI | None = None,
                 dstore_path: str | None = None):
        self.logger = LoggerService.get_logger(__name__)
        self.session = session
        self.config = get_settings()
        self.api_client = api_client
        self.dstore_path = dstore_path

    def save_calculation_results(
            self,
            calculationbranch: CalculationBranch) -> None:
        """Save OpenQuake calculation results to database.

        Args:
            calculationbranch: The calculation branch object

        Raises:
            Exception: If result retrieval or saving fails
        """
        # Get calculation data using OQCalculationAPI
        self.logger.info("Retrieving results for calculation "
                         f"branch {calculationbranch.oid}")

        if self.api_client is not None:
            dstore = self.api_client.get_result()
        elif self.dstore_path is not None:
            dstore = read(self.dstore_path)
        else:
            raise ValueError("No API client or datastore path provided")

        oq_parameter_inputs = dstore['oqparam']

        # Flatten and deduplicate types
        all_types = {
            it for sub in oq_parameter_inputs.aggregate_by for it in sub}

        # Bulk fetch aggregation tags once per exposuremodel
        aggregation_tags_list = AggregationTagRepository.get_by_exposuremodel(
            self.session, calculationbranch.exposuremodel_oid,
            types=list(all_types))
        aggregation_tag_by_name = {
            tag.name: tag for tag in aggregation_tags_list}

        risk_type = ERiskType(oq_parameter_inputs.calculation_mode)

        # Extract and prepare risk values using IO functions
        self.logger.debug(f"Extracting {risk_type.name} risk "
                          "values from OpenQuake datastore")
        raw_risk_values = extract_risk_from_datastore(dstore, risk_type)
        self.logger.debug(
            f"Retrieved {len(raw_risk_values)} risk value records")
        risk_values, df_agg_val = prepare_risk_data_for_storage(
            raw_risk_values, calculationbranch, risk_type,
            aggregation_tag_by_name)

        # Save to database
        self.logger.debug(
            f"Saving {len(risk_values)} risk values and "
            f"{len(df_agg_val)} aggregation mappings to database")

        RiskValueRepository.insert_many(
            self.session, risk_values, df_agg_val)

        self.logger.info("Successfully saved results for "
                         f"calculation branch {calculationbranch.oid}")
