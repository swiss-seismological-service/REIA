import logging
from operator import attrgetter

import pandas as pd
from sqlalchemy.orm import Session

from reia.api import OQCalculationAPI
from reia.datamodel import CalculationBranch
from reia.io.dstore import get_risk_from_dstore
from reia.repositories.asset import AggregationTagRepository
from reia.repositories.lossvalue import RiskValueRepository
from reia.schemas.enums import ERiskType
from settings import get_config

LOGGER = logging.getLogger(__name__)


class ResultsService:
    """Service for handling OpenQuake calculation results."""

    def __init__(self, session: Session, api_client: OQCalculationAPI):
        self.session = session
        self.config = get_config()
        self.api_client = api_client

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
        dstore = self.api_client.get_result()
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

        # Retrieve and enrich risk values
        risk_values = get_risk_from_dstore(dstore, risk_type)
        risk_values = risk_values.copy()  # If risk_values is shared elsewhere

        risk_values['weight'] *= calculationbranch.weight
        risk_values['_calculation_oid'] = calculationbranch.calculation_oid
        risk_values['_calculationbranch_oid'] = calculationbranch.oid
        risk_values['_type'] = risk_type.name
        risk_values['losscategory'] = risk_values['losscategory'].map(
            attrgetter('name'))
        risk_values['_oid'] = pd.RangeIndex(start=1, stop=len(risk_values) + 1)

        # Build many-to-many reference table
        df_agg_val = pd.DataFrame({
            'riskvalue': risk_values['_oid'],
            'aggregationtag': risk_values.pop('aggregationtags'),
            '_calculation_oid': risk_values['_calculation_oid'],
            'losscategory': risk_values['losscategory']
        })

        # Explode aggregation tags (list -> rows)
        df_agg_val = df_agg_val.explode('aggregationtag', ignore_index=True)

        # Map to tag object
        tag_objs = df_agg_val['aggregationtag'].map(aggregation_tag_by_name)
        df_agg_val['aggregationtype'] = tag_objs.map(attrgetter('type'))
        df_agg_val['aggregationtag'] = tag_objs.map(attrgetter('oid'))

        # Save to database
        RiskValueRepository.insert_many(
            self.session, risk_values, df_agg_val)

        LOGGER.info(
            f"Saved results for calculation branch {calculationbranch.oid}")
