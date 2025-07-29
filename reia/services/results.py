import os
from operator import attrgetter

import pandas as pd
from openquake.commonlib.datastore import DataStore
from openquake.risklib.scientific import LOSSTYPE
from sqlalchemy.orm import Session

from reia.api import OQCalculationAPI
from reia.datamodel import CalculationBranch, ELossCategory
from reia.io import RISK_COLUMNS_MAPPING
from reia.repositories.asset import AggregationTagRepository
from reia.repositories.lossvalue import RiskValueRepository
from reia.schemas.enums import ERiskType
from reia.services.logger import LoggerService
from settings import get_config


class ResultsService:
    """Service for handling OpenQuake calculation results."""

    def __init__(self, session: Session, api_client: OQCalculationAPI):
        self.logger = LoggerService.get_logger(__name__)
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
        self.logger.info("Retrieving results for calculation "
                         f"branch {calculationbranch.oid}")
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
        self.logger.debug(f"Extracting {risk_type.name} risk "
                          "values from OpenQuake datastore")
        risk_values = self._get_risk_from_dstore(dstore, risk_type)
        risk_values = risk_values.copy()  # If risk_values is shared elsewhere
        self.logger.debug(f"Retrieved {len(risk_values)} risk value records")

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
        self.logger.debug(
            f"Saving {len(risk_values)} risk values and "
            f"{len(df_agg_val)} aggregation mappings to database")
        RiskValueRepository.insert_many(
            self.session, risk_values, df_agg_val)

        self.logger.info("Successfully saved results for "
                         f"calculation branch {calculationbranch.oid}")

    def _get_risk_from_dstore(self, dstore: DataStore, risk_type: ERiskType):
        """Extract risk data from OpenQuake datastore.

        Args:
            dstore: OpenQuake datastore containing calculation results
            risk_type: Type of risk calculation (LOSS or DAMAGE)

        Returns:
            DataFrame with processed risk values
        """
        all_agg_keys = [d.decode().split(',')
                        for d in dstore['agg_keys'][:]]

        df = dstore.read_df('risk_by_event')  # get risk_by_event

        weights = dstore['weights'][:]
        events = dstore.read_df('events', 'id')[['rlz_id']]

        # risk by event contains more agg_id's than keys which
        # are used to store the total per agg value. Remove them.
        df = df.loc[df['agg_id'] != len(all_agg_keys)]
        cols_mapping = RISK_COLUMNS_MAPPING[risk_type]
        df = df.rename(columns=cols_mapping)[cols_mapping.values()]

        if int(os.getenv('OQ_VERSION', '15')) >= 15:
            loss_types = LOSSTYPE
        else:
            loss_types = dstore['oqparam'].loss_types

        df['losscategory'] = df['losscategory'].map(
            lambda x: ELossCategory[loss_types[x].upper()])

        df['aggregationtags'] = df['aggregationtags'].map(
            all_agg_keys.__getitem__)

        # events have an associated weight which comes from the branch weight
        events['weight'] = events['rlz_id'].map(weights.__getitem__)

        df['weight'] = df['eventid'].map(events['weight']) / \
            dstore['oqparam'].number_of_ground_motion_fields

        if risk_type == ERiskType.DAMAGE:
            df = df[(df['dg1_value'] > 0)
                    | (df['dg2_value'] > 0)
                    | (df['dg3_value'] > 0)
                    | (df['dg4_value'] > 0)
                    | (df['dg5_value'] > 0)]

        return df
