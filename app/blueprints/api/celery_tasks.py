from flask import current_app

from app.extensions import celery_app
from .utils import oqapi_wait_for_job
from datamodel import session, MeanAssetLoss

from openquake.calculators.extract import Extractor


@celery_app.task(name='app.tasks.fetch_oq_results')
def fetch_oq_results(oqJobId, calcId):
    # wait for calculation to finish
    oqapi_wait_for_job(oqJobId)

    # fetch results
    extractor = Extractor(oqJobId)
    data = extractor.get('avg_losses-rlzs').to_dframe()

    data = data[['asset_id', 'value']].rename(
        columns={'asset_id': '_asset_oid', 'value': 'loss_value'})

    # save results to database
    data = data.apply(lambda x: MeanAssetLoss(
        _losscalculation_oid=calcId, **x), axis=1)
    session.add_all(data)
    session.commit()
    current_app.logger.info('Done Saving Results')
