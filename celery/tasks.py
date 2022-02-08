from celery import Celery
from core.utils import oqapi_wait_for_job
from core.database import session
from esloss.datamodel import MeanAssetLoss

from openquake.calculators.extract import Extractor


celery = Celery()


@celery.task()
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
