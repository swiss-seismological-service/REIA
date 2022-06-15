import io
from sqlalchemy.orm import Session
from esloss.datamodel.calculations import LossCalculation, EStatus
from core.db.crud import update_calculation_status

from core.oqapi import (oqapi_send_calculation,
                        oqapi_wait_for_job_completion,
                        oqapi_wait_for_job_status)


def execute_openquake_calculation(files: list[io.StringIO],
                                  calculation: LossCalculation,
                                  session: Session):
    try:
        # send calculation to openquake
        response = oqapi_send_calculation(*files)

        # check if response is ok
        if response.ok:
            update_calculation_status(
                calculation._oid, EStatus.PENDING, session)
        else:
            return update_calculation_status(
                calculation._oid, EStatus.ERROR, session)

        # wait for job to run and update status to RUNNING
        oqapi_wait_for_job_status(
            response.json()['job_id'],
            ('executing', 'complete', 'failed', 'aborted'))
        update_calculation_status(
            calculation._oid, EStatus.RUNNING, session)

        end_status = oqapi_wait_for_job_completion(calculation._oid)

        if end_status == 'complete':
            calculation = update_calculation_status(
                calculation._oid, EStatus.COMPLETE, session)
        else:
            calculation = update_calculation_status(
                calculation._oid, EStatus.ERROR, session)

        return calculation
    except Exception as e:
        update_calculation_status(
            calculation._oid, EStatus.ERROR, session)
        raise e
