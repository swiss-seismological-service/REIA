from datetime import datetime
from uuid import UUID

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from reia.datamodel import RiskAssessment
from reia.webservice.repositories.base import async_repository_factory
from reia.webservice.schemas import WSRiskAssessment


class RiskAssessmentRepository(async_repository_factory(
        WSRiskAssessment, RiskAssessment)):

    @classmethod
    def get_filtered_query(
        cls,
        originid: str | None = None,
        starttime: datetime | None = None,
        endtime: datetime | None = None,
        published: bool | None = None,
        preferred: bool | None = None
    ) -> Select:
        """
        Build a filtered query for risk assessments
        Returns SQLAlchemy Select statement for pagination
        """
        stmt = select(RiskAssessment) \
            .options(selectinload(RiskAssessment.losscalculation),
                     selectinload(RiskAssessment.damagecalculation))

        if starttime:
            stmt = stmt.filter(
                RiskAssessment.creationinfo_creationtime >= starttime)
        if endtime:
            stmt = stmt.filter(
                RiskAssessment.creationinfo_creationtime <= endtime)
        if originid:
            stmt = stmt.filter(RiskAssessment.originid == originid)
        if published is not None:
            stmt = stmt.filter(RiskAssessment.published == published)
        if preferred is not None:
            stmt = stmt.filter(RiskAssessment.preferred == preferred)

        stmt = stmt.order_by(RiskAssessment.creationinfo_creationtime.desc())

        return stmt

    @classmethod
    async def get_by_id(cls, session: AsyncSession,
                        oid: UUID) -> RiskAssessment | None:
        """Get a single risk assessment by UUID with relationships loaded"""
        stmt = select(RiskAssessment) \
            .where(RiskAssessment._oid == oid) \
            .options(selectinload(RiskAssessment.losscalculation),
                     selectinload(RiskAssessment.damagecalculation))
        result = await session.execute(stmt)
        return result.unique().scalar_one_or_none()
