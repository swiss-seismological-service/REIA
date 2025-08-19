from datetime import datetime

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectin_polymorphic, selectinload

from reia.datamodel import Calculation, DamageCalculation, LossCalculation
from reia.webservice.repositories.base import async_repository_factory
from reia.webservice.schemas import WSCalculation


class CalculationRepository(async_repository_factory(
        WSCalculation, Calculation)):

    @classmethod
    def get_filtered_query(
        cls,
        starttime: datetime | None = None,
        endtime: datetime | None = None
    ) -> Select:
        """
        Build a filtered query for calculations
        Returns SQLAlchemy Select statement for pagination
        """
        stmt = select(Calculation).options(
            selectin_polymorphic(
                Calculation, [LossCalculation, DamageCalculation]),
            selectinload(LossCalculation.losscalculationbranches),
            selectinload(DamageCalculation.damagecalculationbranches)
        )

        if starttime:
            stmt = stmt.filter(
                Calculation.creationinfo_creationtime >= starttime)
        if endtime:
            stmt = stmt.filter(
                Calculation.creationinfo_creationtime <= endtime)

        stmt = stmt.order_by(Calculation.creationinfo_creationtime.desc())

        return stmt

    @classmethod
    async def get_by_id(cls, session: AsyncSession,
                        id: int) -> Calculation | None:
        """Get a single calculation by ID with relationships loaded"""
        stmt = select(Calculation).options(
            selectin_polymorphic(
                Calculation, [LossCalculation, DamageCalculation]),
            selectinload(LossCalculation.losscalculationbranches),
            selectinload(DamageCalculation.damagecalculationbranches)
        ).where(Calculation._oid == id)

        result = await session.execute(stmt)
        return result.unique().scalar_one_or_none()
