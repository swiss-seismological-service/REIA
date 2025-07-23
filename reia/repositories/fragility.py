import pandas as pd
from sqlalchemy import insert, select
from sqlalchemy.orm import Session

from reia.datamodel.fragility import \
    BusinessInterruptionFragilityModel as BusinessInterruptionFragilityModelORM
from reia.datamodel.fragility import \
    ContentsFragilityModel as ContentsFragilityModelORM
from reia.datamodel.fragility import FragilityFunction as FragilityFunctionORM
from reia.datamodel.fragility import FragilityModel as FragilityModelORM
from reia.datamodel.fragility import LimitState as LimitStateORM
from reia.datamodel.fragility import Mapping as MappingORM
from reia.datamodel.fragility import \
    NonstructuralFragilityModel as NonstructuralFragilityModelORM
from reia.datamodel.fragility import \
    StructuralFragilityModel as StructuralFragilityModelORM
from reia.datamodel.fragility import TaxonomyMap as TaxonomyMapORM
from reia.repositories import pandas_read_sql
from reia.repositories.base import repository_factory
from reia.schemas.fragility_schemas import (BusinessInterruptionFragilityModel,
                                            ContentsFragilityModel,
                                            FragilityFunction, FragilityModel,
                                            LimitState, Mapping,
                                            NonstructuralFragilityModel,
                                            StructuralFragilityModel,
                                            TaxonomyMap)


class FragilityModelRepository(repository_factory(
        FragilityModel, FragilityModelORM)):
    @classmethod
    def create(cls, session: Session, data: FragilityModel) -> FragilityModel:
        """Create a new fragility model."""

        fragility_functions = []
        for ff in data.fragilityfunctions:
            limit_states = [LimitStateORM(**ls.model_dump())
                            for ls in ff.limitstates]
            fragility_function = FragilityFunctionORM(
                **ff.model_dump(exclude={'limitstates'}))
            fragility_function.limitstates = limit_states
            fragility_functions.append(fragility_function)

        db_model = FragilityModelORM(
            **data.model_dump(exclude={'fragilityfunctions'}))
        db_model.fragilityfunctions = fragility_functions

        session.add(db_model)
        session.commit()
        session.refresh(db_model)
        return FragilityModel.model_validate(db_model)


class ContentsFragilityModelRepository(repository_factory(
        ContentsFragilityModel, ContentsFragilityModelORM)):
    pass


class StructuralFragilityModelRepository(repository_factory(
        StructuralFragilityModel, StructuralFragilityModelORM)):
    pass


class NonstructuralFragilityModelRepository(repository_factory(
        NonstructuralFragilityModel, NonstructuralFragilityModelORM)):
    pass


class BusinessInterruptionFragilityModelRepository(
    repository_factory(
        BusinessInterruptionFragilityModel,
        BusinessInterruptionFragilityModelORM)):
    pass


class FragilityFunctionRepository(repository_factory(
        FragilityFunction, FragilityFunctionORM)):
    pass


class LimitStateRepository(repository_factory(
        LimitState, LimitStateORM)):
    pass


class TaxonomyMapRepository(repository_factory(
        TaxonomyMap, TaxonomyMapORM)):
    @classmethod
    def insert_many(cls,
                    session: Session,
                    mappings: pd.DataFrame,
                    name: str) -> TaxonomyMap:
        """Insert multiple mappings into the repository."""
        taxonomy_map = TaxonomyMapORM(name=name)
        session.add(taxonomy_map)
        session.flush()

        mappings['_taxonomymap_oid'] = taxonomy_map._oid
        stmt = insert(MappingORM).values(
            mappings.to_dict(orient='records'))

        session.execute(stmt)
        session.commit()
        session.refresh(taxonomy_map)

        return TaxonomyMap.model_validate(taxonomy_map)


class MappingRepository(repository_factory(
        Mapping, MappingORM)):
    @classmethod
    def get_by_taxonomymap_oid(cls,
                               session: Session,
                               taxonomymap_oid: int) -> pd.DataFrame:
        """Get mappings by taxonomy map OID."""
        stmt = select(MappingORM).where(
            MappingORM._taxonomymap_oid == taxonomymap_oid)
        result = pandas_read_sql(stmt, session)
        return result
