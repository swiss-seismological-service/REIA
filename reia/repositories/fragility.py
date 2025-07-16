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
    pass


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
    pass


class MappingRepository(repository_factory(
        Mapping, MappingORM)):
    pass
