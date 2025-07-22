import enum


class ELossCategory(str, enum.Enum):
    CONTENTS = 'contents'
    BUSINESS_INTERRUPTION = 'business_interruption'
    NONSTRUCTURAL = 'nonstructural'
    OCCUPANTS = 'occupants'
    STRUCTURAL = 'structural'
    NULL = 'null'


class EStatus(int, enum.Enum):
    FAILED = 1
    ABORTED = 2
    CREATED = 3
    SUBMITTED = 4
    EXECUTING = 5
    COMPLETE = 6


class EEarthquakeType(str, enum.Enum):
    SCENARIO = 'scenario'
    NATURAL = 'natural'


class ECalculationType(str, enum.Enum):
    RISK = 'risk'
    LOSS = 'loss'
    DAMAGE = 'damage'


class ERiskType(str, enum.Enum):
    LOSS = 'scenario_risk'
    DAMAGE = 'scenario_damage'
