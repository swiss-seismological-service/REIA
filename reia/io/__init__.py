import configparser
import enum
from dataclasses import dataclass

from reia.datamodel.calculations import (DamageCalculation,
                                         DamageCalculationBranch,
                                         LossCalculation,
                                         LossCalculationBranch)
from reia.datamodel.fragility import (BusinessInterruptionFragilityModel,
                                      ContentsFragilityModel,
                                      NonstructuralFragilityModel,
                                      StructuralFragilityModel)
from reia.datamodel.vulnerability import (
    BusinessInterruptionVulnerabilityModel, ContentsVulnerabilityModel,
    NonstructuralVulnerabilityModel, OccupantsVulnerabilityModel,
    StructuralVulnerabilityModel)

LOSSCATEGORY_VULNERABILITY_MAPPING = {
    'structural': StructuralVulnerabilityModel,
    'nonstructural': NonstructuralVulnerabilityModel,
    'contents': ContentsVulnerabilityModel,
    'business_interruption': BusinessInterruptionVulnerabilityModel,
    'occupants': OccupantsVulnerabilityModel}

LOSSCATEGORY_FRAGILITY_MAPPING = {
    'structural': StructuralFragilityModel,
    'nonstructural': NonstructuralFragilityModel,
    'contents': ContentsFragilityModel,
    'business_interruption': BusinessInterruptionFragilityModel}

CALCULATION_MAPPING = {'scenario_risk': LossCalculation,
                       'scenario_damage': DamageCalculation}

CALCULATION_BRANCH_MAPPING = {'scenario_risk': LossCalculationBranch,
                              'scenario_damage': DamageCalculationBranch}

ASSETS_COLS_MAPPING = {'taxonomy': 'taxonomy_concept',
                       'number': 'buildingcount',
                       'contents': 'contentsvalue',
                       'day': 'dayoccupancy',
                       'night': 'nightoccupancy',
                       'transit': 'transitoccupancy',
                       'structural': 'structuralvalue',
                       'nonstructural': 'nonstructuralvalue',
                       'business_interruption': 'businessinterruptionvalue'
                       }

VULNERABILITY_FK_MAPPING = {
    'structural_vulnerability_file': '_structuralvulnerabilitymodel_oid',
    'contents_vulnerability_file': '_contentsvulnerabilitymodel_oid',
    'occupants_vulnerability_file': '_occupantsvulnerabilitymodel_oid',
    'nonstructural_vulnerability_file': '_nonstructuralvulnerabilitymodel_oid',
    'business_interruption_vulnerability_file':
    '_businessinterruptionvulnerabilitymodel_oid',
    'taxonomy_mapping_csv': '_taxonomymap_oid'}

FRAGILITY_FK_MAPPING = {
    'structural_fragility_file': '_structuralfragilitymodel_oid',
    'contents_fragility_file': '_contentsfragilitymodel_oid',
    'nonstructural_fragility_file': '_nonstructuralfragilitymodel_oid',
    'business_interruption_fragility_file':
    '_businessinterruptionfragilitymodel_oid',
    'taxonomy_mapping_csv': '_taxonomymap_oid'}


@dataclass
class CalculationBranchSettings:
    """ Contains the weight and a OQ settings file for a calculation"""
    weight: float
    config: configparser.ConfigParser


class ERiskType(str, enum.Enum):
    LOSS = 'scenario_risk'
    DAMAGE = 'scenario_damage'


RISK_COLUMNS_MAPPING = {
    ERiskType.LOSS: {'event_id': 'eventid',
                     'agg_id': 'aggregationtags',
                     'loss_id': 'losscategory',
                     'loss': 'loss_value'},
    ERiskType.DAMAGE: {'event_id': 'eventid',
                       'agg_id': 'aggregationtags',
                       'loss_id': 'losscategory',
                       'dmg_1': 'dg1_value',
                       'dmg_2': 'dg2_value',
                       'dmg_3': 'dg3_value',
                       'dmg_4': 'dg4_value',
                       'dmg_5': 'dg5_value', }}
