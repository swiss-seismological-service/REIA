from reia.io import FRAGILITY_FK_MAPPING, VULNERABILITY_FK_MAPPING
import configparser
from itertools import groupby

import pandas as pd

from reia.schemas.calculation_schemas import (BranchInputSchema,
                                              CalculationBranchSettings,
                                              DamageCalculation,
                                              DamageCalculationBranch,
                                              LossCalculation,
                                              LossCalculationBranch)
from reia.utils import flatten_config


def validate_and_parse_calculation_input(
        branch_settings: list[CalculationBranchSettings]
) -> tuple[LossCalculation | DamageCalculation,
           list[LossCalculationBranch | DamageCalculationBranch]]:
    """Validates input and returns existing database Pydantic schemas.

    Args:
        branch_settings: List of calculation branch settings.

    Returns:
        Tuple containing calculation and branches ready for database insertion
    """
    validate_calculation_input(branch_settings)
    parsed_data = parse_calculation_branches(branch_settings)
    return map_to_database_schemas(parsed_data)


def validate_calculation_input(
        branch_settings: list[CalculationBranchSettings]) -> None:
    """Validate calculation input settings.

    Args:
        branch_settings: List of calculation branch settings to validate.
    """
    # Validate weights sum to 1
    total_weight = sum(branch.weight for branch in branch_settings)
    if abs(total_weight - 1.0) > 1e-6:
        raise ValueError(
            'The sum of the weights for calculation branches must be 1.')
    # Validate cross-branch consistency
    configs = [s.config for s in branch_settings]
    validate_branch_consistency(configs)


def parse_calculation_branches(branch_settings: list[CalculationBranchSettings]
                               ) -> tuple[dict, list[dict]]:
    """Parse calculation branches into clean data structures.

    Args:
        branch_settings: List of calculation branch settings to parse.

    Returns:
        Tuple of (calculation_dict, branches_dicts)
    """
    # Parse each branch
    parsed_branches = [parse_single_branch(
        setting) for setting in branch_settings]

    # Extract calculation-level metadata from first branch
    first_branch = parsed_branches[0]
    calculation_dict = {
        'calculation_mode': first_branch.calculation_mode,
        'description': first_branch.description,
        'aggregateby': [x.strip() for x in
                        first_branch.aggregate_by.split(',')]
        if first_branch.aggregate_by else None
    }

    # Convert parsed branches to database format
    branches_dicts = [_map_branch_to_database_dict(
        branch) for branch in parsed_branches]

    return calculation_dict, branches_dicts


def map_to_database_schemas(
        parsed_data: tuple[dict, list[dict]]
) -> tuple[LossCalculation | DamageCalculation,
           list[LossCalculationBranch | DamageCalculationBranch]]:
    """Convert parsed data to database Pydantic schemas.

    Args:
        parsed_data: Tuple of (calculation_dict, branches_dicts)

    Returns:
        Tuple containing calculation and branch objects
    """
    from reia.io import CALCULATION_BRANCH_MAPPING, CALCULATION_MAPPING

    calculation_dict, branches_dicts = parsed_data

    calculation_mode = calculation_dict.pop('calculation_mode')
    calculation = CALCULATION_MAPPING[calculation_mode].model_validate(
        calculation_dict)

    calculation_branches = []
    for branch_dict in branches_dicts:
        branch_mode = branch_dict.pop('calculation_mode')
        branch = CALCULATION_BRANCH_MAPPING[branch_mode].model_validate(
            branch_dict)
        calculation_branches.append(branch)

    return calculation, calculation_branches


def _map_branch_to_database_dict(parsed_branch: BranchInputSchema) -> dict:
    """Map a parsed branch to database dictionary format.

    Args:
        parsed_branch: Parsed branch schema

    Returns:
        Dictionary ready for database schema validation
    """

    branch_dict = {
        '_exposuremodel_oid': parsed_branch.exposuremodel_oid,
        'calculation_mode': parsed_branch.calculation_mode,
        'config': parsed_branch.config_dict,
        'weight': parsed_branch.weight
    }

    # Add model references based on calculation mode
    if parsed_branch.calculation_mode == 'scenario_risk' and \
            parsed_branch.vulnerability_models:
        for model_key, model_oid in parsed_branch.vulnerability_models.items():
            branch_dict[VULNERABILITY_FK_MAPPING[
                f'{model_key}_vulnerability_file']] = model_oid

    if parsed_branch.calculation_mode == 'scenario_damage' and \
            parsed_branch.fragility_models:
        for model_key, model_oid in parsed_branch.fragility_models.items():
            branch_dict[FRAGILITY_FK_MAPPING[
                f'{model_key}_fragility_file']] = model_oid

    # Add taxonomy mapping if present
    if parsed_branch.taxonomymap_oid:
        if parsed_branch.calculation_mode == 'scenario_risk':
            branch_dict[VULNERABILITY_FK_MAPPING['taxonomy_mapping_csv']
                        ] = parsed_branch.taxonomymap_oid
        else:
            branch_dict[FRAGILITY_FK_MAPPING['taxonomy_mapping_csv']
                        ] = parsed_branch.taxonomymap_oid

    return branch_dict


def validate_branch_consistency(
        configs: list[configparser.ConfigParser]) -> None:
    """Cross-branch validation logic
    (sections, calculation_mode, aggregate_by).

    Args:
        configs: List of configparser objects to validate for consistency.

    Raises:
        ValueError: If branches are inconsistent in required ways.
    """
    # Sort aggregation keys in order to be able to string-compare
    for config in configs:
        if config.has_option('general', 'aggregate_by'):
            sorted_agg = [x.strip() for x in
                          config['general']['aggregate_by'].split(',')]
            sorted_agg = ','.join(sorted(sorted_agg))
            config['general']['aggregate_by'] = sorted_agg

    # Validate that the necessary options are consistent over branches
    if not _equal_section_options(configs, 'vulnerability'):
        raise ValueError('All branches of a calculation need to calculate '
                         'the same vulnerability loss categories.')

    if not _equal_section_options(configs, 'fragility'):
        raise ValueError('All branches of a calculation need to calculate '
                         'the same fragility damage categories.')

    if not _equal_option_value(configs, 'general', 'aggregate_by'):
        raise ValueError('Aggregation keys must be the same '
                         'in all calculation branches.')

    if not _equal_option_value(configs, 'general', 'calculation_mode'):
        raise ValueError('Calculation mode must be the same '
                         'in all calculation branches.')


def parse_single_branch(
        setting: CalculationBranchSettings) -> BranchInputSchema:
    """Parse one branch config into validated schema.

    Args:
        setting: Single calculation branch setting to parse.

    Returns:
        Validated BranchInputSchema object.
    """
    config = setting.config

    # Clean and flatten config
    flat_job = configparser.ConfigParser()
    flat_job.read_dict(config)
    for s in ['vulnerability', 'exposure', 'hazard', 'fragility']:
        if flat_job.has_section(s):
            flat_job.remove_section(s)
    flat_job = flatten_config(flat_job)

    # Extract basic metadata
    calculation_mode = flat_job.pop('calculation_mode')
    description = flat_job.pop('description', None)
    aggregate_by = flat_job.pop('aggregate_by', None)

    # Get exposure model OID
    exposuremodel_oid = config['exposure']['exposure_file']

    # Count number of GMFs
    gmfs = pd.read_csv(config['hazard']['gmfs_csv'])
    number_of_ground_motion_fields = len(gmfs.eid.unique())

    # Parse model references based on calculation mode
    vulnerability_models = None
    fragility_models = None
    taxonomymap_oid = None

    if calculation_mode == 'scenario_risk' and config.has_section(
            'vulnerability'):
        vulnerability_models = {}
        for k, v in config['vulnerability'].items():
            if k != 'taxonomy_mapping_csv':
                model_key = k.replace('_vulnerability_file', '')
                vulnerability_models[model_key] = v
            else:
                taxonomymap_oid = v

    if calculation_mode == 'scenario_damage' and config.has_section(
            'fragility'):
        fragility_models = {}
        for k, v in config['fragility'].items():
            if k != 'taxonomy_mapping_csv':
                model_key = k.replace('_fragility_file', '')
                fragility_models[model_key] = v
            else:
                taxonomymap_oid = v

    return BranchInputSchema(
        weight=setting.weight,
        calculation_mode=calculation_mode,
        description=description,
        aggregate_by=aggregate_by,
        exposuremodel_oid=exposuremodel_oid,
        config_dict=flat_job,
        number_of_ground_motion_fields=number_of_ground_motion_fields,
        vulnerability_models=vulnerability_models,
        fragility_models=fragility_models,
        taxonomymap_oid=taxonomymap_oid
    )


def _equal_section_options(configs: list[configparser.ConfigParser],
                           name: str) -> bool:
    """Returns True if all configparsers have consistent section options.

    Args:
        configs: List of configparser objects to compare.
        name: Name of the section to check.

    Returns:
        True if all configparsers have:
            - The same section and the same option keys inside section.
            - All don't have the section.
    """
    has_any = any(c.has_section(name) for c in configs)

    if not has_any:
        return True

    has_all = all(c.has_section(name) for c in configs)

    if not has_all:
        return False

    g = groupby(dict(c[name]).keys() for c in configs)
    return next(g, True) and not next(g, False)


def _equal_option_value(configs: list[configparser.ConfigParser],
                        section: str, name: str) -> bool:
    """Returns True if all configparsers have consistent option values.

    Args:
        configs: List of configparser objects to compare.
        section: Name of the section containing the option.
        name: Name of the option to check.

    Returns:
        True if all configparsers have:
            - The same option inside the same section with the same value.
            - All don't have this option.
    """
    has_any = any(c.has_option(section, name) for c in configs)

    if not has_any:
        return True

    has_all = all(c.has_option(section, name) for c in configs)

    if not has_all:
        return False

    g = groupby(c[section][name] for c in configs)
    return next(g, True) and not next(g, False)
