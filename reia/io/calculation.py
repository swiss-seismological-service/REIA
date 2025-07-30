import configparser
from itertools import groupby

from reia.io import (CALCULATION_BRANCH_MAPPING, CALCULATION_MAPPING,
                     MODEL_FIELD_MAPPINGS)
from reia.schemas.calculation_schemas import (Calculation, CalculationBranch,
                                              CalculationBranchSettings)
from reia.utils import flatten_config


def validate_calculation_input(
        branch_settings: list[CalculationBranchSettings]) -> None:
    """Cross-branch validation,
    (sections, calculation_mode, aggregate_by, weights).

    Args:
        branch_settings: List of calculation branch settings to validate.

    Raises:
        ValueError: If branches are inconsistent in required ways.
    """
    # Validate weights sum to 1
    total_weight = sum(branch.weight for branch in branch_settings)
    if abs(total_weight - 1.0) > 1e-6:
        raise ValueError(
            'The sum of the weights for calculation branches must be 1.')

    # Validate cross-branch consistency
    configs = [s.config for s in branch_settings]

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


def create_calculation(
        branch_settings: list[CalculationBranchSettings]) -> Calculation:
    """Extract calculation-level metadata from branch settings.

    Args:
        branch_settings: List of calculation branch settings.

    Returns:
        Dictionary with calculation metadata ready for schema validation.
    """
    # Use first branch to extract calculation-level metadata
    first_config = branch_settings[0].config

    calculation_mode = first_config['general']['calculation_mode']
    description = first_config['general'].get('description')
    aggregate_by = first_config['general'].get('aggregate_by')

    calculation_dict = {
        'description': description,
        'aggregateby': [x.strip() for x in aggregate_by.split(',')]
        if aggregate_by else None
    }

    calculation = CALCULATION_MAPPING[calculation_mode].model_validate(
        calculation_dict
    )

    return calculation


def create_calculation_branch(config: configparser.ConfigParser,
                              weight: float
                              ) -> CalculationBranch:
    """Create calculation branch directly from settings.

    Args:
        setting: Single calculation branch setting to parse.

    Returns:
        Validated CalculationBranch object.
    """

    # Clean and flatten config
    flat_job = configparser.ConfigParser()
    flat_job.read_dict(config)
    for s in ['vulnerability', 'exposure', 'hazard', 'fragility']:
        if flat_job.has_section(s):
            flat_job.remove_section(s)
    flat_job = flatten_config(flat_job)

    # Extract calculation mode
    calculation_mode = flat_job.pop('calculation_mode')

    # Get exposure model OID
    exposuremodel_oid = config['exposure']['exposure_file']

    # Build branch data dictionary
    branch_dict = {
        '_exposuremodel_oid': exposuremodel_oid,
        'config': flat_job,
        'weight': weight
    }

    # Add model references using unified mapping approach
    if calculation_mode in MODEL_FIELD_MAPPINGS:
        mapping_config = MODEL_FIELD_MAPPINGS[calculation_mode]
        model_section = mapping_config['model_section']
        model_suffix = mapping_config['model_suffix']
        field_mapping = mapping_config['field_mapping']

        if config.has_section(model_section):
            for k, v in config[model_section].items():
                if k == 'taxonomy_mapping_csv':
                    branch_dict[mapping_config['taxonomy_field']] = v
                elif k.endswith(model_suffix):
                    branch_dict[field_mapping[k]] = v

    # Get appropriate branch class and create instance
    branch_class = CALCULATION_BRANCH_MAPPING[calculation_mode]
    return branch_class.model_validate(branch_dict)


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
