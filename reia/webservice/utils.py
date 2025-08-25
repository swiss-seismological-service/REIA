import pandas as pd
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from reia.config.names import (csv_column_names, csv_names_aggregations,
                               csv_names_categories, csv_names_sum, csv_round)
from reia.webservice.wquantile import weighted_quantile


def replace_path_param_type(app: FastAPI,
                            path_param: str,
                            new_type: type):

    for i, route in enumerate(app.router.routes):
        if f'{{{path_param}}}' in route.path:
            path = route.path
            route.endpoint.__annotations__[path_param] = new_type
            endpoint = route.endpoint
            methods = route.methods

            del app.router.routes[i]
            app.add_api_route(path, endpoint, methods=methods)


def csv_response(type: str, *args) -> StreamingResponse:
    """
    Generate descriptive CSV-names which can be edited using config.names.
    """
    args = args[0]

    oid = args['calculation_id']
    agg = args['aggregation_type']
    filter = args['filter_tag_like']
    category = args[f'{type}_category']
    sum = args['sum']

    filename = construct_csv_filename(type, oid, agg, filter, category, sum)
    data = args['statistics']

    data.drop(columns=['category'], inplace=True)

    if sum:
        data.drop(columns=['tag'], inplace=True)
    data = rename_column_headers(data, type, category, agg)

    data = data.round(csv_round)

    output = data.to_csv(index=False)

    return StreamingResponse(
        iter([output]),
        media_type='text/csv',
        headers={"Content-Disposition":
                 f"attachment;filename={filename}.csv"})


def construct_csv_filename(type, oid, agg, filter, category, sum) -> str:
    if sum:
        agg = csv_names_sum[agg] if agg in csv_names_sum else f'{agg}-sum'
    else:
        agg = csv_names_aggregations[agg] if agg in \
            csv_names_aggregations else agg

    category = csv_names_categories[type][category] if (
        type in csv_names_categories
        and category in csv_names_categories[type]) else category

    return f"{type}_{oid}_" \
        f"{agg}" \
        f"{f'-{filter}' if filter else ''}" \
        f"_{category}"


def rename_column_headers(
        df: pd.DataFrame, type, category, agg) -> pd.DataFrame:

    mapping = csv_column_names[type][category] if (
        type in csv_column_names
        and category in csv_column_names[type]) else {}

    tag_mapping = csv_column_names['aggregation'][agg] if (
        agg in csv_column_names['aggregation']) else {}

    # build list of dictionary keys and apply order to df columns
    # as well as unselecting columns which are not present in naming dict
    tag_name = list(tag_mapping.keys()) or \
        ['tag'] if 'tag' in df.columns else []
    order = tag_name + [m for m in mapping.keys() if m in df.columns]

    df = df[order]

    return df.rename(columns=mapping | tag_mapping)


def aggregate_by_branch_and_event(
        data: pd.DataFrame, aggregation_type) -> pd.DataFrame:

    # group by branch and event
    group = data.groupby(
        lambda x: data['branchid'].loc[x]
        + data['eventid'].loc[x] * (10 ** 4))

    # get value columns
    value_column = [i for i in data.columns if 'value' in i]

    values = pd.DataFrame()
    values['weight'] = group.apply(
        lambda x: x['weight'].sum() / len(x))
    for name in value_column:
        values[name] = group.apply(
            lambda x: x[name].sum())

    values[aggregation_type] = False

    return values


def calculate_statistics(
        data: pd.DataFrame, aggregation_type: str) -> pd.DataFrame:
    """
    Calculate weighted statistics (mean, 10th and 90th percentiles) for value columns.
    
    Uses parallel processing for large datasets with multiple value columns
    to improve performance.
    
    Args:
        data: DataFrame with value columns, weight, and aggregation_type
        aggregation_type: Column name to group by for statistics
        
    Returns:
        DataFrame with calculated statistics per aggregation group
    """
    from concurrent.futures import ThreadPoolExecutor

    value_columns = [col for col in data.columns if 'value' in col]
    
    if not value_columns:
        return pd.DataFrame()

    # Pre-compute groupby for quantile calculations
    grouped_data = data.groupby(aggregation_type)
    
    def calculate_column_statistics(column_info):
        """Calculate mean and percentiles for a single value column"""
        _, column = column_info
        base_name = column.split('_')[0]

        # Create working copy with weighted values
        working_data = data[[column, 'weight', aggregation_type]].copy()
        working_data['weighted_value'] = working_data['weight'] * working_data[column]

        # Calculate weighted mean
        mean_values = working_data.groupby(aggregation_type)['weighted_value'].sum()

        # Calculate weighted quantiles (10th and 90th percentiles)
        quantile_results = _calculate_weighted_quantiles(
            grouped_data, column, (0.1, 0.9)
        )
        pc10_values, pc90_values = zip(*quantile_results)

        return {
            f'{base_name}_mean': mean_values,
            f'{base_name}_pc10': pd.Series(pc10_values, index=mean_values.index),
            f'{base_name}_pc90': pd.Series(pc90_values, index=mean_values.index)
        }

    # Choose processing strategy based on data size and column count
    column_statistics = _process_columns(
        value_columns, calculate_column_statistics, data
    )

    # Combine all column statistics into final DataFrame
    return _combine_statistics(column_statistics, aggregation_type)


def _calculate_weighted_quantiles(grouped_data, column, quantiles):
    """Calculate weighted quantiles with pandas version compatibility"""
    try:
        # Newer pandas versions
        return grouped_data.apply(
            lambda group: weighted_quantile(
                group[column], quantiles, group['weight']
            ),
            include_groups=False
        )
    except TypeError:
        # Older pandas versions fallback
        return grouped_data.apply(
            lambda group: weighted_quantile(
                group[column], quantiles, group['weight']
            )
        )


def _process_columns(value_columns, calculate_fn, data):
    """Process columns sequentially or in parallel based on data characteristics"""
    from concurrent.futures import ThreadPoolExecutor
    
    should_parallelize = len(value_columns) > 1 and len(data) > 100000
    
    if should_parallelize:
        # Parallel processing for large multi-column datasets
        with ThreadPoolExecutor(max_workers=3) as executor:
            return list(executor.map(
                calculate_fn, enumerate(value_columns)
            ))
    else:
        # Sequential processing for small datasets or single columns
        return [calculate_fn((i, col)) for i, col in enumerate(value_columns)]


def _combine_statistics(column_statistics, aggregation_type):
    """Combine individual column statistics into final DataFrame"""
    if not column_statistics:
        return pd.DataFrame()
    
    # Merge all column results
    statistics = pd.DataFrame()
    for column_result in column_statistics:
        for stat_name, stat_series in column_result.items():
            statistics[stat_name] = stat_series

    # Format final output
    statistics = statistics.rename_axis('tag').reset_index()
    statistics['tag'] = statistics['tag'].apply(lambda x: [x] if x else [])
    statistics = statistics.round(5)

    return statistics


def merge_statistics_to_buildings(statistics: pd.DataFrame,
                                  buildings: pd.DataFrame,
                                  aggregation_type: str) -> pd.DataFrame:

    statistics['merge_tag'] = statistics['tag'].apply(
        lambda x: ''.join(sorted(x)))

    # add sum of buildings to dataframe
    buildings = pd.concat([
        buildings,
        pd.DataFrame([{'buildingcount': buildings['buildingcount'].sum(),
                       aggregation_type: ''}])
    ], ignore_index=True)

    statistics = statistics.merge(
        buildings.rename(columns={'buildingcount': 'buildings'}),
        how='inner',
        left_on='merge_tag',
        right_on=aggregation_type).fillna(0)

    # remove columns which were added in this method
    statistics.drop(columns=['merge_tag', aggregation_type], inplace=True)

    return statistics


async def pandas_read_sql(stmt, session):
    """
    Get a pandas dataframe from a SQL statement.
    """
    result = await session.execute(stmt)
    rows = result.fetchall()
    columns = result.keys()
    df = pd.DataFrame(rows, columns=columns)
    return df
