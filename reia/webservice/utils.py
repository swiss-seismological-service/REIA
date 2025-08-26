import pandas as pd
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from reia.config.names import (csv_column_names, csv_names_aggregations,
                               csv_names_categories, csv_names_sum, csv_round)


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


async def pandas_read_sql(stmt, session):
    """
    Get a pandas dataframe from a SQL statement.
    """
    result = await session.execute(stmt)
    rows = result.fetchall()
    columns = result.keys()
    df = pd.DataFrame(rows, columns=columns)
    return df