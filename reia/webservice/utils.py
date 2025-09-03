import pandas as pd
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from reia.config.settings import get_webservice_settings
from reia.webservice.schemas import WSRiskCategory

settings = get_webservice_settings()


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

    data = data.round(settings.csv_names.round)

    output = data.to_csv(index=False)

    return StreamingResponse(
        iter([output]),
        media_type='text/csv',
        headers={"Content-Disposition":
                 f"attachment;filename={filename}.csv"})


def construct_csv_filename(type: str,
                           oid: int,
                           agg: str,
                           filter: str,
                           category: WSRiskCategory,
                           sum: bool) -> str:
    if sum:
        agg = settings.csv_names.sum[
            agg] if agg in settings.csv_names.sum else f'{agg}-sum'
    else:
        agg = settings.csv_names.aggregations[agg] if agg in \
            settings.csv_names.aggregations else agg

    category = settings.csv_names.categories[type][category] if (
        type in settings.csv_names.categories
        and category in settings.csv_names.categories[type]) else category

    return f"{type}_{oid}_" \
        f"{agg}" \
        f"{f'-{filter}' if filter else ''}" \
        f"_{category.value}"


def rename_column_headers(df: pd.DataFrame,
                          type: str,
                          category: WSRiskCategory,
                          agg: str) -> pd.DataFrame:

    column_names = getattr(settings.csv_names.column_names, type)
    mapping = column_names.get(category.value, {})
    tag_mapping = settings.csv_names.column_names.aggregation.get(agg, {})

    # build list of dictionary keys and apply order to df columns
    # as well as unselecting columns which are not present in naming dict
    tag_name = list(tag_mapping.keys()) or \
        ['tag'] if 'tag' in df.columns else []
    order = tag_name + [m for m in mapping.keys() if m in df.columns]

    df = df[order]

    return df.rename(columns=mapping | tag_mapping)


async def pandas_read_sql(stmt, session):
    """
    Get a pandas dataframe from a SQL statement.
    """
    result = await session.execute(stmt)
    rows = result.fetchall()
    columns = result.keys()
    df = pd.DataFrame(rows, columns=columns)
    return df
