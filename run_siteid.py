
import pandas as pd
from esloss.datamodel.asset import Site
from esloss.datamodel.lossvalues import ELossCategory, SiteLoss
from openquake.commonlib.datastore import read
from openquake.risklib.scientific import LOSSTYPE
from sqlalchemy import select

from core.db import session

pd.set_option('display.max_rows', None)
dstore = read(68)  # site_id
# dstore = read(60)  # Canton


oq_parameter_inputs = dstore['oqparam']
all_keys = list(dstore.keys())

all_agg_keys = [d.decode().split(',')
                for d in dstore['agg_keys'][:]]

total_values_per_agg_key = dstore.read_df('agg_values')

df = dstore.read_df('risk_by_event')  # get risk_by_event
# df = dstore.read_df('risk_by_event', 'event_id')  # indexed with event_id

assert(oq_parameter_inputs.aggregate_by[0][0] == 'site_id')
assert(oq_parameter_inputs.calculation_mode == 'scenario_risk')

site_collection = dstore.read_df('sitecol', 'sids')

# either or:
# all_agg_keys.append(['Total']) #
df = df.loc[df['agg_id'] != len(all_agg_keys)]

df = df.rename(columns={'event_id': 'eventid',
                        'agg_id': 'site',
                        'loss_id': 'losscategory',
                        'variance': 'loss_uncertainty',
                        'loss': 'loss_value'})

df['losscategory'] = df['losscategory'].apply(
    lambda x: ELossCategory[LOSSTYPE[x].upper()])

print(df)

stmt = select(Site) \
    .where(Site._assetcollection_oid == 2) \
    .order_by(Site.longitude, Site.latitude)

# sort sites and site_collection so I can reference by id (is not true,
# doesnt work, see notes)
sites = session.execute(stmt).scalars().all()
site_collection = site_collection.sort_values(by=['lon', 'lat'])
