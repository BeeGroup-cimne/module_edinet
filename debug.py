from bee_dataframes.utils_connection import BeeDataConnection
import pandas as pd
from calendar import monthrange
from list_functions import unique, getFromDict, pretty_numeric, ensure_list
import json
from datetime_functions import date_n_month, last_day_n_month
from dateutil.relativedelta import relativedelta
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import bee_data_cleaning as dc
host = '217.182.160.171'
db = 'edinet'
username = "bgruser"
password = "gR0uP_b33u$er"

host = '37.59.27.175'
db = 'edinet_rest_service'
username = "cimne-edinet"
password = "3nm1C--3d1n3t"

conn = BeeDataConnection(db,host,username,password)

x = conn.mongo_query_find_one('raw_data', {"deviceId": "4789a2cf-d00a-539c-afd0-705dd01bf721"})
x = conn.mongo_query_find_one('raw_data', {"deviceId": "0593785b-14b6-50d3-9bff-92e9f58051cb"})
x = conn.mongo_query_find_one('raw_data', {"deviceId": "081fb5a6-ec15-5300-a00d-ea419dd700ce"})
x = conn.mongo_query_find_one('raw_data', {"deviceId": "086cc9a5-210f-5349-af77-9611130a59b0"})

df = pd.DataFrame({"timestamps": x['timestamps'], "v": x['values']})

df = df.set_index('timestamps')

df = df.sort_index()

negatives = dc.detect_min_threshold_outliers(df.v, 0)
df.v = dc.clean_series(df.v, negatives)

outliers = dc.detect_znorm_outliers(df.v, 30, "rolling", window=168)

df.v = dc.clean_series(df.v, outliers)

df.v.plot()

plt.show()



x = conn.mongo_query_find_one('raw_data', {"deviceId": "0017ce17-76cd-5b13-b7de-f827840fb197"})
x = conn.mongo_query_find_one('raw_data', {"deviceId": "00e63d65-2fd5-561f-835a-c9df50aaa59d"})
x = conn.mongo_query_find_one('raw_data', {"deviceId": "01208619-5e94-58b8-9673-608febf0dab9"})
x = conn.mongo_query_find_one('raw_data', {"deviceId": "34c62202-0075-5c50-a3e5-1f2bb51d04aa"})
df = pd.DataFrame({"timestamps": x['timestamps'], "v": x['values']})

df = df.set_index('timestamps')

df = df.sort_index()

df = df.resample("D").max().interpolate().diff(1,0)

plt.figure()
df.v.plot()

plt.show()


df1

modelling_unit = "edinetId4368-electricityConsumption"

df_new = conn.obtain_daily_dataset(modelling_unit)

df_new.dropna(inplace=True)
if df_new.empty != True:
    number_of_days = df_new.groupby(pd.TimeGrouper(freq='M')).count()
    number_of_days.rename(columns={'value': 'numberOfDays'},inplace=True)
    df_new = df_new.groupby(pd.TimeGrouper(freq='M')).sum()
    final_df = df_new.join(number_of_days)
    final_df.dropna(inplace=True)
else:
    final_df = pd.DataFrame()
surf = None

res = []
for k, value in final_df.iterrows():
    res_parcial = {}
    res_parcial["v_surf"] = (value.value / surf) if surf else "null"
    res_parcial["v"] = value.value
    res_parcial["m"] = int(k.strftime('%Y%m'))
    res_parcial["rd"] = int(value.numberOfDays)
    res_parcial["td"] = monthrange(k.year,k.month)[1]
    res.append(res_parcial)

value_months = []
keys_for_filtering_and_best_criteria=['rawMonths', 'v']
item_v = keys_for_filtering_and_best_criteria[0]
for item in res:
    item['v'] = item['v'] * item.get('td') / item.get('rd')  ##
    item['v_surf'] = item['v_surf'] * item.get('td') / item.get('rd') if item['v_surf'] != 'null' else None
    ym = int(item.pop('m'))
    value = getFromDict(item, keys_for_filtering_and_best_criteria[1:]) if len(keys_for_filtering_and_best_criteria) > 1 else item

    if not ym in value_months:
        value_months.append({
            'm': ym,
            item_v: json.loads(json.dumps(value))
        })

criteria_groups = [("entityId","SHERPA_LAZIO")]
df = pd.DataFrame(value_months)


df = df.set_index('m').sort_index()
ts_to = datetime(2016, 11, 30, 23, 59, 59)
min_yearmonths=[int((last_day_n_month(ts_to, 0) + relativedelta(seconds=1) - relativedelta(months=i)).strftime("%Y%m")) for
                        i in [12]]
cost = {}
i=0
df_i = df[df.index >= min_yearmonths[i]]
unique_ym = unique(list(df_i.index))
val = list(df_i[item_v])
columns_similar = [item[0] for item in [('month', 'int'), ('criteria', 'string'), ('groupCriteria', 'string'),
                      ('average', 'string'),
                      ('penalty', 'float'), ('numberCustomers', 'int'), ('coefficientVariation', 'float'),
                      ('coefficientDispersion', 'float')]]
similar = []
for c, gc in criteria_groups:
    mongo_r = conn.mongo_query_find("edinet_similar_groups_description",{"criteria":c,"groupCriteria":gc, "type":"tertiaryElectricityConsumption"})
    for m in mongo_r:
        similar.append(m)

df_similars = pd.DataFrame(similar)
df_similars = df_similars.set_index('month').sort_index()
df_s_i = df_similars[[item in unique_ym for item in list(df_similars.index)]]
df_s_i = df_s_i.reset_index().drop_duplicates(subset='month', keep='last').set_index('month').sort_index()
fixed_part = [50]
n_months = [12]
for cg in criteria_groups:
    # Get the similar neighbors within this criteria group
    similar = df_s_i[list(df_s_i.criteria == cg[0])]

    if len(similar) > 0:
        similar = similar[list(similar.groupCriteria == cg[1])][
            ['average', 'coefficientVariation', 'coefficientDispersion']]
        similar = similar.reindex(unique_ym)

        if len(similar) > 0:
            # try:
            avg = list(similar.average)
            # except:
            #    yield _, str(list(similar.average))

            diff = []
            # Calculate the difference between customer month value and the related average within each group criteria
            #### Euclidean distance is computed because the monthly values could be either a float or a list.
            #### The result for each month is always a float or np.nan.
            for j in range(len(val)):
                if (isinstance(val[j], list) or isinstance(val[j], float)) and \
                        (isinstance(avg[j], list) or isinstance(avg[j], float)) and \
                        len(ensure_list(val[j])) == len(ensure_list(avg[j])):
                    diff.append(np.linalg.norm(((np.array(val[j]) - np.array(avg[j])) / np.array(avg[j])) * 100))
                else:
                    diff.append(np.nan)

            # Fixed_part iterator
            for f in fixed_part:

                # COST FUNCTION
                cst = np.array(similar.coefficientDispersion) * (f / 100) + np.array(similar.coefficientDispersion) * \
                      (1 - (f / 100)) * np.array(diff)

                if "%s~%s" % (str(n_months[i]), str(f)) not in cost.keys():
                    cost["%s~%s" % (str(n_months[i]), str(f))] = []
                cst_v = pretty_numeric(np.nanmean(cst), 3)
                if cst_v and cg[1] != 'null':
                    cost["%s~%s" % (str(n_months[i]), str(f))].append({
                        'criteria': cg[0],
                        'groupCriteria': cg[1],
                        'ldiff': pretty_numeric(diff, 3),
                        'lcost': pretty_numeric(cst, 3),
                        'diff': pretty_numeric(np.nanmean(diff), 3),
                        'cost': cst_v
                    })


# Yield the results
for n_month in n_months:
    for f in fixed_part:
        key = "%s~%s" % (str(n_month), str(f))
        if key in cost.keys():
            l = sorted(cost[key], key=lambda x: x['cost'])
            # replacing the 'null' by None
            values = res
            if isinstance(values, list):
                for i in range(len(values)):
                    if isinstance(values[i], dict):
                        for key, value in values[i].items():
                            if value == 'null':
                                values[i][key] = None


#POSSIBLE ERROR( SIMILAR GROUP DESCRIPTIONS IGUAL REPETITS)