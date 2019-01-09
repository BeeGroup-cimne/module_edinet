from time import mktime

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from  monthly_model_functions import calculate_day_degree, train_model, predict_model, fill_mean_values, denormalize_data
import bee_data_cleaning as dc
from scipy.stats import percentileofscore
import pyEMIS.ConsumptionModels as ConsumptionModels
from pyEMIS2.models.any import Factory as AF
from pyEMIS2.models.weekly import Factory as WF
from pyEMIS2.models.constant import Factory as c
from pyEMIS2.models.heating3 import Factory as h3

def monthly_calc(modelling_unit, tdf, company, multipliers, df_new):
    """
    Calculates the monthly model following this steeps
        - Translate the monthly data to daily
        - Translate the hourly data, identified by "tertiary" to daily
        - Drop duplicate daily data
        - Calculate the model

    return: A dictionary with the monthly model results.
    """
    partial_baseline = {}
    frequ = 1440
    n = -1
    df_new.dropna(inplace=True)  # nomes hi ha dades en un ts si n hi han per tots els devices en aquell ts
    # TODO: No sabem quina de les mesures s'elimina. Mirar de quedar-se amb les horaries.
    df_new = df_new.reset_index().drop_duplicates(subset='date', keep='last').set_index('date')
    weather_data = pd.DataFrame(tdf)
    daily_weather_data = weather_data.resample('1D').mean()
    daily_weather_data = calculate_day_degree(daily_weather_data, 18, 22)
    monthly_weather_data = daily_weather_data.resample('1M').mean()
    days_for_month = None
    if df_new.empty != True:
        days_for_month = df_new.groupby(pd.TimeGrouper(freq='M')).value.count().tolist()
        df_new = df_new.groupby(pd.TimeGrouper(freq='M')).mean()
    else:
        return {
            'companyId': int(company),
            'devices': str(multipliers),
            'timestamps_month': [0],
            'values_month': [0],
            'P50_month': [0],
            'error': "no data obtained",
            'modellingUnitId': modelling_unit,
            '_created': datetime.now()
        }

    # create train and validation sets
    if len(df_new) > 24:
        monthly_data_train = df_new[:-12]
    else:
        monthly_data_train = df_new
    monthly_data_val = df_new[-12:]
    days_for_month = days_for_month[-12:]

    # generate whole training dataset
    training_data = monthly_data_train.merge(monthly_weather_data, left_index=True, right_index=True)
    training_data = training_data.dropna()

    if training_data.empty == True:
        return {
            'companyId': int(company),
            'devices': str(multipliers),
            'timestamps_month': [],
            'values_month': [],
            'P50_month': [],
            'error': "no training data",
            'modellingUnitId': modelling_unit,
            '_created': datetime.now()
        }

    # train models
    try:
        model_heating = train_model(training_data, "HDD", "value")
    except Exception as e:
        return {
            'companyId': int(company),
            'devices': str(multipliers),
            'error': 'model_heating',
            'error_values': str(training_data),
            'error_exception': str(e),
            'modellingUnitId': modelling_unit,
            '_created': datetime.now()
        }
    try:
        model_cooling = train_model(training_data, "CDD", "value")
    except Exception as e:
        return {
            'companyId': int(company),
            'devices': str(multipliers),
            'error': 'model_cooling',
            'error_values': str(training_data),
            'error_exception': str(e),
            'modellingUnitId': modelling_unit,
            '_created': datetime.now()
        }

    # generate validation whole dataset
    validation_data = monthly_data_val.merge(monthly_weather_data, left_index=True, right_index=True)
    # validation_data = validation_data.dropna()

    if len(validation_data.index) <= 1 or len(validation_data) != len(monthly_data_val):
        return {
            'companyId': int(company),
            'devices': str(multipliers),
            'timestamps_month': [],
            'values_month': [],
            'P50_month': [],
            "error": "Weather data not matching",
            'modellingUnitId': modelling_unit,
            '_created': datetime.now()
        }
    else:
        try:
            validation_data['prediction'] = predict_model(validation_data, model_heating, model_cooling, "HDD", "CDD",
                                                          "prediction", 0.1)
        except Exception as e:
            return {
                'companyId': int(company),
                'devices': str(multipliers),
                'error': 'predict_model',
                'error_validation': str(validation_data),
                'error_exception': str(e),
                'error_data': str(training_data),
                'modellingUnitId': modelling_unit,
                '_created': datetime.now()
            }

        try:
            validation_data = fill_mean_values(validation_data, training_data, "prediction", "value")
        except Exception as e:
            return {
                'companyId': int(company),
                'devices': str(multipliers),
                'error': 'fill_mean_values',
                'error_validation': str(validation_data),
                'error_exception': str(e),
                'error_data': str(training_data),
                'modellingUnitId': modelling_unit,
                '_created': datetime.now()
            }

        monthly_baseline = denormalize_data(validation_data, days_for_month, "value", "prediction")

        return {
            'companyId': int(company),
            'devices': str(multipliers),
            'timestamps_month': monthly_baseline.index.tolist(),
            'values_month': monthly_baseline["value"].tolist(),
            'P50_month': [x if x > 0 or np.isnan(x) else 0 for x in monthly_baseline["prediction"].tolist()],
            'modellingUnitId': modelling_unit,
            '_created': datetime.now()
        }

def baseline_calc_pyemis_old(df_new, tdf, model, energy_type, iters=16):
    if model == 'Weekly30Min':
        n = -336
        frequ = 30
    else:
        n = -168
        frequ = 60

        # if None I need a empty dataframe
    if not isinstance(df_new, pd.DataFrame):
        df_new = pd.DataFrame()

    # join dataframes (ALREADY ALIGNED AND CURATED)
    final = df_new.join(tdf)
    final = final.dropna()

    # final lists
    ts_list = []
    value_list = []
    temps_list = []
    for k, value in final.iterrows():
        ts_list.append(k)
        value_list.append(float(value.value))
        temps_list.append(float(value.temperature))

    # calculate model using old pyemis code

    # create numpyarray
    res = []
    for idx, _ in enumerate(ts_list):
        res.append((temps_list[idx], value_list[idx], mktime(ts_list[idx].timetuple())))
    arr = np.array(res, dtype=[('temperature', 'float'), ('consumption', 'float'), ('timestamp', 'float')])

    if model != 'Weekly30Min':
        factory = ConsumptionModels.WeeklyModelFactory(
            ConsumptionModels.AnyModelFactory(models=[ConsumptionModels.ConstantModel]), timedelta(minutes=frequ))
    else:
        if not energy_type or energy_type != 'waterConsumption':
            factory = ConsumptionModels.WeeklyModelFactory(ConsumptionModels.AnyModelFactory(),
                                                           timedelta(minutes=frequ))
        else:
            factory = ConsumptionModels.WeeklyModelFactory(
                ConsumptionModels.AnyModelFactory(models=[ConsumptionModels.ConstantModel]),
                timedelta(minutes=frequ))

    levels = {}
    smileys = []
    prediction = []
    ts_list_final = []
    temps_list_final = []
    value_list_final = []
    for i in xrange(iters, 0, -1):
        # calculo el model amb totes les dades excepte la darrera setmana
        model_fine = True if len(arr[:n * i]) > abs(n) else False
        try:
            Model = factory(arr[:n * i])
        except:
            model_fine = False

        # si el model es correcte continuare
        if model_fine:
            # poso la info que puc calcular
            if i > 1:
                ts_list_final.extend(ts_list[n * i:n * (i - 1)])
                temps_list_final.extend(temps_list[n * i:n * (i - 1)])
                value_list_final.extend(value_list[n * i:n * (i - 1)])
            else:
                ts_list_final.extend(ts_list[n * i:])
                temps_list_final.extend(temps_list[n * i:])
                value_list_final.extend(value_list[n * i:])

            # parameters = Model.parameters()

            # calculo els percentils per aquest model i les seves dades de partida
            percentiles = Model.percentiles(arr[:n * i], [5, 25, 75, 95])

            # calculo la prediccio pel seguent mes que no esta al model
            predict = Model.prediction(arr[n * i:n * (i - 1)]) if i > 1 else Model.prediction(arr[n * i:])
            prediction.extend(predict)
            for key in ['5', '25', '75', '95']:
                try:
                    level_val = (predict[n:] + percentiles[key][n:]).tolist()
                    levels[key].extend(level_val)
                except:
                    level_val = (predict[n:] + percentiles[key][n:]).tolist()
                    levels[key] = level_val

            # smiley faces
            res_model = Model.residuals(arr[:n * i])
            res_last_week = Model.residuals(arr[n * i:n * (i - 1)]) if i > 1 else Model.residuals(arr[n * i:])
            smiley = np.array([percentileofscore(res_model, r) for r in res_last_week])
            smileys.extend(smiley.tolist())

    # nyapa models horaris per evitar els percentils negatius. Revisar models !
    if model != 'Weekly30Min':
        for key in ['5', '25', '75', '95']:
            if key in levels:
                for i, val in enumerate(levels[key]):
                    if val < -10:
                        if i == 0:
                            levels[key][i] = levels[key][i + 1]
                        elif i == len(levels[key]) - 1:
                            levels[key][i] = levels[key][i - 1]
                        else:
                            levels[key][i] = (levels[key][i - 1] + levels[key][i + 1]) / 2

    # save model @ mongo!!
    return {
            'timestamps': ts_list_final,
            'temperatures': temps_list_final,
            'values': value_list_final,
            'P5': levels['5'] if '5' in levels else None,
            'P25': levels['25'] if '25' in levels else None,
            'P75': levels['75'] if '75' in levels else None,
            'P95': levels['95'] if '95' in levels else None,
            'prediction': prediction,
            'smileys': smileys,
        }



def baseline_calc_pyemis_new(df_new, tdf, model, energy_type, iters=16):
    if model == 'Weekly30Min':
        n = -336
        frequ = 30
    else:
        n = -168
        frequ = 60

        # if None I need a empty dataframe
    if not isinstance(df_new, pd.DataFrame):
        df_new = pd.DataFrame()

    # join dataframes (ALREADY ALIGNED AND CURATED)
    final = df_new.join(tdf)
    final = final.dropna()

    # final lists
    ts_list = []
    value_list = []
    temps_list = []
    for k, value in final.iterrows():
        ts_list.append(k)
        value_list.append(float(value.value))
        temps_list.append(float(value.temperature))
    # create numpyarray
    res = []
    for idx, _ in enumerate(ts_list):
        res.append((value_list[idx], temps_list[idx], mktime(ts_list[idx].timetuple())))
    arr = np.array(res, dtype=[('value', 'int32'), ('temperature', 'int32'), ('timestamp', 'float')])

    if model != 'Weekly30Min':
        factory = WF(AF([c()]))
    else:
        if not energy_type or energy_type != 'waterConsumption':
            factory = WF(AF([c(), h3()]))
        else:
            factory = WF(AF([c()]))

    levels = {}
    smileys = []
    prediction = []
    ts_list_final = []
    temps_list_final = []
    value_list_final = []
    for i in xrange(iters, 0, -1):
        # calculo el model amb totes les dades excepte la darrera setmana
        model_fine = True if len(arr[:n * i]) > abs(n) else False
        try:
            Model = factory(arr[:n * i])
        except:
            model_fine = False

        # si el model es correcte continuare
        if model_fine:
            # calculo els percentils per aquest model i les seves dades de partida
            percentiles = {"{}".format(p): Model.scoreatpercentile(arr[:n * i], p) for p in [5, 25, 75, 95]}
            if percentiles:
                # poso la info que puc calcular
                if i > 1:
                    ts_list_final.extend(ts_list[n * i:n * (i - 1)])
                    temps_list_final.extend(temps_list[n * i:n * (i - 1)])
                    value_list_final.extend(value_list[n * i:n * (i - 1)])
                else:
                    ts_list_final.extend(ts_list[n * i:])
                    temps_list_final.extend(temps_list[n * i:])
                    value_list_final.extend(value_list[n * i:])
                for key in ['5', '25', '75', '95']:
                    try:
                        level_val = percentiles[key][n:].tolist()
                        levels[key].extend(level_val)
                    except:
                        level_val = percentiles[key][n:].tolist()
                        levels[key] = level_val

                # calculo la prediccio per la seguent setmana que no esta al model
                predict = Model.prediction(arr[n * i:n * (i - 1)]) if i > 1 else Model.prediction(arr[n * i:])
                prediction.extend(predict)

                # smiley faces
                smiley = Model.percentileofscore(arr[n * i:n * (i - 1)]) if i > 1 else Model.percentileofscore(
                    arr[n * i:])
                smileys.extend(smiley.tolist())

    return {
        'timestamps': ts_list_final,
        'temperatures': temps_list_final,
        'values': value_list_final,
        'P5': levels['5'] if '5' in levels else None,
        'P25': levels['25'] if '25' in levels else None,
        'P75': levels['75'] if '75' in levels else None,
        'P95': levels['95'] if '95' in levels else None,
        'prediction': prediction,
        'smileys': smileys,
    }