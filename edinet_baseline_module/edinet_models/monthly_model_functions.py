from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import statsmodels.api as sm
from calendar import monthrange


def calculate_day_degree(daily_weather_data, base_temp_heating, base_temp_cooling):
    """
    returns the desired type of day degree for the weather data.
    :param daily_weather_data: dataframe with daily data and 'temperature' column
    :param base_temp: the base temp to calculate the day degree
    :return: a dataframe with new HDD and CDD columns with the day degree data
    """
    daily_weather_data["HDD"] = base_temp_heating - daily_weather_data.temperature
    daily_weather_data.HDD[daily_weather_data.HDD < 0] = 0
    daily_weather_data["CDD"] = daily_weather_data.temperature - base_temp_cooling
    daily_weather_data.CDD[daily_weather_data.CDD < 0] = 0
    return daily_weather_data

def train_model(data, day_degree_column, value_column, plot=False):
    data_affected = data[data[day_degree_column] > 0]
    if len(data_affected.index) <= 1:
        return None
    model = sm.OLS(data_affected[value_column], sm.add_constant(data_affected[day_degree_column]))
    fit_model = model.fit()
    return fit_model

def predict_model(data, model_1, model_2, day_degree_column_1, day_degree_column_2, prediction_column, significance):
    significance_models = [False, False]
    data[prediction_column] = np.nan
    if model_1 and model_1.pvalues[day_degree_column_1] < significance and model_1.params[day_degree_column_1] > 0:
        print("has significance with {}".format(day_degree_column_1))
        significance_models[0] = True
        data_affected_1 = data[(data[day_degree_column_1] > 0)]
        prediction = model_1.predict(sm.add_constant(data_affected_1[day_degree_column_1]))
        data[prediction_column] = prediction
    else:
        print("NO significance with {}".format(day_degree_column_1))

    if model_2 and model_2.pvalues[day_degree_column_2] < significance and model_2.params[day_degree_column_2] > 0:
        print("has significance with {}".format(day_degree_column_2))
        significance_models[1] = True
        data_affected_2 = data[(data[day_degree_column_2] > 0)]
        prediction = model_2.predict(sm.add_constant(data_affected_2[day_degree_column_2]))
        data[prediction_column] = prediction
    else:
        print("NO significance with {}".format(day_degree_column_2))

    if all(significance_models):
        data_affected_3 = data[(data[day_degree_column_1] > 0) & (data[day_degree_column_2] > 0)]
        baseload_1 = model_1.params['const']
        baseload_2 = model_2.params['const']
        pred_mod_1 = model_1.predict(sm.add_constant(data_affected_3[day_degree_column_1]))
        pred_mod_2 = model_2.predict(sm.add_constant(data_affected_3[day_degree_column_2]))
        pred_mod_1 = pred_mod_1 - baseload_1
        pred_mod_2 = pred_mod_2 - baseload_2
        data[prediction_column] = np.mean([baseload_1,baseload_2]) + pred_mod_1 + pred_mod_2
    return data[prediction_column]

def fill_mean_values(data, historic_data, prediction_column, value_column ):
    no_model_months = data[prediction_column][data[prediction_column].isnull()].index.month
    for m in no_model_months:
        month_data = historic_data[historic_data.index.month == m][value_column]
        if len(month_data) <= 1:
            data[prediction_column][data.index.month == m] = np.nan
        else:
            data[prediction_column][data.index.month == m] = month_data.mean()
    return data

def denormalize_data(data, days_for_month, value_column, prediction_column):
    data[value_column] = data[value_column] * days_for_month
    data[prediction_column] = data[prediction_column] * days_for_month
    return data

def plot_model(data, value_column, prediction_column):
    plt.figure(figsize=(15, 4))
    plt.plot(data.index, data[value_column], color="black")
    plt.plot(data.index, data[prediction_column], color="blue")

def plot_model_fitting(model, training_data, day_degree_column, value_column ):
    data_affected = training_data[training_data[day_degree_column] > 0]
    ax = data_affected.plot(x=day_degree_column, y=value_column, kind='scatter')
    ax.plot(data_affected[day_degree_column], model.predict(sm.add_constant(data_affected[day_degree_column])))
