import pytz
from rpy2 import robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.packages import importr
import rpy2.robjects.packages as rpackages

def set_r_environment(filename):
    """
        Function to set up the R environment
        :param dataframe:
        :return:
    """
    utils = rpackages.importr('utils')
    utils.chooseCRANmirror(ind=1)
    base = importr('base')
    base.source(filename)
    pandas2ri.activate()
    robjects.globalenv['load_libraries']()

def train_gaussian_mixture_model(dataframe, value_column, k, timezone):
    """
    Wrapper function to function implemented in R
    :param dataframe:
    :return:
    """

    dataframe["time"] = dataframe.index
    dataframe.to_csv("test.csv")
    model = robjects.globalenv['GaussianMixtureModel_clustering'](dataframe, value_column=value_column, k=k, tz=timezone)
    return model

def predict_gaussian_mixture_model(model, dataframe, value_column, timezone):
    """
    Wrapper function to function implemented in R
    :param dataframe:
    :return:
    """
    dataframe["time"] = dataframe.index
    result = robjects.globalenv['GaussianMixtureModel_prediction'](model, dataframe, value_column, timezone)
    return result


def prepare_dataframe(model, dataframe, value_column, n_lags, lat=None, lon=None, timezone="UTC", str_val=None):
    """
        Wrapper function to function implemented in R
        :param dataframe:
        :return:
    """
    dataframe["time"] = dataframe.index
    result = robjects.globalenv['prepare_dataframe'](model, dataframe, value_column, n_lags, lat, lon, timezone=timezone)
    return result


def train_gam(model, type, dataframe, value_column, by_s, n_max=0, m_max=0):
    """
        Wrapper function to function implemented in R
        :param dataframe:
        :return:
    """
    dataframe["time"] = dataframe.index
    x = robjects.globalenv['train_gam'](model, type, dataframe, value_column, by_s, n_max, m_max)
    return x


def clean_linear(model):
    """
        Wrapper function to function implemented in R
        :param dataframe:
        :return:
    """
    x = robjects.globalenv['clean_linear'](model)
    return x

def train_linear(model, type, dataframe, value_column, by_s, n_max=0, m_max=0):
    """
        Wrapper function to function implemented in R
        :param dataframe:
        :return:
    """
    dataframe["time"] = dataframe.index
    x = robjects.globalenv['train_linear'](model, type, dataframe, value_column, by_s, n_max, m_max)
    return x


def predict_model(model, dataframe, by_s=False):
    """
        Wrapper function to function implemented in R
        :param dataframe:
        :return:
    """
    dataframe["time"] = dataframe.index
    if by_s:
        return robjects.globalenv['predict_mod_by_s'](model, dataframe)
    else:
        return robjects.globalenv['predict_mod_all'](model, dataframe)

