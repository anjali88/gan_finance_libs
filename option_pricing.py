# Options pricing 
import datetime as dt
import numpy as np
import pandas as pd

# General functions

def compute_payoff(strike_price, expiration_price, option_type):
    """Function returning the payoff of a european call or put.
    For a call option: If the price at expiration is below the strike price, the call is worth 0.
    Else the call is worth strike_price - expiration_price.
    For a put option: If the price strike_price at expiration is above the strike price, the put is worth 0.
    Else the put is worth expiration_price - strike_price
    """
    if option_type == "call":
        return(max(0, strike_price - expiration_price))
    elif option_type == "put":
        return(max(0, expiration_price - strike_price))

# Specific to the brownian movement method
def get_drift_std(return_series):
    """Function returning characteristics of the time series: 
    drift, standard  deviation and volatility
    """
    u = return_series.mean()
    var = return_series.var()
    std = np.sqrt(var)
    drift = u - 0.5 * var
    return(drift, std)

def get_history(apinode_client, ticker):
    price_history = apinode_client.sql_query(
        endpoint_id = "_get_price_history",
        parameters = {"ticker": ticker}
    ).get("results", {})[0]
    price_history_column_names = [i["name"] for i in price_history.get("columns", [])]
    price_history_df = pd.DataFrame(
        data = price_history.get("rows", []), 
        columns = price_history_column_names
    )
    price_history_df = price_history_df.astype(dict(
        zip(price_history_column_names, [str, np.float])))
    price_history_df.dropna(inplace = True)
    price_history_df["log_returns"] = np.log(
        1 + price_history_df["adj_close"].pct_change())
    return(price_history, price_history_df)
