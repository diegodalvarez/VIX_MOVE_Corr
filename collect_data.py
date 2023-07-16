# -*- coding: utf-8 -*-
"""

@author: Diego Alvarez
"""

import os
import ssl
import pdblp
import pandas as pd
import yfinance as yf
import datetime as dt
import pandas_datareader as web


# Path Management
parent_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
data_path = os.path.join(parent_path, "data")
out_path = os.path.join(parent_path, "out")

if os.path.exists(data_path) == False: os.makedirs(data_path)
if os.path.exists(out_path) == False: os.makedirs(out_path)

# Background Data
fred_tickers = ["DGS1MO", "DGS3MO", "DGS1", "DGS2", "DGS3", "DGS7", "DGS10", "DGS20", "DGS30"]
yf_tickers = ["^MOVE", "^VIX"]
corr_tickers = ["COR1M Index"]
skew_tickers = ["SKEW Index"]

start_date = dt.date(year = 2000, month = 1, day = 1)
end_date = dt.date(year = 2023, month = 5, day = 30)

# Collect Treasuries
def collect_treasuries(
    fred_tickers: list,
    start_date: dt.datetime,
    end_date: dt.datetime,
    data_path: os.path) -> pd.DataFrame:
    
    fred_path = os.path.join(data_path, "fred.parquet")
    
    try:
        
        print("[INFO] Trying to read Treasury data")
        df_out = (pd.read_parquet(
            path = fred_path,
            engine = "pyarrow"))
        print("[INFO] Treasury Data Found")
        
    except:
        
        print("[INFO] Collecting Treasury Data")
        df_fred = (web.DataReader(
            fred_tickers,
            "fred",
            start = start_date,
            end = end_date))
        
        df_out = (df_fred.reset_index().rename(
            columns = {"DATE": "date"}).
            melt(id_vars = "date").dropna())
        
        df_out.to_parquet(
            path = fred_path,
            engine = "pyarrow")
        
        print("[INFO] Treasury Data Collected and saved to {}".format(
            fred_path))
        
    return df_out

# Collect VIX and MOVE
def collect_vols(
    yf_tickers: list,
    start_date: dt.datetime,
    end_date: dt.datetime,
    data_path: os.path) -> pd.DataFrame:
    
    yf_path = os.path.join(data_path, "yf.parquet")
    
    try:
        
        print("[INFO] Trying to collect Volatility Data")
        df_out = (pd.read_parquet(
            path = yf_path,
            engine = "pyarrow"))
        print("[INFO] Volatality Data Found")
        
    except:
        
        print("[INFO] Collecting Volatility Data")
        df_yf = (yf.download(
            tickers = yf_tickers,
            start = start_date,
            end = end_date))
        
        df_out = (df_yf[
            "Close"].
            reset_index().
            rename(columns = {
                "^MOVE": "MOVE",
                "^VIX": "VIX",
                "Date": "date"}).
            melt(id_vars = "date").
            dropna())
        
        df_out.to_parquet(
            path = yf_path,
            engine = "pyarrow")
        
        print("[INFO] Colleted Volatility and saved to {}".format(
            yf_path))
        
    return df_out

# collect corr data
def collect_corr(
    tickers: list,
    start_date: dt.datetime,
    end_date: dt.datetime,
    data_path: os.path) -> pd.DataFrame: 
    
    corr_path = os.path.join(data_path, "corr.parquet")

    try:
        
        print("[INFO] Trying to collect CORR data")
        df_test = pd.read_parquet(
            path = corr_path,
            engine = "pyarrow")
        print("[INFO] CORR Data Found")
    
    except: 

        print("[INFO] Collecting CORR data")
        end_date_input  = end_date.strftime("%Y%m%d")
        start_date_input = start_date.strftime("%Y%m%d")
        
        con = pdblp.BCon(debug = False, port = 8194, timeout = 5_000)
        con.start()
        
        df_tmp = (con.bdh(
            tickers = tickers,
            flds = ["PX_LAST"],
            start_date = start_date_input,
            end_date = end_date_input).
            reset_index().
            melt(id_vars = "date"))
        
        (df_tmp.to_parquet(
            path = corr_path,
            engine = "pyarrow"))
        
        print("[INFO] Collected CORR data and saved to {}".format(
            corr_path))
        
def collect_spx_names(
    start_date: dt.datetime,
    end_date: dt.datetime,
    data_path: os.path) -> pd.DataFrame:
    
    spx_patch = os.path.join(data_path, "spx_names.parquet")
    
    try:
        
        print("[INFO] Trying to collect SPX Single Names Data")
        df_out = (pd.read_parquet(
            path = spx_patch,
            engine = "pyarrow"))
        print("[INFO] SPX Single Names Data Found")
        
    except:
        
        ssl._create_default_https_context = ssl._create_unverified_context
        tickers = (pd.read_html(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
            [0]["Symbol"].
            drop_duplicates().
            to_list())
        
        print("[INFO] Collecting SPX Single Names Data")
        df_yf = (yf.download(
            tickers = tickers,
            start = start_date,
            end = end_date))
        
        df_out = (df_yf[
            "Close"].
            reset_index().
            rename(columns = {"Date": "date"}).
            melt(id_vars = "date").
            dropna())
        
        df_out.to_parquet(
            path = spx_patch,
            engine = "pyarrow")
        
        print("[INFO] Colleted SPX Single Names and saved to {}".format(
            spx_patch))
        
# collect corr data
def collect_skew(
    tickers: list,
    start_date: dt.datetime,
    end_date: dt.datetime,
    data_path: os.path) -> pd.DataFrame: 
    
    skew_path = os.path.join(data_path, "skew.parquet")

    try:
        
        print("[INFO] Trying to collect CORR data")
        df_test = pd.read_parquet(
            path = skew_path,
            engine = "pyarrow")
        print("[INFO] CORR Data Found")
    
    except: 

        print("[INFO] Collecting Skew data")
        end_date_input  = end_date.strftime("%Y%m%d")
        start_date_input = start_date.strftime("%Y%m%d")
        
        con = pdblp.BCon(debug = False, port = 8194, timeout = 5_000)
        con.start()
        
        df_tmp = (con.bdh(
            tickers = tickers,
            flds = ["PX_LAST"],
            start_date = start_date_input,
            end_date = end_date_input).
            reset_index().
            melt(id_vars = "date"))
        
        (df_tmp.to_parquet(
            path = skew_path,
            engine = "pyarrow"))
        
        print("[INFO] Collected CORR data and saved to {}".format(
            skew_path))
        
if __name__ == "__main__":

    #collect_vols(yf_tickers, start_date, end_date, data_path)
    #collect_treasuries(fred_tickers, start_date, end_date, data_path)
    #collect_corr(corr_tickers, start_date, end_date, data_path)
    #collect_spx_names(start_date, end_date, data_path)
    collect_skew(skew_tickers, start_date, end_date, data_path)
    
    