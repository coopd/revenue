import pandas as pd
import numpy as np
import os
import glob
import json
from datetime import datetime, timedelta
from datetime import date
from matplotlib.dates import DateFormatter


#transaction read in
transaction_path = "/Volumes/indigobio/Shared/Production/Transaction-Reports/Monthlies/"
csv_files = sorted(glob.glob(os.path.join(transaction_path, "*.csv")))
df_transaction = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)

# External documents
site_to_customer_key = pd.read_csv('/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/site_license_trans_price_key.csv')

cust_assay_comp = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/Cust_Assay_Comp_CAS.csv')

# Rename and reformat 'ds'
df_transaction['ds'] = pd.to_datetime(df_transaction['processed_at'], infer_datetime_format=True)
df_transaction.loc[:, 'ds'] = df_transaction['ds'].dt.tz_localize(None)

df_transaction.loc[:, 'ds'] = [datetime.date(x) for x in df_transaction['ds']]
#df_transaction['ds'] = pd.to_datetime(df_transaction['ds'], infer_datetime_format=True)

df_transaction = df_transaction[(df_transaction.ds > date(2020, 12, 31))]
df_transaction = df_transaction[(df_transaction.ds < date(2022, 8, 1))]
df_transaction['ds'] = pd.to_datetime(df_transaction['ds'], infer_datetime_format=True)

df_transaction['year'] = [x.timetuple().tm_year for x in df_transaction['ds']]
df_transaction['month'] = [x.timetuple().tm_mon for x in df_transaction['ds']]

# create the transaction with grouping for sample sum, chromatogram sum
df_transaction = df_transaction.groupby(['year', 'month', 'site_name', 'assay_name'], as_index=False).agg({'sample_count': 'sum', 'chromatogram_count':'sum'})
df_transaction = df_transaction.fillna(0).reset_index(drop=True)

df_trans_cust = pd.merge(df_transaction, site_to_customer_key, on='site_name', how='left')
df_large_cust = df_trans_cust[df_trans_cust['entity'].isin(['Millennium_Health','Quest','LabCorp'])]

df_large_cust = df_large_cust.groupby(['year','entity', 'customer', 'assay_name'], as_index=False).agg({'sample_count': 'mean', 'chromatogram_count':'mean'})
df_large_cust = df_large_cust.sort_values(by=['customer', 'assay_name', 'year', 'sample_count'])

df_large_cust.to_csv('assay_volume_for_reimb.csv')