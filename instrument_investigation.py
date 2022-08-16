import pandas as pd
import os
import glob
from datetime import datetime
from datetime import date

#transaction read in
transaction_path = "/Volumes/indigobio/Shared/Production/Transaction-Reports/Monthlies/"
csv_files = sorted(glob.glob(os.path.join(transaction_path, "*.csv")))
df_transaction = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)

# External documents
site_to_customer_key = pd.read_csv('/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/site_license_trans_price_key.csv')
serial_numbers = pd.read_csv('/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/inst_sn_master.csv')

# Rename and reformat 'ds'
df_transaction['ds'] = pd.to_datetime(df_transaction['processed_at'], infer_datetime_format=True)
df_transaction.loc[:, 'ds'] = df_transaction['ds'].dt.tz_localize(None)

# Convert the date formatting to daily, sum the samples/chromatogram to correspond
df_transaction.loc[:, 'ds'] = [datetime.date(x) for x in df_transaction['ds']]
df_transaction = df_transaction.sort_values(by=['ds'])
df_transaction = df_transaction[(df_transaction.ds > date(2022, 6, 30))]
df_transaction = df_transaction[(df_transaction.ds < date(2022, 8, 1))]
df_transaction['ds'] = pd.to_datetime(df_transaction['ds'], infer_datetime_format=True)

# filter to only the active sites list (list made pre-June '22)
#active_site_list = site_to_customer_key['site_name'].tolist()
#df_transaction = df_transaction.loc[(df_transaction['site_name'].isin(active_site_list))]

df_transaction = df_transaction[["site_name", "instrument_name"]]
df_transaction = df_transaction.drop_duplicates()

# get entity and customer for differentiating the identical inst names
df_trans_cust = pd.merge(df_transaction, site_to_customer_key, on='site_name', how='left')

# join up the current serial numbers
df_trans_sn = pd.merge(df_trans_cust, serial_numbers, on=['entity','customer','instrument_name'], how='left').fillna(0)
df_trans_sn = df_trans_sn[['entity','customer','site_name','instrument_name','serial_number','license','unit','price_per_trans']]
df_trans_sn = df_trans_sn.sort_values(by=['site_name','entity','customer','instrument_name','serial_number'])

df_trans_sn.to_csv('Active_Inst_w_Available_SN_(July_22).csv')

