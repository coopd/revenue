import pandas as pd
import os
import glob
from datetime import datetime
from datetime import date
pd.options.mode.chained_assignment = None



#transaction read in

transaction_path = "/Volumes/indigobio/Shared/Production/Transaction-Reports/Monthlies/"
csv_files = sorted(glob.glob(os.path.join(transaction_path, "*.csv")))
df_transaction = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)

site_to_customer_key = pd.read_csv('/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/site_license_trans_price_key.csv')
inst_list = pd.read_csv('/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/cust_inst_sn.csv')


# Rename and reformat 'ds'
df_transaction['ds'] = pd.to_datetime(df_transaction['processed_at'], infer_datetime_format=True)
df_transaction.loc[:, 'ds'] = df_transaction['ds'].dt.tz_localize(None)

# filter to only the active sites
active_site_list = site_to_customer_key['site_name'].tolist()
df_transaction = df_transaction.loc[(df_transaction['site_name'].isin(active_site_list))]

# Convert the date formatting to daily, sum the samples/chromatogram to correspond
df_transaction.loc[:, 'ds'] = [datetime.date(x) for x in df_transaction['ds']]

df_transaction['ds'] = pd.to_datetime(df_transaction['ds'], infer_datetime_format=True)


filtered_df = pd.merge(site_to_customer_key, df_transaction, on=['site_name'], how='left')
result = filtered_df.groupby(['instrument_name','customer'], as_index=False)['ds'].min()
#minimums = result.to_frame()
#minimums = minimums.reset_index()

#inst_sn = inst_list[inst_list['serial_number'] != 'Unknown']
serial_df = pd.merge(inst_list, result, on=['instrument_name', 'customer'], how='left')
serial_df = serial_df.drop_duplicates()
serial_df = serial_df.fillna(0)
serial_df.to_csv('inst_sn_w_date.csv')
