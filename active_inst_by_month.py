import pandas as pd
import os
import glob
from datetime import datetime
from datetime import date
pd.options.mode.chained_assignment = None


# EXTERNAL RESOURCE READ IN ----------------------------------------------------------------------------------------

# transaction read in
transaction_path = "/Volumes/indigobio/Shared/Production/Transaction-Reports/Monthlies/"
csv_files = sorted(glob.glob(os.path.join(transaction_path, "*.csv")))
df_transaction = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)

# External documents
inst_count = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/simple_inst_count.csv')

site_to_customer_key = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/site_license_trans_price_key.csv')
#inst_list = pd.read_csv('/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/instrument_sn.csv')
inst_list = pd.read_csv('/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/cust_inst_sn.csv')

cust_assay_comp = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/Cust_Assay_Comp_CAS.csv')


# TRANSACTION PROCESSING ---------------------------------------------------

# Rename and reformat 'ds'
df_transaction['ds'] = pd.to_datetime(df_transaction['processed_at'], infer_datetime_format=True)
df_transaction.loc[:, 'ds'] = df_transaction['ds'].dt.tz_localize(None)

# filter to only the sites in csv
site_list = site_to_customer_key['site_name'].tolist()
df_transaction = df_transaction.loc[(df_transaction['site_name'].isin(site_list))]

# Convert the date formatting to daily, sum the samples/chromatogram to correspond
df_transaction.loc[:, 'ds'] = [datetime.date(x) for x in df_transaction['ds']]

# create the transaction with grouping for sample sum, chromatogram sum
df_transaction = df_transaction.groupby(['site_name', 'instrument_name', 'batch_name', 'batch_id', 'assay_name', 'ds'],
                                        as_index=False).agg({'sample_count': 'sum', 'chromatogram_count': 'sum'})
df_transaction = df_transaction.fillna(0).reset_index(drop=True)

# filter less than 3 records, prophet only works for 2+ records by group
# df_transaction = df_transaction.groupby('ds').filter(lambda x: len(x) > 2)
df_transaction = df_transaction.sort_values(by=['ds'])
df_transaction = df_transaction[(df_transaction.ds > date(2019, 12, 31))]
df_transaction['ds'] = pd.to_datetime(df_transaction['ds'], infer_datetime_format=True)

df_transaction['year'] = [x.timetuple().tm_year for x in df_transaction['ds']]
df_transaction['month'] = [x.timetuple().tm_mon for x in df_transaction['ds']]
# ONLY LICENSED INSTRUMENTS -----------------------------------------------------------------------------------

inst_sn = inst_list[inst_list['serial_number'] != 'Unknown']

# Get active inst per month ------------------------

customer_trans = pd.merge(site_to_customer_key, df_transaction, on=['site_name'], how='left')

inst_count_per_month = customer_trans.groupby(['year', 'month', 'entity',  'customer'], as_index=False).agg(
    {'instrument_name': 'nunique', 'assay_name': 'nunique'})
inst_count_per_month.rename(columns = {'instrument_name': 'monthly_inst_count', 'assay_name': 'monthly_assay_count'}, inplace = True)

inst_trans_sum = customer_trans.groupby(['year', 'month', 'entity',  'customer', 'instrument_name'], as_index=False).agg(
        {'sample_count': 'sum', 'chromatogram_count': 'sum'})

staging_inst = pd.merge(inst_trans_sum, inst_count_per_month, on=['year', 'month', 'entity',  'customer'], how='left')
staging_inst = staging_inst.sort_values(by=['customer', 'year', 'month'])

#active_inst_per_month = pd.merge(staging_inst, inst_list, on=['entity',  'customer', 'instrument_name'], how='left')
#active_inst_per_month = active_inst_per_month.sort_values(by=['customer', 'year', 'month'])

#active_inst_per_month = active_inst_per_month.drop_duplicates()

staging_inst.to_csv('active_per_month.csv')
