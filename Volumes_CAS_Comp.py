import pandas as pd
import os
import glob
import re
from datetime import datetime
import json


# -------- FUNCTIONS ----------
# We need to know how many dupes are dropping off each read in
# You need to do the re.match() for looking

def combine(file_list):
    combined = []
    total_compound_validation = []
    cas_join_validation = []
    dropped_dupes = []
    failed_assay_names = []
    ist_assay_comp = []
    for file_name in file_list:

        active_site_list = site_to_customer_key['site_name'].tolist()
        site_string = file_name[98:-5]

        for site in active_site_list:
            # print(site)
            m = re.search("%s" % site, site_string)
            if not m:
                # print('fail')
                continue
            else:
                site_from_file = m.group()
                # print('Site Name Found: Pass')
                break

        translate = open(file_name)
        data = json.load(translate)
        failed_assay = dict(name='PASS, PASS', compounds='PASS')
        assay_site = (data['name'] + "," + site_from_file)
        compound_method = pd.DataFrame(data["compound_methods"])
        df_compound = pd.DataFrame(compound_method['name'])

        ist_check = pd.merge(df_compound, ist_compounds, how='inner', left_on='name', right_on='compounds')
        cascheck1 = pd.merge(df_compound, compound_cas_key, how='inner', left_on='name', right_on='compounds')
        cascheck2 = cascheck1.drop_duplicates(subset='CAS')
        cas_dupe_count = cascheck1[cascheck1.duplicated(subset='CAS')]

        non_ist_compounds = pd.merge(df_compound, ist_check, how='left', left_on='name', right_on='compounds',
                                     indicator=True)
        non_ist_compounds = non_ist_compounds.loc[(non_ist_compounds['_merge'] == 'left_only')].reset_index(drop=True)
        non_ist_compounds = non_ist_compounds.drop(['_merge', 'name_y', 'compounds', 'CAS'], axis=1)

        missing_compounds_staging = pd.merge(non_ist_compounds, cascheck1, how='left', left_on='name_x',
                                             right_on='compounds', indicator=True)
        missing_compounds_staging = missing_compounds_staging.loc[
            (missing_compounds_staging['_merge'] == 'left_only')].reset_index(drop=True)
        missing_compounds_staging = missing_compounds_staging.drop(['_merge', 'name_x', 'CAS'], axis=1)
        missing_compounds_staging = missing_compounds_staging.dropna()

        # Convert compounds to a list so we can have a dict of lists
        list_compound = cascheck2['compounds'].tolist()
        list_total_compounds = df_compound['name'].tolist()
        list_cas_join_compounds = cascheck1['compounds'].tolist()
        list_dupe_compounds = cas_dupe_count['compounds'].tolist()
        list_missing_compounds = missing_compounds_staging['compounds'].tolist()
        list_ist_compound = ist_check['compounds'].tolist()

        if len(missing_compounds_staging.compounds) != 0:
            failed_assay = dict(name=assay_site, compounds=list_missing_compounds)
            print(failed_assay)

        # Create dictionaries of the assay/site:compounds
        dict_compound_cas = dict(name=assay_site, compounds=list_compound)
        dict_unfilter_json_comps = dict(name=assay_site, compounds=list_total_compounds)
        dict_cas_join_validation = dict(name=assay_site, compounds=list_cas_join_compounds)
        dict_dupe_compounds = dict(name=assay_site, compounds=list_dupe_compounds)
        dict_ist_compounds = dict(name=assay_site, compounds=list_ist_compound)

        # Append iterations to the empty variables
        combined.append(dict_compound_cas)
        total_compound_validation.append(dict_unfilter_json_comps)
        cas_join_validation.append(dict_cas_join_validation)
        dropped_dupes.append(dict_dupe_compounds)
        failed_assay_names.append(failed_assay)
        ist_assay_comp.append(dict_ist_compounds)

    return combined, total_compound_validation, cas_join_validation, dropped_dupes, failed_assay_names, ist_assay_comp


# Function for exploding the function output

def break_down(compound_file):
    df_of_compound = pd.DataFrame(compound_file)
    df_of_compound = df_of_compound.explode('compounds').reset_index(drop=True)
    df_of_compound[['assay_name', 'site_name']] = df_of_compound.name.str.split(",", expand=True, )
    df_of_compound = df_of_compound.dropna()
    df_of_compound = df_of_compound.drop('name', axis=1)
    return df_of_compound


def missing_compounds_test(x):
    x = x.loc[(x['compounds'] != 'PASS')].reset_index(drop=True)
    if len(x) == 0:
        print('No Missing Compound Test: Pass')
    else:
        print('No Missing Compound Test: Fail')


# Function for quick left join only eval to see missing values
def left_join_comparison(x, y):
    df_join = pd.merge(x, y, how='left', on=['assay_name', 'site_name'], indicator=True)
    df_join = df_join.loc[(df_join['_merge'] == 'left_only')].reset_index(drop=True)
    return df_join


# -------- DEPENDANCY FILES -------------


# External csv's
# site_to_customer_key = pd.read_csv('/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/site_name_key.csv').fillna('Remainder')
site_to_customer_key = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/site_license_trans_price_key.csv')
compound_cas_key = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Compound_to_CAS/Compound_to_CAS(Latest).csv')
ist_compounds = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Compound_to_CAS/compound_to_ist.csv')

path = "/Volumes/indigobio/Shared/Production/Transaction-Reports/Monthlies/"
csv_files = sorted(glob.glob(os.path.join(path, "*.csv")))

df_transaction = pd.DataFrame(columns=['site_name', 'processed_at', 'instrument_name', 'batch_name',
                                       'batch_id', 'assay_name', 'assay_id', 'sample_count',
                                       'determination_count', 'chromatogram_count', 'total_db_determinations',
                                       'total_db_size_in_mb', 'system'])

# ------- df_transaction Processing ---------------------

for file in csv_files:
    m = re.search(r'indigoascent4', file)
    if m:
        tf = pd.read_csv(file)
        tf['system'] = "S4"
    else:
        m = re.search(r'indigoarq1', file)
        if m:
            tf = pd.read_csv(file)
            tf['system'] = "ARQ"
        else:
            tf = pd.read_csv(file)
            tf['system'] = "S3"
    df_transaction = pd.concat([df_transaction, tf], ignore_index=True)

# One time functions - convert processed to ds, eliminate the non-active sites
df_transaction['ds'] = pd.to_datetime(df_transaction['processed_at'], infer_datetime_format=True, utc=False)

active_site_list = site_to_customer_key['site_name'].tolist()
df_transaction = df_transaction.loc[(df_transaction['site_name'].isin(active_site_list))]

df_transaction.loc[:, 'ds'] = [datetime.date(x) for x in df_transaction['ds']]
# df_transaction['year'] = [x.timetuple().tm_year for x in df_transaction['ds']]
# df_transaction['month'] = [x.timetuple().tm_mon for x in df_transaction['ds']]
# df['year_day'] = [x.timetuple().tm_yday for x in df['date_time']]

# df = df.drop(columns=['total_db_determinations', 'total_db_size_in_mb', 'determination_count'])

# Convert the date formatting to daily, sum the samples/chromatogram to correspond
# df_transaction.loc[:, 'ds'] = [datetime.date(x) for x in df_transaction['ds']]

# create the transaction with grouping for sample sum, chromatogram sum
df_transaction = df_transaction.groupby(['ds', 'site_name', 'instrument_name', 'assay_name'], as_index=False).agg(
    {'sample_count': 'sum', 'chromatogram_count': 'sum'})
df_transaction = df_transaction.fillna(0).reset_index(drop=True)

# filter less than 3 records, prophet only works for 2+ records by group
# df_transaction = df_transaction.groupby('ds').filter(lambda x: len(x) > 2)
df_transaction = df_transaction.sort_values(by=['ds'])
startdate = pd.to_datetime("2020-1-1").date()
df_transaction = df_transaction[(df_transaction.ds >= startdate)]
df_transaction['ds'] = pd.to_datetime(df_transaction['ds'], infer_datetime_format=True)

# ----- Merge with Customer, compound, transaction etc ----------------

df_customer_trans = pd.merge(df_transaction, site_to_customer_key, how='left', on='site_name')

# ------- JSON Compounds -----------------

# Set paths and read S3/S4 Assays
path1 = "/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Compounds/assays-last-6-months"
json_file_names1 = sorted(glob.glob(os.path.join(path1, "*.json")))

# List comprehension for combining files
json_list = [y for x in [json_file_names1] for y in x]

# Call to function
compound_to_CAS, total_compounds, pre_dupe_CAS_compounds, dropped_duplicates, failed_assay_site, ist_assay_site = combine(
    json_list)

# Break down all the dictionaries into df's
df_total_compound = break_down(total_compounds)
df_pre_dupe_CAS_compounds = break_down(pre_dupe_CAS_compounds)
df_ist_site_assay = break_down(ist_assay_site)
df_dropped_compounds = break_down(dropped_duplicates)
df_failed_assay_site = break_down(failed_assay_site)
compound_site_assay = break_down(compound_to_CAS)

filtered_comp_site = pd.merge(compound_site_assay, ist_compounds, on='compounds', indicator=True, how='outer').query(
    '_merge=="left_only"').drop('_merge', axis=1)
filtered_CAS_staging = pd.merge(filtered_comp_site, compound_cas_key, how='inner', on='compounds')
filtered_CAS_to_compound = pd.merge(filtered_CAS_staging, site_to_customer_key, how='left', on='site_name')

filtered_CAS_to_compound = filtered_CAS_to_compound.drop('CAS_x', axis=1)
filtered_CAS_to_compound.rename(columns={'CAS_y': 'CAS'}, inplace=True)
# filtered_CAS_master = filtered_CAS_to_compound.copy()
# filtered_CAS_master = filtered_CAS_master.drop(['compounds'], axis=1)

# This code below drops 122 duplicated rows
# filtered_CAS_master = filtered_CAS_master.drop_duplicates()
filtered_CAS_to_compound = filtered_CAS_to_compound.drop_duplicates()
# filtered_CAS_to_compound.to_csv('filtered_cas_compound_key.csv')

CAS_count = filtered_CAS_to_compound.groupby(['site_name', 'assay_name'])['CAS'].count()
# CAS_count = pd.DataFrame(CAS_count).reset_index()
# CAS_count = CAS_count.drop(['index'], axis=1)

filtered_CAS_master = pd.merge(filtered_CAS_to_compound, CAS_count, how='left', on=['site_name', 'assay_name'])
filtered_CAS_master = filtered_CAS_master.rename(columns={'CAS_y': 'compound_count_per_assay', 'CAS_x': 'CAS'})

# This code below drops 122 duplicated rows
filtered_CAS_master = filtered_CAS_master.drop_duplicates()
# filtered_CAS_master.to_csv('CAS_Cust_Only.csv')

# Final merge of CAS and Customer_Transaction
# no duplicates, 9990101 rows
combined_both = pd.merge(df_customer_trans, filtered_CAS_master, how='left',
                         on=['site_name', 'assay_name', 'entity', 'customer'])

# Create the rolled down version of sample/chromatogram per compound
combined_both['processed_sample_count'] = combined_both['sample_count'] / combined_both['compound_count_per_assay']
combined_both['processed_chromatogram_count'] = combined_both['chromatogram_count'] / combined_both[
    'compound_count_per_assay']

# Replace the NaN's for sites missing compounds with the original Sample/Chromatogram
combined_both['processed_sample_count'] = combined_both['processed_sample_count'].fillna(combined_both['sample_count'])
combined_both['processed_chromatogram_count'] = combined_both['processed_chromatogram_count'].fillna(
    combined_both['chromatogram_count'])

combined_both = combined_both.drop(['sample_count', 'chromatogram_count'], axis=1)
combined_both.to_csv('df_trans_CAS_Update.csv')

# -------- Get Cust_Assay_Comp_CAS.csv for the revenue documents

# assay count for dividing inst price into price per sample, per assay
assay_count = filtered_CAS_to_compound.groupby('customer')['assay_name'].nunique()
filtered_assay_count = pd.merge(filtered_CAS_to_compound, assay_count, how='left', on='customer')
filtered_assay_count = filtered_assay_count.rename(columns={'assay_name_y': 'assay_count_per_site', 'assay_name_x':'assay_name'})


# compound count for dividing inst price into price per sample, per compound
CAS_count = filtered_CAS_to_compound.groupby(['customer', 'assay_name'])['CAS'].nunique()
filtered_CAS_master = pd.merge(filtered_assay_count, CAS_count, how='left', on=['customer', 'assay_name'])
filtered_CAS_master = filtered_CAS_master.rename(columns={'CAS_y': 'compound_count_per_assay', 'CAS_x':'CAS'})

filtered_CAS_master['assay_x_comp_count'] = filtered_CAS_master['assay_count_per_site']*filtered_CAS_master['compound_count_per_assay']

# This code below drops 122 duplicated rows
filtered_CAS_master = filtered_CAS_master.drop_duplicates()

filtered_CAS_master = filtered_CAS_master.groupby(['customer', 'assay_name', 'compounds', 'CAS'], as_index=False).agg({'assay_x_comp_count':'sum'})

filtered_CAS_master.to_csv('Cust_Assay_Comp_CAS.csv')
