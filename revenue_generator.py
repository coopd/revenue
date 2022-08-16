import pandas as pd
import os
import glob
from datetime import datetime
from datetime import date
import numpy as np
from compare_rev_equivalence import pivot_entity

pd.options.mode.chained_assignment = None


# ------------ FUNCTIONS ------------------------------

def create_revenue(transaction, site_to_customer, inst_price_df, inst_count_df, serial_numbers,
                   cust_assay_comp):
    simple_cust = pd.DataFrame(
        columns=['ds', 'entity', 'customer', 'yearly_inst_price', 'monthly_inst_price', 'daysinmonth',
                 'daily_inst_price'])
    simple_count = pd.DataFrame(columns=['ds', 'entity', 'customer', 'inst_count'])
    inst_by_date = pd.DataFrame(columns=['inst_num', 'ds', 'entity', 'customer'])

    compound_count = cust_assay_comp.groupby(['customer', 'assay_name'], as_index=False).agg({'compounds': 'nunique'})
    compound_count.rename(columns={'compounds': 'compounds_per_assay'}, inplace=True)
    cust_assay_comp_count = pd.merge(cust_assay_comp, compound_count, on=['customer', 'assay_name'], how='left')

    for index, i in inst_count_df.iterrows():
        start_day = i['start_date']
        end_day = i['end_date']
        count_df = pd.DataFrame({'ds': pd.date_range(start=start_day, end=end_day)})
        count_df['entity'] = i['entity']
        count_df['customer'] = i['customer']
        count_df['inst_count'] = i['inst_count']

        simple_count = pd.concat([simple_count, pd.DataFrame.from_records(count_df)])

    simple_count = simple_count[(simple_count['ds'] >= "2021-1-1")]

    for index, i in simple_count.iterrows():
        inst_begin = 1
        inst_count = i['inst_count'] + 1
        inst_count_df = pd.DataFrame({'inst_num': range(inst_begin, inst_count)})
        inst_count_df['ds'] = i['ds']
        inst_count_df['entity'] = i['entity']
        inst_count_df['customer'] = i['customer']

        inst_by_date = pd.concat([inst_by_date, pd.DataFrame.from_records(inst_count_df)])

    serial_numbers = serial_numbers.drop('min_trans_date', axis=1)
    inst_sn_by_date = pd.merge(inst_by_date, serial_numbers, on=['entity', 'customer', 'inst_num'], how='left')

    for index, i in inst_price_df.iterrows():
        entity = i['entity']
        customer = i['customer']
        start_day = i['trans_start']
        end_day = i['trans_end']
        yearly_amt = i['start_inst_price']
        monthly_amt = i['start_inst_price'] / 12

        staging_df = pd.DataFrame({'ds': pd.date_range(start=start_day, end=end_day)})
        staging_df['entity'] = entity
        staging_df['customer'] = customer
        staging_df['yearly_inst_price'] = yearly_amt
        staging_df['monthly_inst_price'] = monthly_amt
        staging_df['daysinmonth'] = staging_df['ds'].dt.daysinmonth
        staging_df['daily_inst_price'] = staging_df['monthly_inst_price'] / staging_df['daysinmonth']

        simple_cust = pd.concat([simple_cust, pd.DataFrame.from_records(staging_df)])

    simple_cust = simple_cust[(simple_cust['ds'] >= "2021-1-1")]

    simple_count = simple_count.drop_duplicates()
    simple_cust = simple_cust.drop_duplicates()

    # Join the two for getting Inst_Counts later in code
    simple_cust_count = pd.merge(inst_sn_by_date, simple_cust, on=['ds', 'customer', 'entity'], how='left')
    simple_cust_count = simple_cust_count.drop_duplicates()

    simple_cust_assay = pd.merge(simple_cust_count, cust_assay_comp_count, on='customer', how='left').fillna(0)

    simple_cust_assay['revenue'] = np.where(simple_cust_assay.assay_x_comp_count > 0.,
                                            simple_cust_assay.daily_inst_price / simple_cust_assay.assay_x_comp_count,
                                            simple_cust_assay.daily_inst_price)

    simple_cust_assay = simple_cust_assay.drop_duplicates()

    # get unit and license info, but not site_name since it would make the df truly enormous
    cust_unit_lic = site_to_customer.drop(['site_name', 'price_per_trans'], axis=1)
    cust_unit_lic = cust_unit_lic.drop_duplicates()

    simple_cust_inst = pd.merge(simple_cust_assay, cust_unit_lic, on=['entity', 'customer'], how='left')

    simple_cust_inst = simple_cust_inst.groupby(
        ['ds', 'entity', 'customer', 'license', 'unit', 'instrument_name', 'serial_number', 'assay_name', 'compounds',
         'CAS'], as_index=False).agg({'revenue': 'sum'})

    # ---- Total Count for
    # Inst/Trans
    # -------------------------------------------------------------------------------------------------------------

    df_total_count = transaction.groupby(['site_name', 'instrument_name', 'assay_name', 'ds'], as_index=False).agg(
        {'sample_count': 'sum', 'chromatogram_count': 'sum'})
    df_total_w_cust = pd.merge(df_total_count, site_to_customer, on='site_name', how='inner')

    # ------ Hybrid Top 6 Instrument -----------------------------------------------------------------------------------------------------------

    hybrid_staging = transaction.copy()

    hybrid_locations = site_to_customer.loc[site_to_customer['license'] == 'hybrid']
    hybrid_locations = hybrid_locations['site_name'].unique()
    hybrid_locations = hybrid_locations.tolist()
    print(hybrid_locations)

    hybrid_staging = hybrid_staging[hybrid_staging['site_name'].isin(hybrid_locations)]

    hybrid_staging['year'] = [x.timetuple().tm_year for x in hybrid_staging['ds']]
    hybrid_staging['month'] = [x.timetuple().tm_mon for x in hybrid_staging['ds']]

    hybrid_staging = pd.merge(hybrid_staging, site_to_customer, on='site_name', how='left')

    hybrid_inst_df = hybrid_staging.groupby(['year', 'month', 'customer', 'instrument_name'], as_index=False).agg(
        {'chromatogram_count': 'sum'})
    hybrid_inst_df = hybrid_inst_df.drop_duplicates()

    # top 6 instruments by chromatogram
    top6inst = (hybrid_inst_df.sort_values(['year', 'month', 'chromatogram_count', 'customer', 'instrument_name'],
                                           ascending=[True, True, False, False, False])
                .groupby(['year', 'month', 'customer'],
                         as_index=False,
                         sort=False)
                .nth([0, 1, 2, 3, 4, 5]))

    # mark instruments that are top 6 for their month
    top6inst['hybrid_instrument_flag'] = 1

    # create a df for all the dates so you can hae the value join up on daily level vs monthly
    get_dates_top6 = hybrid_staging.groupby(['ds', 'year', 'month', 'customer', 'instrument_name'], as_index=False).agg(
        {'chromatogram_count': 'sum'})

    hybrid_master = pd.merge(get_dates_top6, top6inst, on=['year', 'month', 'customer', 'instrument_name'],
                             how='left').fillna(0)
    hybrid_master = hybrid_master.drop(['chromatogram_count_x', 'chromatogram_count_y', 'year', 'month'], axis=1)
    hybrid_master = hybrid_master.drop_duplicates()

    hybrid_top6_master = hybrid_master[hybrid_master.hybrid_instrument_flag == 1]
    hybrid_trans_master = hybrid_master[hybrid_master.hybrid_instrument_flag == 0]

    # ------ Instrument Master -----------------------------------------------------------------------------------------------------------

    ####### Hybrid Section

    total_wo_hybrid_trans = pd.merge(df_total_w_cust, hybrid_trans_master, on=['ds', 'customer', 'instrument_name'],
                                     indicator=True, how='outer').query('_merge=="left_only"').drop(
        ['_merge', 'hybrid_instrument_flag'], axis=1)
    filter_total_w_hybrid_flag = pd.merge(total_wo_hybrid_trans, hybrid_top6_master,
                                          on=['ds', 'customer', 'instrument_name'], indicator=True, how='left')

    simple_cust_final = pd.merge(simple_cust_inst, filter_total_w_hybrid_flag,
                                 on=['entity', 'customer', 'ds', 'assay_name', 'instrument_name', 'unit', 'license'],
                                 how='left')

    simple_cust_final['rev_per_sample'] = simple_cust_final['revenue'] / simple_cust_final['sample_count']
    simple_cust_final['rev_per_chromatogram'] = simple_cust_final['revenue'] / simple_cust_final['chromatogram_count']

    simple_cust_final['trans_min_flag'] = 0
    print(list(simple_cust_final.columns))

    simple_cust_final = simple_cust_final[
        ['ds', 'entity', 'customer', 'site_name', 'instrument_name', 'license', 'unit', 'assay_name', 'compounds',
         'CAS',
         'hybrid_instrument_flag', 'sample_count', 'chromatogram_count', 'revenue', 'rev_per_sample',
         'rev_per_chromatogram', 'trans_min', 'trans_min_flag']]

    # ---- Transaction Component ------------------------------------------------------------------------------------------------------------------------------------------
    print('transaction')
    total_without_hybrid_inst = pd.merge(df_total_w_cust, hybrid_top6_master, on=['ds', 'customer', 'instrument_name'],
                                         indicator=True, how='outer').query('_merge=="left_only"').drop('_merge',
                                                                                                        axis=1)
    total_without_hybrid_inst = total_without_hybrid_inst.fillna(0)

    total_without_hybrid_inst = total_without_hybrid_inst.loc[
        total_without_hybrid_inst['license'].isin(['transaction', 'hybrid'])]
    total_without_hybrid_inst = total_without_hybrid_inst.drop_duplicates()

    filter_trans_comp = pd.merge(total_without_hybrid_inst, cust_assay_comp_count, on=['customer', 'assay_name'],
                                 how='left')

    for index, i in filter_trans_comp.iterrows():
        if i.loc['unit'] == 'chromatogram_count':
            rev_total = (i['chromatogram_count'] * i['price_per_trans'])
            rev_by_assay = rev_total / i['compounds_per_assay']
            rev_per_sample = rev_by_assay / i['sample_count']
            rev_per_chromatogram = rev_by_assay / i['chromatogram_count']
            filter_trans_comp.at[index, 'revenue'] = rev_by_assay
            filter_trans_comp.at[index, 'rev_per_sample'] = rev_per_sample
            filter_trans_comp.at[index, 'rev_per_chromatogram'] = rev_per_chromatogram
        elif i.loc['unit'] == 'sample_count':
            rev_total = (i['sample_count'] * i['price_per_trans'])
            rev_by_assay = rev_total / i['compounds_per_assay']
            rev_per_sample = rev_by_assay / i['sample_count']
            rev_per_chromatogram = rev_by_assay / i['chromatogram_count']
            filter_trans_comp.at[index, 'revenue'] = rev_by_assay
            filter_trans_comp.at[index, 'rev_per_sample'] = rev_per_sample
            filter_trans_comp.at[index, 'rev_per_chromatogram'] = rev_per_chromatogram
        else:
            continue

    filter_trans_comp = filter_trans_comp.fillna(0)

    # Trans Minimum Flag Section ------------------------------------------------
    trans_rev_copy = filter_trans_comp.copy()

    trans_rev_copy['year'] = [x.timetuple().tm_year for x in trans_rev_copy['ds']]
    trans_rev_copy['month'] = [x.timetuple().tm_mon for x in trans_rev_copy['ds']]

    monthly_trans_rev = trans_rev_copy.groupby(['year', 'month', 'customer', 'trans_min'], as_index=False).agg(
        {'revenue': 'sum'})
    monthly_trans_rev['trans_min_flag'] = 0

    # add a flag if under trans min
    for index, i in monthly_trans_rev.iterrows():
        if i.loc['revenue'] < i.loc['trans_min']:
            monthly_trans_rev.at[index, 'trans_min_flag'] = 1
        else:
            continue

    # create a df for all the dates so you can have the value join up on daily level vs monthly
    get_dates_trans = trans_rev_copy.groupby(['ds', 'year', 'month', 'customer'], as_index=False).agg(
        {'revenue': 'sum', 'instrument_name': 'nunique'})

    get_dates_trans.rename(columns={'instrument_name': 'instrument_count'}, inplace=True)

    trans_min_flag_df = pd.merge(get_dates_trans, monthly_trans_rev, on=['year', 'month', 'customer'],
                                 how='inner').fillna(0)
    trans_min_flag_df = trans_min_flag_df.drop(['revenue_x', 'revenue_y'], axis=1)
    trans_min_flag_df = trans_min_flag_df.drop_duplicates()

    trans_staging = pd.merge(filter_trans_comp, trans_min_flag_df, on=['ds', 'customer'], how='left')
    trans_staging = trans_staging.drop('trans_min_y', axis=1)
    trans_staging.rename(columns={'trans_min_x': 'trans_min'}, inplace=True)

    count_active_days = trans_staging.groupby(['year', 'month', 'customer'], as_index=False).agg(
        {'ds': 'nunique'})
    count_active_days.rename(columns={'ds': 'count_activity_days'}, inplace=True)

    trans_final = pd.merge(trans_staging, count_active_days, on=['year', 'month', 'customer'], how='left')

    for index, i in trans_final.iterrows():
        if i.loc['trans_min_flag'] == 1:
            daily_rev = i['trans_min'] / i['count_activity_days']
            rev_per_inst = daily_rev / i['instrument_count']
            if i['compounds_per_assay'] > 0:
                rev_by_assay = rev_per_inst / i['compounds_per_assay']
            else:
                rev_by_assay = rev_per_inst
            rev_per_sample = rev_by_assay / i['sample_count']
            rev_per_chromatogram = rev_by_assay / i['chromatogram_count']
            trans_final.at[index, 'revenue'] = rev_by_assay
            trans_final.at[index, 'rev_per_sample'] = rev_per_sample
            trans_final.at[index, 'rev_per_chromatogram'] = rev_per_chromatogram
        else:
            continue

    trans_final = trans_final[
        ['ds', 'entity', 'customer', 'site_name', 'instrument_name', 'license', 'unit', 'assay_name', 'compounds',
         'CAS',
         'hybrid_instrument_flag', 'sample_count', 'chromatogram_count', 'revenue', 'rev_per_sample',
         'rev_per_chromatogram', 'trans_min', 'trans_min_flag']]
    # -----------------------------------------------------------------------------------------------------------------

    # concatenate the two df's
    # frames = [simple_cust_final, trans_final]
    # output_df = pd.concat(frames).fillna(0)

    # output_df = output_df[['ds', 'entity', 'customer', 'site_name', 'instrument_name','license', 'unit','assay_name','compounds','CAS',
    # 'hybrid_instrument_flag', 'sample_count', 'chromatogram_count', 'revenue', 'rev_per_sample',
    # 'rev_per_chromatogram', 'trans_min', 'trans_min_flag']]

    return trans_final, simple_cust_final


# EXTERNAL RESOURCE READ IN ----------------------------------------------------------------------------------------

# transaction read in
transaction_path = "/Volumes/indigobio/Shared/Production/Transaction-Reports/Monthlies/"
csv_files = sorted(glob.glob(os.path.join(transaction_path, "*.csv")))
df_transaction = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)

# External documents
inst_price = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/instrument_ARR.csv').fillna(0)

inst_count = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/simple_inst_count.csv')

site_to_customer_key = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/site_license_trans_price_key.csv')
inst_list = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/cust_inst_sn.csv')
trans_min = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/trans_min.csv')

serial_numbers = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/inst_sn_master.csv')

cust_assay_comp = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/Cust_Assay_Comp_CAS.csv')

# Bill revenue
bill_rev = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/Bill_Revenue.csv')

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
df_transaction = df_transaction.sort_values(by=['ds'])
df_transaction = df_transaction[(df_transaction.ds > date(2019, 12, 31))]
df_transaction['ds'] = pd.to_datetime(df_transaction['ds'], infer_datetime_format=True)

# ONLY LICENSED INSTRUMENTS -----------------------------------------------------------------------------------

# inst_sn = inst_list[inst_list['serial_number'] != 'Unknown']

# Call function and save to PDF
trans_export, inst_export = create_revenue(df_transaction, site_to_customer_key, inst_price, inst_count, serial_numbers,
                                           cust_assay_comp)

trans_export.to_csv('Indigo_Transaction_Revenue.csv')
inst_export.to_csv('Indigo_Instrument_Revenue.csv')

# trim for only one license type
bill_inst_rev = bill_rev[bill_rev['license'] == 'instrument']
bill_trans_rev = bill_rev[bill_rev['license'] != 'instrument']

# inst evals
validate_inst_rev21 = pivot_entity(inst_export, bill_inst_rev, 2021)
validate_inst_rev21.to_csv('Inst_Revenue_Check_21.csv')

validate_inst_rev22 = pivot_entity(inst_export, bill_inst_rev, 2022)
validate_inst_rev22.to_csv('Inst_Revenue_Check_22.csv')

# trans evals
validate_trans_rev21 = pivot_entity(trans_export, bill_trans_rev, 2021)
validate_trans_rev21.to_csv('Transaction_Revenue_Check_21.csv')
