import pandas as pd
import os
import glob
from datetime import datetime
from datetime import date
pd.options.mode.chained_assignment = None


# ------------ FUNCTIONS ------------------------------

def create_revenue(transaction, site_to_customer, inst_price_df, inst_count_df, inst_sn, cust_assay_comp, trans_min):
    simple_cust = pd.DataFrame(
        columns=['ds', 'entity', 'customer', 'yearly_inst_price', 'monthly_inst_price', 'daysinmonth',
                 'daily_inst_price'])
    simple_count = pd.DataFrame(columns=['ds', 'entity', 'customer', 'inst_count'])

    for index, i in inst_count_df.iterrows():
        start_day = i['start_date']
        end_day = i['end_date']
        count_df = pd.DataFrame({'ds': pd.date_range(start=start_day, end=end_day)})
        count_df['entity'] = i['entity']
        count_df['customer'] = i['customer']
        count_df['inst_count'] = i['inst_count']

        #simple_count = simple_count.append(count_df, ignore_index=True)
        simple_count = pd.concat([simple_count, pd.DataFrame.from_records(count_df)])

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
        # staging_df['revenue'] = staging_df['daily_inst_price']

        #simple_cust = simple_cust.append(staging_df, ignore_index=True)
        simple_cust = pd.concat([simple_cust, pd.DataFrame.from_records(staging_df)])

    simple_count = simple_count.drop_duplicates()
    simple_cust = simple_cust.drop_duplicates()

    # Join the two for getting Inst_Counts later in code
    simple_cust_count = pd.merge(simple_count, simple_cust, on=['ds', 'customer', 'entity'], how='left')

    simple_cust_inst = pd.merge(simple_cust_count, inst_sn, on=['customer', 'entity'], how='left')

    # ------ Cust/inst/Assay/Comp/CAS Master -----------------------------------------------------------------------

    #simple_cust_inst_assay = pd.merge(simple_cust_inst, cust_assay_comp, on=['customer', 'entity'], how='left')
    #simple_cust_inst_assay['daily_inst_per_assay'] =

    # ------ Hybrid Top 6 Instrument -------------------------------------------------------------------------------

    hybrid_staging = transaction.copy()

    ## TO DO JUNE 23RD this section needs to be replaced with reading in which sites have a hybrid license distinction
    # hybrid_locations = ['mtl','mtltest','arup831','arup831test','arup891','arup891test','arup894','arup894test',
    # 'arup895','arup895test','arup897','aruptest']

    hybrid_locations = site_to_customer.loc[site_to_customer['license'] == 'hybrid']
    hybrid_locations = hybrid_locations['site_name'].unique()
    hybrid_locations = hybrid_locations.tolist()

    hybrid_staging = hybrid_staging[hybrid_staging['site_name'].isin(hybrid_locations)]

    hybrid_staging['year'] = [x.timetuple().tm_year for x in hybrid_staging['ds']]
    hybrid_staging['month'] = [x.timetuple().tm_mon for x in hybrid_staging['ds']]

    hybrid_staging = pd.merge(hybrid_staging, site_to_customer, on='site_name', how='left')

    hybrid_inst_df = hybrid_staging.groupby(['year', 'month', 'customer', 'instrument_name'], as_index=False).agg(
        {'chromatogram_count': 'sum'})
    hybrid_inst_df = hybrid_inst_df.drop_duplicates()

    # CURRENTLY ONLY FOR CHROMATOGRAMS NEEDS UPDATE TO HANDLE SAMPLES
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

    hybrid_top6_master = hybrid_master[hybrid_master.hybrid_instrument_flag != 0]
    # hybrid_trans_master = hybrid_master[hybrid_master.hybrid_instrument_flag != 1]

    # SET GENERAL QUEST RESOURCES ------------------------------------------------------------------------------------

    quest_inst = simple_cust_count[simple_cust_count['entity'] == 'Quest']

    tiered_quest_inst = quest_inst[(quest_inst['ds'] > "2019-12-31")]
    tiered_quest_inst = tiered_quest_inst[(tiered_quest_inst['ds'] < "2021-1-1")]

    # quest_inst = quest_inst.drop(['trans_start', 'trans_end', 'start_inst_price'], axis=1)
    quest_inst = quest_inst.drop_duplicates()
    quest_df_total_count = quest_inst.groupby(['entity', 'ds'], as_index=False).agg({'inst_count': 'sum'})

    tiered_quest_total_count = quest_df_total_count[(quest_df_total_count['ds'] > "2019-12-31")]
    tiered_quest_total_count = quest_df_total_count[(quest_df_total_count['ds'] < "2021-1-1")]

    # -----------------------------------------------------------------------------------------------------------------

    # This is used in multiple segments, leave alone unless you've checked all subsequent levels
    df_total_count = transaction.groupby(['site_name', 'instrument_name', 'assay_name', 'ds'], as_index=False).agg(
        {'sample_count': 'sum', 'chromatogram_count': 'sum'})

    # -----------------------------------------------------------------------------------------------------------------

    # INSTRUMENT REVENUE SECTION (QUEST POST '21') -------------------------------------------------------------------

    # simple customer date filter
    date_filtered_simple_cust = simple_cust_inst[(simple_cust_inst['ds'] < "2023-01-01")]
    date_filtered_simple_cust = date_filtered_simple_cust[(date_filtered_simple_cust['ds'] > "2019-12-31")]

    # remove the pre-2021 Quest figures
    filtered_simple_cust = pd.merge(date_filtered_simple_cust, tiered_quest_inst, indicator=True, how='outer').query(
        '_merge=="left_only"').drop('_merge', axis=1)

    filtered_simple_cust['revenue'] = filtered_simple_cust['daily_inst_price'] * filtered_simple_cust['inst_count']

    # grab the sites- the cust and entity already exist
    simple_cust_site = pd.merge(simple_cust_count, site_to_customer, on=['entity', 'customer'], how='left')
    # simple_cust_site = simple_cust_site.loc[simple_cust_site['license'].isin(['instrument','hybrid'])]

    simple_cust_w_trans = pd.merge(simple_cust_site, df_total_count, on=['site_name', 'ds'], how='inner')

    # get the count of assasy ran by an instrument per day so rtevenue isn't double counting
    assay_per_inst_count = transaction.groupby(['site_name', 'instrument_name', 'ds'], as_index=False).agg(
        {'assay_name': 'nunique'})
    assay_per_inst_count = assay_per_inst_count.rename(columns={'assay_name': 'assay_count'})
    simple_cust_total = pd.merge(simple_cust_w_trans, assay_per_inst_count, on=['site_name', 'instrument_name', 'ds'],
                                 how='inner')

    # Hybrid Section
    simple_cust_final = pd.merge(simple_cust_total, hybrid_top6_master, on=['ds', 'customer', 'instrument_name'],
                                 how='left').fillna(0)

    simple_cust_final['revenue'] = simple_cust_final['daily_inst_price'] / simple_cust_final['assay_count']
    simple_cust_final['revenue_per_sample'] = simple_cust_final['revenue'] / simple_cust_final['sample_count']
    simple_cust_final['revenue_per_chromatogram'] = simple_cust_final['revenue'] / simple_cust_final[
        'chromatogram_count']

    simple_cust_final = simple_cust_final[(simple_cust_final['ds'] > "2019-12-31")].drop_duplicates()

    simple_cust_final['trans_min'] = 0
    simple_cust_final['trans_min_flag'] = 0

    simple_cust_final = simple_cust_final[
        ['ds', 'entity', 'customer', 'site_name', 'instrument_name', 'inst_count', 'assay_name', 'assay_count',
         'license', 'hybrid_instrument_flag', 'yearly_inst_price', 'monthly_inst_price', 'daily_inst_price',
         'sample_count', 'chromatogram_count', 'revenue', 'revenue_per_sample', 'revenue_per_chromatogram', 'trans_min',
         'trans_min_flag', 'daysinmonth']]

    # PRE_2021, TIERED INSTRUMENT PRICING --------------------------------------------------------------------------

    # pre_df_quest_w_tier = pd.merge(pre_df_total_count, quest_tier_staging, left_on=['entity', 'inst_count'], right_on=['entity', 'quest_total_inst_count'], how='left')
    # pre_df_quest_w_tier = pre_df_quest_w_tier.drop('inst_count', axis=1)

    # pre_inst_count_stage = quest_inst[(quest_inst['ds'] > "2019-12-31")]
    # pre_inst_count_stage = quest_inst[(quest_inst['ds'] < "2021-1-1")]
    # pre_df_quest_w_price = pd.merge(simple_count, pre_df_quest_w_tier, left_on=['entity', 'ds'], right_on=['entity', 'ds'], how='inner')
    # pre_df_quest_w_price = pre_df_quest_w_price[(pre_df_quest_w_price['ds'] > "2019-12-31")]
    # pre_df_quest_w_price = pre_df_quest_w_price[(pre_df_quest_w_price['ds'] < "2021-1-1")]

    # pre_df_quest_w_price = pre_df_quest_w_price.drop(['quest_total_inst_count'], axis=1)
    # pre_df_quest_w_price = pre_df_quest_w_price.drop_duplicates()

    # pre_quest_site = pd.merge(pre_df_quest_w_price, site_to_customer, on=['entity','customer'], how='left')

    # pre_quest_totals = pd.merge(pre_quest_site, df_total_count, on=['site_name','ds'], how='inner')

    # merge with the assyas_per_inst so you can calculate the revenue
    # pre_quest_final = pd.merge(pre_quest_totals, assay_per_inst_count, on=['site_name', 'instrument_name', 'ds'], how='left')
    # pre_quest_final = pre_quest_final.drop_duplicates()

    # pre_quest_final['daysinmonth'] = pre_quest_final['ds'].dt.daysinmonth
    # pre_quest_final['daily_inst_price'] = pre_quest_final['monthly_inst_price']/pre_quest_final['daysinmonth']

    # pre_quest_final['revenue'] = pre_quest_final['daily_inst_price']/pre_quest_final['assay_count']
    # pre_quest_final['revenue_per_sample'] = pre_quest_final['daily_inst_price']/pre_quest_final['sample_count']
    # pre_quest_final['revenue_per_chromatogram'] = pre_quest_final['daily_inst_price']/pre_quest_final['chromatogram_count']

    # pre_quest_final['trans_min'] = 0
    # pre_quest_final['trans_min_flag'] = 0
    # pre_quest_final = pre_quest_final.drop_duplicates()

    # -----------------------------------------------------------------------------------------------------------------

    # NEED TO COME HERE AND SWITCH OUT WORK_UNIT FOR NEW SITE_CUST
    # LAYER HYBRID IN

    # JUNE 23RD THIS IS UGLY FILTER YOU SHOULD REWRITE WORK_UNIT INTO SITE_TO_CUST
    trans_total_cust = pd.merge(df_total_count, site_to_customer, on=['site_name'], how='left')
    transaction_customers = ['chromatogram_count', 'sample_count']
    trans_total_cust = trans_total_cust.loc[trans_total_cust['unit'].isin(transaction_customers)]
    trans_total_cust = trans_total_cust.drop_duplicates()

    # initialize the column the loop will rreplace on per line basis
    trans_total_cust['daily_inst_price'] = 0

    # get all the transactional hybrids by outer joi dropping the top6 inst
    trans_price_df = pd.merge(trans_total_cust, hybrid_top6_master, indicator=True, how='outer').query(
        '_merge=="left_only"').drop('_merge', axis=1)
    trans_price_df = trans_price_df.fillna(0)

    for index, i in trans_price_df.iterrows():
        if i.loc['unit'] == 'chromatogram_count':
            rev_total = i['chromatogram_count'] * i['price_per_trans']
            rev_per_sample = rev_total / i['sample_count']
            rev_per_chromatogram = rev_total / i['chromatogram_count']
            trans_price_df.at[index, 'revenue'] = rev_total
            trans_price_df.at[index, 'revenue_per_sample'] = rev_per_sample
            trans_price_df.at[index, 'revenue_per_chromatogram'] = rev_per_chromatogram
        elif i.loc['unit'] == 'sample_count':
            rev_total = i['sample_count'] * i['price_per_trans']
            rev_per_sample = rev_total / i['sample_count']
            rev_per_chromatogram = rev_total / i['chromatogram_count']
            trans_price_df.at[index, 'revenue'] = rev_total
            trans_price_df.at[index, 'revenue_per_sample'] = rev_per_sample
            trans_price_df.at[index, 'revenue_per_chromatogram'] = rev_per_chromatogram
        else:
            continue

    trans_price_df = trans_price_df.fillna(0)

    # Inst count table
    trans_inst_count_df = trans_price_df.groupby(['customer', 'ds'], as_index=False).agg({'instrument_name': 'nunique'})
    trans_inst_count_df = trans_inst_count_df.rename(columns={'instrument_name': 'inst_count'})
    trans_price_inst_df = pd.merge(trans_price_df, trans_inst_count_df, on=['customer', 'ds'], how='left')

    # Trans Minimum Flag Section ------------------------------------------------
    staging_trans_rev = trans_price_inst_df.copy()

    staging_trans_rev['year'] = [x.timetuple().tm_year for x in staging_trans_rev['ds']]
    staging_trans_rev['month'] = [x.timetuple().tm_mon for x in staging_trans_rev['ds']]

    monthly_trans_rev = staging_trans_rev.groupby(['year', 'month', 'customer'], as_index=False).agg({'revenue': 'sum'})
    trans_min_comparison = pd.merge(monthly_trans_rev, trans_min, on='customer', how='left')
    trans_min_comparison['trans_min_flag'] = 0

    # add a flag if under trans min
    for index, i in trans_min_comparison.iterrows():
        if i.loc['revenue'] < i.loc['trans_min']:
            trans_min_comparison.at[index, 'trans_min_flag'] = 1
        else:
            continue

    # create a df for all the dates so you can have the value join up on daily level vs monthly
    get_dates_trans = staging_trans_rev.groupby(['ds', 'year', 'month', 'customer'], as_index=False).agg(
        {'revenue': 'sum'})

    trans_min_flag_df = pd.merge(get_dates_trans, trans_min_comparison, on=['year', 'month', 'customer'],
                                 how='inner').fillna(0)
    trans_min_flag_df = trans_min_flag_df.drop(['revenue_x', 'revenue_y', 'year', 'month'], axis=1)
    trans_min_flag_df = trans_min_flag_df.drop_duplicates()

    # Final trans revenue processing -----------------------------------------------------
    trans_pricing_w_flag = pd.merge(trans_price_inst_df, trans_min_flag_df, on=['ds', 'customer'], how='left')

    assay_per_inst_count = trans_total_cust.groupby(['customer', 'site_name', 'instrument_name', 'ds'],
                                                    as_index=False).agg({'assay_name': 'nunique'})
    assay_per_inst_count = assay_per_inst_count.rename(columns={'assay_name': 'assay_count'})

    trans_master = pd.merge(trans_pricing_w_flag, assay_per_inst_count,
                            on=['site_name', 'customer', 'instrument_name', 'ds'], how='left')
    trans_master['daysinmonth'] = trans_master['ds'].dt.daysinmonth
    trans_master['daily_inst_price'] = 0
    trans_master['monthly_inst_price'] = 0
    trans_master['yearly_inst_price'] = 0

    # YOU NEED TO ENSURE THIS ISN"T DOUBLE COUNTING
    # for index, i in trans_master.iterrows():
    # if i.loc['trans_min_flag'] == 1:

    # monthly_min_staging = i.loc['trans_min']/i.loc['inst_count']
    # monthly_min = monthly_min_staging/i.loc['assay_count']
    # yearly_min = monthly_min*12
    # daily_min = monthly_min/i.loc['daysinmonth']
    # rev_per_sample = daily_min/i['sample_count']
    # rev_per_chromatogram = daily_min/i['chromatogram_count']

    # trans_master.at[index,'yearly_inst_price'] = yearly_min
    # trans_master.at[index,'monthly_inst_price'] = monthly_min
    # trans_master.at[index,'daily_inst_price'] = daily_min
    # trans_master.at[index,'revenue'] = daily_min
    # trans_master.at[index,'revenue_per_sample'] = rev_per_sample
    # trans_master.at[index,'revenue_per_chromatogram'] = rev_per_chromatogram

    # else:
    # continue

    trans_master = trans_master[
        ['ds', 'entity', 'customer', 'site_name', 'instrument_name', 'inst_count', 'assay_name', 'assay_count',
         'license', 'hybrid_instrument_flag', 'yearly_inst_price', 'monthly_inst_price', 'daily_inst_price',
         'sample_count', 'chromatogram_count', 'unit', 'price_per_trans', 'revenue', 'revenue_per_sample',
         'revenue_per_chromatogram', 'trans_min', 'trans_min_flag', 'daysinmonth']]
    # -----------------------------------------------------------------------------------------------------------------

    # concatenate the two df's
    frames = [simple_cust_final, trans_price_df]
    output_df = pd.concat(frames).fillna(0)

    output_df = output_df[
        ['ds', 'entity', 'customer', 'site_name', 'instrument_name', 'inst_count', 'assay_name', 'assay_count',
         'license', 'hybrid_instrument_flag', 'yearly_inst_price', 'monthly_inst_price', 'daily_inst_price',
         'sample_count', 'chromatogram_count', 'unit', 'price_per_trans', 'revenue', 'revenue_per_sample',
         'revenue_per_chromatogram', 'trans_min', 'trans_min_flag', 'daysinmonth']]

    return filtered_simple_cust


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
#inst_list = pd.read_csv('/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/instrument_sn.csv')
inst_list = pd.read_csv('/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/cust_inst_sn.csv')
trans_min = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/trans_min.csv')

cust_assay_comp = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/Cust_Assay_Comp_CAS.csv')

work_unit_rates = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/price_per_trans.csv')
work_unit_rates = work_unit_rates.drop('customer_name', axis=1)

# Quest
quest_inst_stage = pd.read_csv(
    '/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/Quest_inst_count.csv')
# quest_tier_stage = pd.read_csv('/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/Quest_discount_tier_postAmmend.csv')
# quest_pre_ammend = pd.read_csv('/Volumes/indigobio/Shared/Research/Forecasting/Assay_Configuration/Supplementary/quest_invoice_pre_ammend.csv')


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
df_transaction = df_transaction[(df_transaction.ds >= date(2019, 12, 31))]
df_transaction['ds'] = pd.to_datetime(df_transaction['ds'], infer_datetime_format=True)

# ONLY LICENSED INSTRUMENTS -----------------------------------------------------------------------------------

inst_sn = inst_list[inst_list['serial_number'] != 'Unknown']


# Call function and save to PDF
x = create_revenue(df_transaction, site_to_customer_key, inst_price, inst_count, inst_sn, cust_assay_comp, trans_min)

x.to_csv('inst_check.csv')
