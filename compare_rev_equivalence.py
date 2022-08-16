import pandas as pd

pd.options.mode.chained_assignment = None


def pivot_entity(in_df, bill_rev, year):
    bill_filter_df = bill_rev[bill_rev['year'] == year]

    bill_pivot = pd.pivot_table(bill_filter_df, index=["customer"],
                                values=["revenue"],
                                aggfunc='sum',
                                fill_value=0,
                                columns=["month"])

    # bill_pivot = bill_pivot.reset_index()
    bill_pivot = pd.DataFrame(bill_pivot.to_records())

    bill_mapping = {bill_pivot.columns[1]: 1, bill_pivot.columns[2]: 2, bill_pivot.columns[3]: 3,
                    bill_pivot.columns[4]: 4, bill_pivot.columns[5]: 5, bill_pivot.columns[6]: 6,
                    bill_pivot.columns[7]: 7, bill_pivot.columns[8]: 8, bill_pivot.columns[9]: 9,
                    bill_pivot.columns[10]: 10, bill_pivot.columns[11]: 11, bill_pivot.columns[12]: 12}

    bill_final = bill_pivot.rename(columns=bill_mapping)

    # Cooper df version
    filter_df = in_df[in_df['ds'].dt.year == year]
    filter_df['year'] = [x.timetuple().tm_year for x in filter_df['ds']]
    filter_df['month'] = [x.timetuple().tm_mon for x in filter_df['ds']]
    filter_df = filter_df.round(decimals=2)
    filter_df = filter_df.groupby(['year', 'month', 'customer'], as_index=False).agg({'revenue': 'sum'})
    filter_df = filter_df.sort_values(by=['customer', 'year', 'month'])

    coop_pivot = pd.pivot_table(filter_df, index=["customer"],
                                values=["revenue"],
                                aggfunc='sum', fill_value=0,
                                columns=["month"])

    coop_pivot = pd.DataFrame(coop_pivot.to_records())

    coop_mapping = {coop_pivot.columns[1]: 1, coop_pivot.columns[2]: 2, coop_pivot.columns[3]: 3,
                    coop_pivot.columns[4]: 4, coop_pivot.columns[5]: 5, coop_pivot.columns[6]: 6,
                    coop_pivot.columns[7]: 7, coop_pivot.columns[8]: 8, coop_pivot.columns[9]: 9,
                    coop_pivot.columns[10]: 10, coop_pivot.columns[11]: 11, coop_pivot.columns[12]: 12}

    coop_final = coop_pivot.rename(columns=coop_mapping)

    df_all = pd.concat([bill_final.set_index('customer'), coop_final.set_index('customer')],
                       axis='columns', keys=['Bill', 'Cooper'])

    df_final = df_all.swaplevel(axis='columns')

    df_final.sort_index(axis=1, level=[0, 1], ascending=[True, False], inplace=True)

    return df_final
