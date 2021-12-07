import os
import yaml
import pandas as pd
import argparse

def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config

def get_data(config_path):
    config = read_params(config_path)
    data_path = config["data_source"]["s3_source"]
    df = pd.read_csv(data_path, sep=",", encoding='utf-8')
    zero_impute = ['total_rech_data_6', 'total_rech_data_7', 'total_rech_data_8', 'total_rech_data_9',
        'av_rech_amt_data_6', 'av_rech_amt_data_7', 'av_rech_amt_data_8', 'av_rech_amt_data_9',
        'max_rech_data_6', 'max_rech_data_7', 'max_rech_data_8', 'max_rech_data_9'
       ]
    cat_cols=['night_pck_user_6', 'night_pck_user_7', 'night_pck_user_8', 'night_pck_user_9', 'fb_user_6', 'fb_user_7', 'fb_user_8', 'fb_user_9']
    df[zero_impute] = df[zero_impute].apply(lambda x: x.fillna(0))
    df[cat_cols]=df[cat_cols].apply(lambda x: x.fillna(-1))
    df.drop(['mobile_number', 'circle_id'],axis=1,inplace=True)
    df.drop(['last_date_of_month_6', 'last_date_of_month_7', 'last_date_of_month_8', 'last_date_of_month_9', 'date_of_last_rech_6', 'date_of_last_rech_7', 'date_of_last_rech_8', 'date_of_last_rech_9', 'date_of_last_rech_data_6', 'date_of_last_rech_data_7', 'date_of_last_rech_data_8', 'date_of_last_rech_data_9'],axis=1,inplace=True)
    initial_cols = df.shape[1]
    MISSING_THRESHOLD = 0.7
    include_cols = list(df.apply(lambda column: True if column.isnull().sum()/df.shape[0] < MISSING_THRESHOLD else False))
    df=df.loc[:, include_cols]
    df = df.dropna()
    
    df['total_data_rech_6'] = df.total_rech_data_6 * df.av_rech_amt_data_6
    df['total_data_rech_7'] = df.total_rech_data_7 * df.av_rech_amt_data_7
    
    # calculate total recharge amount for June and July --> call recharge amount + data recharge amount
    df['amt_data_6'] = df.total_rech_amt_6 + df.total_data_rech_6
    df['amt_data_7'] = df.total_rech_amt_7 + df.total_data_rech_7
    
    df['av_amt_data_6_7'] = (df.amt_data_6 + df.amt_data_7)/2
    
    churn_filtered = df.loc[df.av_amt_data_6_7 >= df.av_amt_data_6_7.quantile(0.7), :]
    churn_filtered = churn_filtered.reset_index(drop=True)
    
    churn_filtered = churn_filtered.drop(['total_data_rech_6', 'total_data_rech_7','amt_data_6', 'amt_data_7', 'av_amt_data_6_7'], axis=1)
    
    # calculate total incoming and outgoing minutes of usage
    churn_filtered['total_calls_mou_9'] = churn_filtered.total_ic_mou_9 + churn_filtered.total_og_mou_9
    # calculate 2g and 3g data consumption
    churn_filtered['total_internet_mb_9'] =  churn_filtered.vol_2g_mb_9 + churn_filtered.vol_3g_mb_9
    churn_filtered['churn'] = churn_filtered.apply(lambda row: 1 if (row.total_calls_mou_9 == 0 and row.total_internet_mb_9 == 0) else 0, axis=1)
    
    # delete derived variables
    churn_filtered = churn_filtered.drop(['total_calls_mou_9', 'total_internet_mb_9'], axis=1)
    
    churn_filtered.churn = churn_filtered.churn.astype("category")
    
    
    churn_filtered = churn_filtered.filter(regex='[^9]$', axis=1)
    
    col_9_names = df.filter(regex='9$', axis=1).columns

    # update num_cols and cat_cols column name list
    
    cat_cols = [col for col in cat_cols if col not in col_9_names]
    cat_cols.append('churn')
    num_cols = [col for col in churn_filtered.columns if col not in cat_cols]
    
    # change columns types
    churn_filtered[num_cols] = churn_filtered[num_cols].apply(pd.to_numeric)
    churn_filtered[cat_cols] = churn_filtered[cat_cols].apply(lambda column: column.astype("category"), axis=0)
    
    def cap_outliers(array, k=3):
        upper_limit = array.mean() + k*array.std()
        lower_limit = array.mean() - k*array.std()
        array[array<lower_limit] = lower_limit
        array[array>upper_limit] = upper_limit
        return array
        
    churn_filtered[num_cols] = churn_filtered[num_cols].apply(cap_outliers, axis=0)
    
    churn_filtered['churn'] = pd.to_numeric(churn_filtered['churn'])
    
    return churn_filtered

if __name__=="__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    parsed_args = args.parse_args()
    data = get_data(config_path = parsed_args.config)