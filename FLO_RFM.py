import datetime

import pandas

pandas.set_option("display.max_columns", None)
pandas.set_option("display.max_rows", None)
pandas.set_option("display.float_format", lambda x: '%.2f' % x)
pandas.set_option("display.max_columns", None)
pandas.set_option("display.width", 1000)

dataframe_ = pandas.read_csv("Datasets/flo_data_20k.csv")
dataframe = dataframe_.copy()

dataframe.head()
dataframe["order_channel"].value_counts()
dataframe["last_order_channel"].value_counts()

dataframe.head(10)
dataframe.columns
dataframe.shape
dataframe.describe().T
dataframe.isnull().sum()
dataframe.info()

dataframe["order_num_total"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
dataframe["customer_value_total"] = dataframe["customer_value_total_ever_offline"] + dataframe[
    "customer_value_total_ever_online"]
dataframe.head()

date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
dataframe[date_columns] = dataframe[date_columns].apply(pandas.to_datetime)
dataframe.info()

dataframe.groupby("order_channel").agg({"master_id": "count",
                                        "order_num_total": "sum",
                                        "customer_value_total": "sum"})

dataframe.sort_values("customer_value_total", ascending=False)[:10]
dataframe.sort_values("order_num_total", ascending=False)[:10]


def data_prep(dataframee):
    dataframe["order_num_total"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["customer_value_total"] = dataframe["customer_value_total_ever_offline"] + dataframe[
        "customer_value_total_ever_online"]
    date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
    dataframe[date_columns] = dataframe[date_columns].apply(pandas.to_datetime)
    return dataframe


# RFM metriklerinin hesaplanması

dataframe["last_order_date"].max()
analysis_date = datetime.datetime(2021, 6, 1)

rfm = pandas.DataFrame()
rfm["customer_id"] = dataframe["master_id"]
rfm["recency"] = (analysis_date - dataframe["last_order_date"]).astype('timedelta64[D]')
rfm["frequency"] = dataframe["order_num_total"]
rfm["monetary"] = dataframe["customer_value_total"]
rfm.head()
