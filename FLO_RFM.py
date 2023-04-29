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
    dataframe["customer_value_total"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]
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

rfm["recency_score"] = pandas.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1])
rfm["frequency_score"] = pandas.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
rfm["monetary_score"] = pandas.qcut(rfm["monetary"], 5, labels=[1, 2, 3, 4, 5])
rfm.head()

rfm["RF_SCORE"] = (rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str))
rfm["RFM_SCORE"] = (
        rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str) + rfm["monetary_score"].astype(str))

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

rfm["segment"] = rfm["RF_SCORE"].replace(seg_map, regex=True)
rfm.head()

rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])

target_segments_customer_ids = rfm[rfm["segment"].isin(["champions", "loyal_customers"])]["customer_id"]
customers_ids = dataframe[(dataframe["master_id"].isin(target_segments_customer_ids)) & (dataframe["interested_in_categories_12"].str.contains("KADIN"))]["master_id"]
customers_ids.to_csv("yeni_marka_hedef_musteri_id.csv", index=False)

target_segments_customer_ids = rfm[rfm["segment"].isin(["cant_loose", "atrisk", "new_customers"])]["customer_id"]
cust_ids = dataframe[(dataframe["master_id"].isin(target_segments_customer_ids)) & (
        (dataframe["interested_in_categories_12"].str.contains("ERKEK")) | (dataframe["interested_in_categories_12"].str.contains("COCUK")))]["master_id"]

cust_ids.to_csv("indirim_hedef_müşteri_ids.csv", index=False)


def create_rfm(dataframee):
    # Veriyi hazırlama
    dataframe["order_num_total"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["customer_value_total"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]
    date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
    dataframe[date_columns] = dataframe[date_columns].apply(pandas.to_datetime)

    # RFM metriklerinin hesaplanması
    dataframe["last_order_date"].max()
    analysis_date = datetime.datetime(2021, 6, 1)

    rfm = pandas.DataFrame()
    rfm["customer_id"] = dataframe["master_id"]
    rfm["recency"] = (analysis_date - dataframe["last_order_date"]).astype('timedelta64[D]')
    rfm["frequency"] = dataframe["order_num_total"]
    rfm["monetary"] = dataframe["customer_value_total"]

    rfm["recency_score"] = pandas.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1])
    rfm["frequency_score"] = pandas.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
    rfm["monetary_score"] = pandas.qcut(rfm["monetary"], 5, labels=[1, 2, 3, 4, 5])

    rfm["RF_SCORE"] = (rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str))
    rfm["RFM_SCORE"] = (rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str) + rfm["monetary_score"].astype(str))

    # Segmentlerin oluşturulması
    seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_Risk',
        r'[1-2]5': 'cant_loose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalists',
        r'5[4-5]': 'champions'
    }
    rfm["segment"] = rfm["RF_SCORE"].replace(seg_map, regex=True)

    return rfm[["customer_id", "recency", "frequency", "monetary", "RF_SCORE", "RFM_SCORE", "segment"]]


rfm_dataframe = create_rfm(dataframe)
rfm_dataframe.head()
