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
