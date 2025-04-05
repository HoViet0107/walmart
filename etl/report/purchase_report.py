from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum
from etl.report.base_report import BaseReport

class PurchaseReport(BaseReport):
    def __init__(self, df: DataFrame):
        super().__init__(df)

    def generate_overview(self):
        total_purchases = self.df.count()
        total_revenue = self.df.agg(sum("purchase_amount").alias("total_revenue")).collect()[0]["total_revenue"]
        print(f"Total Purchases: {total_purchases}")
        print(f"Total Revenue: {total_revenue}")

    def top_customers(self):
        top_customers_df = self.df.groupBy("customer_id") \
            .agg(sum("purchase_amount").alias("total_spent")) \
            .orderBy(col("total_spent").desc()) \
            .limit(10).toPandas()

        # add rank co 1 -> 10
        top_customers_df.index = top_customers_df.index + 1
        top_customers_df.reset_index(inplace=True)
        top_customers_df.rename(columns={"index": "Rank"}, inplace=True)
        print(top_customers_df)
        self.show_bar_chart(
            data=top_customers_df,
            x_col="customer_id", y_col="total_spent", 
            title="Top 10 Customers", 
            xlabel="Customer ID", 
            ylabel="Total Spent"
        )

        self.export_to_excel(top_customers_df, "reports/top_customers.xlsx")
