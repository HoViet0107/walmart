from pyspark.sql import DataFrame
from etl.report.base_report import BaseReport
import matplotlib.pyplot as plt

class ProductReport(BaseReport):
    def __init__(self, df: DataFrame):
        super().__init__(df)

    def generate_overview(self):
        total_products = self.df.count()
        category_distribution = self.df.groupBy("category").count().toPandas()

        print(f"Total Products: {total_products}")
        print(category_distribution) # print category_distribution

        # Generate and display bar chart for category distribution
        self.show_bar_chart(
            data=category_distribution,
            x_col="category", y_col="count",
            title="Product Category Distribution", xlabel="Category", ylabel="Count"
        )

        # Export category distribution to Excel
        self.export_to_excel(category_distribution, "reports/category_distribution.xlsx")
