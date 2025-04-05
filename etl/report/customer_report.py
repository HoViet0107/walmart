import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import count
from pyspark.sql import DataFrame
from etl.report.base_report import BaseReport


class CustomerReport(BaseReport):
    def __init__(self, df: DataFrame):
        super().__init__(df)

    def generate_overview(self):
        total_customers = self.df.count()
        gender_distribution = self.df.groupBy("gender").count().toPandas()

        print(f"Total Customers: {total_customers}")
        print(gender_distribution)  # print gender_distribution

        # create bar chart for gender distribution
        self.show_bar_chart(
            data=gender_distribution,
            x_col="gender", y_col="count", 
            title="Gender Distribution", xlabel="Gender", ylabel="Count"
        )

        # export gender distribution to excel
        self.export_to_excel(gender_distribution, "reports/gender_distribution.xlsx")
