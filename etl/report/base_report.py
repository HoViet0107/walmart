from pyspark.sql import DataFrame
import matplotlib.pyplot as plt
import seaborn as sns
import os

class BaseReport:
    def __init__(self, df: DataFrame):
        self.df = df

    def export_to_excel(self, pandas_df, filename: str):
        # Create directory if not exists
        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        # Save the DataFrame to Excel
        pandas_df.to_excel(filename, index=False)
        print(f"✅ Report exported to {'reports/'+filename}")
    
    def show_bar_chart(self, data, x_col, y_col, title, xlabel, ylabel):
        plt.figure(figsize=(10, 6))
        sns.barplot(x=x_col, y=y_col, data=data)
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()