# Fashion Retail ETL project

## Overview 
This project is a ETL (Extract, Transform, Load) project that demonstrates the process of extracting data from csv file using Apache Spark(PySpark), performing various transformations, and finally loading the processed data into a MySQL database. The entire process is implemented in Python, and SQL.

## Table of Contents
- [Introduction](#introduction)
- [Technologies Used](#technologies-used)
- [Transformation](#transformation)
- [Installation](#installation)
- [License](#license)

## Introduction

In the retail industry, managing and analyzing large volumes of transactional and customer data is crucial for business insights and decision-making. This Fashion Retail ETL Project is designed to process raw data efficiently and transform it into a structured format for further analysis and reporting.

The project follows the **ETL** (Extract, Transform, Load) pipeline using **Apache Spark** (PySpark) to handle large datasets, perform necessary data cleaning and transformation, and finally store the processed data into a MySQL database.

Key Features:

- **Extract**: Load raw transactional data from CSV files.
- **Transform**: Clean, normalize, and structure data (handle missing values, remove duplicates, format data).
- **Load**: Store clean data into a MySQL database for analysis.
- **Analyze & Report**: Generate overview about customer, product and purchase. Summarize total revenue, summary of the top 10 users with the highest total spending.

## Technologies Used  

    - Python: Programming language used for implementing the Spark ETL process.
    - PySpark: PySpark is the Python API for Apache Spark. It enables you to perform real-time, large-scale data processing in a distributed environment using Python.
    - Matplotlib & Seaborn: For data visualization (bar charts, line charts, etc.) to support insights and reporting.
    - Openpyxl: Used to export processed data and reports to Excel format (.xlsx).
    - MySQL: The final processed data is loaded into a MySQL database.

## Transformation  

The Spark ETL process involves the following transformation steps on the extracted data:

- Date Format Change:
     - Convert date columns to a consistent date format.
     - Change `review_rating` from Double to Float.
- Handling Missing Data:
     - Replace missing values with general values such as `Unknown`
     - Replace missing numerical values with `-1` value.
- Handling Duplicated Data:
     - Identify and remove duplicate records to ensure data integrity.
     - Use Spark’s .dropDuplicates() method to eliminate duplicate rows based on unique identifiers.

After clean up data, load cleaned data to Mysql database or AWS S3:

- Use JDBC Connector to write the cleaned data into MySQL.
- Ensure the database schema matches the transformed data structure.
- Used AWS CLI to upload cleaned data to AWS S3.

## Installation  
1. Download and Install Python for Windows

   Link: [Download Python](https://www.python.org/downloads/)

2. Download and Install MySQL for Windows
   
   Go to: [Download MySQL](https://dev.mysql.com/downloads/installer/)  
   - Select version: Choose version  
   - Select Operating System: Your system  
   - Configure: [MySQL Installation Guide](https://www.geeksforgeeks.org/how-to-install-mysql-in-windows/)

3. Download JDK 8 or 11:  

   Link: [Download JDK](https://drive.google.com/drive/folders/1YiKNzQhiNOz_S_gCyjd_IbWwcAAnvqlx?usp=sharing)  
   - Run MSI file to install

   or
   
   - Put the `bin` folder in the zip file to `"C:\Program Files\Java\jdk-11"` (create if not exists)

4. Clone this repository to your local machine:
   
       git clone https://github.com/your-username/spark-etl-project.git
       cd spark-etl-project
   
5. Open in Vscode
   
       code .
   
6. Open terminal
   
       Ctrl + "`"
        Or
       Toolbar -> Terminal -> New Terminal
   
   Check python:
       
       python --version
       
7. Create Virtual Environment(venv) and activate
        
        python -m venv .venv
        .\.venv\Scripts\activate
   
8. Install the required dependencies:
        
        pip install -r requirements.txt

9. Create aws account then create iam and change the `aws_config.py`
    
10. Run this project:

        python.exe main.py
         or
        Use main.ipynb

## License  

This project is licensed under the MIT License - see the LICENSE file for details.
