# Fashion Retail ETL project

## Overview 
This project is a ETL (Extract, Transform, Load) project that demonstrates the process of extracting data from csv file using Apache Spark(PySpark), performing various transformations, and finally loading the processed data into a MySQL database. The entire process is implemented in Python, and SQL.

## Table of Contents
- [Introduction](#introduction)
- [Technologies Used](#technologies-used)
- [Transformation](##transformation)
- [Installation](#installation)
- [License](##license)

## Introduction  
Giới thiệu chi tiết về dự án...

## Technologies Used  

    - Python: Programming language used for implementing the Spark ETL process.
    - PySpark: Distributed data processing framework used for scalable ETL operations.
    - MySQL: The final processed data is loaded into a MySQL database.

## Transformation  

The Spark ETL process involves the following transformation steps on the extracted data:

- Date Format Change:
     - Convert date columns to a consistent date format.
     - Change `review_rating` from Doble to Float.
- Dealing with missing datas: Replace missing datas with general values such as: `Unknown`, `-1`.
- Dealing with duplicated data

After clean up data, load cleaned data to Mysql database:

- Use 

## Installation  
1. Download and Install Python for Windows  
   Link: [Download Python](https://www.python.org/downloads/)

2. Download and Install MySQL for Windows  
   Go to: [Download MySQL](https://dev.mysql.com/downloads/installer/)  
   - Select version: choose version  
   - Select Operating System: your system  
   - Configure: [MySQL Installation Guide](https://www.geeksforgeeks.org/how-to-install-mysql-in-windows/)

3. Download JDK 8 or 11:  
   Link: [Download JDK](https://drive.google.com/drive/folders/1YiKNzQhiNOz_S_gCyjd_IbWwcAAnvqlx?usp=sharing)  
   - Run MSI file to install

   or
   
   - Put the `bin` folder in the zip file to `"C:\Program Files\Java\jdk-11"` (create if not exists)

5. Clone this repository to your local machine:
   ```sh
   git clone https://github.com/your-username/spark-etl-project.git
   cd spark-etl-project
6. Open in Vscode
   ```sh
   code .
7. Open terminal
   
       Ctrl + "`"
        Or
       Toolbar -> Terminal -> New Terminal
   
   Check version:
       
       python --version
9. Create Virtual Environment(venv) and activate
        
        python -m venv .venv
        .\.venv\Scripts\activate
   
11. Install the required dependencies:
        
        pip install -r requirements.txt
13. Run main.py:

        python.exe main.py
        or
        Use main.ipynb

## Transformation  
Các bước chuyển đổi dữ liệu...

## Error Handling  
Xử lý lỗi...



## Contributing  
Cách đóng góp cho dự án...

## License  
Thông tin giấy phép...
