import mysql.connector

class Executor:
    def __init__(self):
        pass

    def execute(self, sql_statements, message, error_message):
        pass

class MysqlExecutor(Executor):
    def execute(self, sql_statements, message, error_message):
        try:
            cursor.close()  # Đóng cursor trên trên lớp trên
        except Exception:
            pass
        # Kết nối đến MySQL
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='12345',
            database='walmart'
        )
        cursor = conn.cursor()
        try:
            if not sql_statements: 
                print(f"Empty SQL statements!: {error_message}")
                return
            
            # Execute SQL statements
            for sql in sql_statements:
                cursor.execute(sql)
            
            conn.commit()
            print(message)
        except Exception as e:
            # Rollback if error
            conn.rollback()
            print(f'{error_message}: {e}')
        finally:
            conn.rollback()
            # Close cursor and connection
            cursor.close()
            conn.close()