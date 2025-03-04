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
            if not sql_statements:  # Kiểm tra nếu danh sách rỗng
                print(f"Danh sách truy vấn trống!: {error_message}")
                return
            
            # Thực thi các lệnh SQL
            for sql in sql_statements:
                cursor.execute(sql)
            
            conn.commit()
            print(message)
        except Exception as e:
            # Nếu có lỗi, rollback toàn bột thay đổi
            conn.rollback()
            print(f'{error_message}: {e}')
        finally:
            conn.rollback()
            # Đóng cursor và connection
            cursor.close()
            conn.close()