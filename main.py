import mysql.connector
import os
import sys

from datetime import datetime, time
from dotenv import load_dotenv

def get_product_duplicates(conn: mysql.connector, start_date: datetime = None, end_date: datetime = None) -> mysql.connector.cursor:
    cursor = conn.cursor(buffered=True)
    query = ("SELECT p.id, p.external_id, upper(p.title), d.category, d.gender, d.productId FROM products p "
            "JOIN ("
                "SELECT MIN(id) as id, upper(category) as category, upper(gender) as gender, REGEXP_REPLACE(tags, '.*ProductID:\s*|,.+', '') as productId, COUNT(*) FROM products "
                "WHERE updated_at between %s AND %s "
                "GROUP BY productId, upper(category), upper(gender) "
                "HAVING COUNT(*) > 1) d "
            "ON p.id = d.id LIMIT 5")
    
    if start_date is None or end_date is None:
        start_date = datetime.combine(datetime.now(), time.min)
        end_date = datetime.combine(datetime.now(), time.max)

    cursor.execute(query, (start_date, end_date))
    return cursor

def insert_product_duplicates(conn: mysql.connector, title: str) -> None:
    cursor = conn.cursor(buffered=True)
    add_product_duplicate = ("INSERT INTO product_duplicates (title) "
               "VALUES (%s)")
    cursor.execute(add_product_duplicate, (title,))
    conn.commit()
    cursor.close()

def insert_product_duplicate_lists(conn: mysql.connector, product_duplicate_id: str, external_id: str, id: str) -> None:
    cursor = conn.cursor(buffered=True)
    add_product_duplicate_lists = ("INSERT INTO product_duplicate_lists (product_duplicate_id, external_id, product_id) "
                "VALUES (%s, %s, %s)")
    cursor.execute(add_product_duplicate_lists, (product_duplicate_id, external_id, id))
    conn.commit()
    cursor.close()

def main(start_date: datetime = None, end_date: datetime = None):
    load_dotenv()

    mysql_user = os.getenv("MYSQL_USER")
    mysql_password = os.getenv("MYSQL_PASSWORD")
    mysql_database = os.getenv("MYSQL_DATABASE")

    conn = mysql.connector.connect(user=mysql_user, password=mysql_password, database=mysql_database)
    select_id_cursor = conn.cursor(buffered=True)
    select_duplicate_cursor = conn.cursor(buffered=True)

    cursor = get_product_duplicates(conn, start_date, end_date)

    for (id, external_id, title, category, gender, product_id) in cursor:
        print("id: {}, external_id: {}, title: {}, productId: {}, category: {}, gender: {}".format(id, external_id, title, product_id, category, gender))
        insert_product_duplicates(conn, title)

        select_product_duplicate = ("SELECT id FROM product_duplicates WHERE title = %s")
        select_id_cursor.execute(select_product_duplicate, (title,))
        (product_duplicate_id, ) = select_id_cursor.fetchall()[0]
        print("product_duplicate_id: " + product_duplicate_id)

        select_duplicate_items = ("SELECT p.id, p.external_id FROM products p "
            "JOIN (SELECT id, REGEXP_REPLACE(tags, '.*ProductID:\s*|,.+', '') as productId FROM products) pi "
            "ON pi.id = p.id "
            "WHERE productId = %s AND "
            "upper(gender) = %s AND "
            "upper(category) = %s AND "
            "updated_at between %s AND %s")
        
        select_duplicate_cursor.execute(select_duplicate_items, (product_id, gender, category, start_date, end_date))

        for (id, external_id) in select_duplicate_cursor:
            insert_product_duplicate_lists(conn, product_duplicate_id, external_id, id)

    cursor.close()
    select_id_cursor.close()
    select_duplicate_cursor.close()
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        date_format = '%Y-%m-%d'
        print(sys.argv[1])
        try:
            date = datetime.strptime(sys.argv[1], date_format)
            start_date = datetime.combine(date, time.min)
            end_date = datetime.combine(date, time.max)
            main(start_date, end_date)
        except Exception as e:
            print(e)
    else:
        print("Need 1 argument for date")
