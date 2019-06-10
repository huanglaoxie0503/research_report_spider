# -*- coding: utf-8 -*-
import pymysql

from research_report_spider.settings import (
    mysql_host,
    mysql_user,
    mysql_password,
    mysql_db,
    mysql_table,

)


conn = pymysql.connect(
    host=mysql_host,
    user=mysql_user,
    passwd=mysql_password,
    db=mysql_db,
    charset="utf8",
    use_unicode=True,
)
cursor = conn.cursor()


def get_article_id(art_id):
    """验证article_id数据库是否已经存在"""
    try:
        sql = "select * from {0} where report_id=%s;".format(mysql_table)
        cursor.execute(sql, (art_id,))
        results = cursor.fetchall()
        if results:
            return results[0][0]
        else:
            return None
    except pymysql.Error as e:
        print(e)
