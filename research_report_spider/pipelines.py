# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import scrapy
import logging
import pymysql
from research_report_spider.settings import mysql_host, mysql_user, mysql_password, mysql_db
from research_report_spider.items import ResearchReportSpiderItem
from scrapy.pipelines.files import FilesPipeline


class MysqlPipeline(object):
    """同步的方式将数据保存到数据库：方法二"""

    def __init__(self):
        self.conn = pymysql.connect(
            host=mysql_host,
            user=mysql_user,
            passwd=mysql_password,
            db=mysql_db,
            charset="utf8",
            use_unicode=True,
        )
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        try:
            # 插入
            if isinstance(item, ResearchReportSpiderItem):
                self.do_insert(item)
            else:
                logging.info("Error Data")
        except pymysql.Error as e:
            logging.error("-----------------insert faild-----------")
            logging.error(e)
            print(e)

        return item

    def close_spider(self, spider):
        try:
            self.conn.close()
            logging.info("mysql already close")
        except Exception as e:
            logging.info("--------mysql no close-------")
            logging.error(e)

    def do_insert(self, item):
        try:
            insert_sql, params = item.get_insert_sql()

            self.cursor.execute(insert_sql, params)
            self.conn.commit()
            logging.info("----------------insert success-----------")
        except pymysql.Error as e:
            print(e)


class MyFilesPipeline(FilesPipeline):
    """
    下载PDF文件
    """
    def get_media_requests(self, item, info):
        for url in item["pdf_link"]:
            if url:
                yield scrapy.Request(url, meta={"item": item})

    def file_path(self, request, response=None, info=None):
        item = request.meta["item"]
        filename = item["filename"]
        return filename

    def item_completed(self, results, item, info):
        file_paths = [x['path'] for ok, x in results if ok]
        if not file_paths:
            pass
        item["filename"] = file_paths
        return item
