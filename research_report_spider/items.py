# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from research_report_spider.settings import mysql_table


class ResearchReportSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    # 作为id，唯一主键
    report_id = scrapy.Field()
    # 股票代码
    stock_code = scrapy.Field()
    # 股票名称
    stock_name = scrapy.Field()
    # 日期
    publish_time = scrapy.Field()
    # 作者
    author = scrapy.Field()
    # 研报标题
    title = scrapy.Field()
    # 原文评级
    original_rating = scrapy.Field()
    # 评级变动
    rating_changes = scrapy.Field()
    #
    rating_adjust_mark_type = scrapy.Field()
    # 机构
    org_name = scrapy.Field()
    # 内容
    content = scrapy.Field()
    # pdf链接
    pdf_link = scrapy.Field()
    # 文件名
    filename = scrapy.Field()
    # 文件存储路径
    save_path = scrapy.Field()

    def get_insert_sql(self):
        # 插入：sql语句
        insert_sql = """
                        insert into {0}(report_id,stock_code,stock_name,publish_time,author,title,
                        original_rating,rating_changes,rating_adjust_mark_type,org_name,content,pdf_link,save_path)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """.format(mysql_table)

        params = (
            self['report_id'],
            self['stock_code'],
            self['stock_name'],
            self['publish_time'],
            self['author'],
            self['title'],
            self['original_rating'],
            self['rating_changes'],
            self['rating_adjust_mark_type'],
            self['org_name'],
            self['content'],
            self['pdf_link'],
            self['save_path']
        )
        return insert_sql, params
