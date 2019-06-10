# -*- coding: utf-8 -*-
import scrapy
import datetime
import logging
import json

from selenium import webdriver

from research_report_spider.common import operation
from research_report_spider.items import ResearchReportSpiderItem


class ReportSpider(scrapy.Spider):
    name = 'report'
    allowed_domains = ['gw.datayes.com']
    start_urls = ['http://gw.datayes.com/']

    dt = datetime.datetime.now().strftime('%Y-%m-%d')
    today = dt.replace('-', '')

    base_url = 'https://gw.datayes.com/rrp_adventure/web/search?'
    headers = {
        "Origin": "https://robo.datayes.com",
        "Referer": "https://robo.datayes.com/v2/fastreport/company?subType=%E4%B8%8D%E9%99%90&induName=",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36",
    }
    url = "https://gw.datayes.com/rrp_adventure/web/search?pageNow={0}&authorId=&isOptional=false&orgName=&reportType=COMPANY&secCodeList=&reportSubType=&industry=&ratingType=&pubTimeStart={1}&pubTimeEnd={1}&type=EXTERNAL_REPORT&pageSize=40&sortOrder=desc&query=&minPageCount=&maxPageCount="

    def start_requests(self):
        # 获取 cookie
        cookie = self.get_cookies()

        for page in range(1, 5):
            yield scrapy.Request(
                self.url.format(page, self.today),
                headers=self.headers,
                cookies=cookie,
                meta={"page": page, "cookie": cookie}
            )

    def parse(self, response):
        page = response.meta.get('page')
        logging.info('正在抓取第{0}页'.format(page))
        status = response.status
        logging.info(status)
        result = response.text
        result = json.loads(result)

        message = result['message']
        if message != 'success':
            logging.info('message为：{0},请求失败！'.format(message))
            return

        data_all = result['data']['list']
        for info in data_all:
            data = info['data']
            report_id = data['id']
            stock_name = data['companyName']
            author = data['author']
            title = data['title']
            # id判断
            is_ar_id = operation.get_article_id(report_id)
            if is_ar_id:
                logging.info('id:{0}已经存在'.format(is_ar_id))
                continue

            content = data['abstractText']
            if content:
                content = content.replace('\u3000', '').strip()
            else:
                content = content
            stock_code_info = data['stockInfo']
            if stock_code_info is None:
                stock_code = None
            else:
                stock_code = stock_code_info['stockId']

            file_name = '{0}-{1}.pdf'.format(stock_code, title)
            org_dt = data['publishTime'].split('T')
            publish_time = org_dt[0]

            keys = publish_time.split('-')
            year = keys[0]

            filename = "/{0}/{1}/{2}".format(year, publish_time, file_name)

            item = ResearchReportSpiderItem()
            # 作为id，唯一主键
            item['report_id'] = report_id
            # 股票代码
            item['stock_code'] = stock_code
            # 股票名称
            item['stock_name'] = stock_name
            # 日期
            item['publish_time'] = publish_time
            # 作者
            item['author'] = author
            # 研报标题
            item['title'] = title
            # 原文评级
            item['original_rating'] = data['ratingContent']
            # 评级变动
            item['rating_changes'] = data['ratingType']
            #
            item['rating_adjust_mark_type'] = data['ratingAdjustMarkType']
            #  机构
            item['org_name'] = data['orgName']
            # 内容
            item['content'] = content
            # pdf链接
            item['pdf_link'] = [data['s3Url']]
            # 文件名
            item['filename'] = filename
            # 文件存储路径
            item['save_path'] = "H-hezudao/Research_Report{0}".format(filename)

            yield item

    def get_cookies(self):
        # Firefox无头浏览器模式
        from selenium.webdriver.firefox.options import Options
        firefox_options = Options()
        firefox_options.set_headless()
        driver = webdriver.Firefox(firefox_options=firefox_options)

        url = 'https://robo.datayes.com/v2/fastreport/company?subType=%E4%B8%8D%E9%99%90&induName='
        driver.get(url)
        # 获取cookie列表
        cookie_list = driver.get_cookies()
        # 格式化打印cookie
        cookie_dict = {}
        for cookie in cookie_list:
            cookie_dict[cookie['name']] = cookie['value']
        driver.quit()
        logging.info('Firefox 已经 quit')
        return cookie_dict
