import scrapy
from scrapy.http import HtmlResponse
from scrapy.selector import Selector
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.webdriver import WebDriver
from selenium.webdriver.firefox.webelement import FirefoxWebElement
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import glob
import numpy as np
import pandas as pd
from datetime import date, timedelta

# from scrapy_splash import SplashRequest


class EngDataSpider(scrapy.Spider):
    name = 'eng_data'
    allowed_domains = ['www.regelleistung.net']
    start_urls = ['https://www.regelleistung.net/ext/data/?lang=en']

    def __init__(self, name=None, **kwargs):
        fls = glob.glob('/home/sarthak/Visual Studio/Web Scraping Projects/MeritGroup/regelleistung/downloads/*.CSV')
        for f in fls:
            os.remove(f)
        
        self.data_table = None
        
        super().__init__(name=name, **kwargs)
        pass

    def parse_table(self, tableElement: Selector, dt_type: str):
        head_elem = tableElement.xpath('.//div[@class="dataTables_scrollHead"]')
        body_elem = tableElement.xpath('.//div[@class="dataTables_scrollBody"]')

        #Set the shape of the output array
        headers = head_elem.xpath('.//th')
        cols = [''.join(h.xpath('.//text()').getall()) for h in headers]
        dt_table = np.empty(shape=[0,len(cols)])

        data_rows = body_elem.xpath('.//table[@id="data-table"]/tbody/tr')
        for row in data_rows:
            dt_table = np.vstack((dt_table, np.array(row.xpath('.//td/text()').getall())))

        df = pd.DataFrame(dt_table, columns=cols)
        self.logger.info(df.head())
        df['dt_type'] = dt_type
        if self.data_table is None:
            self.data_table = df
        else:
            self.data_table = self.data_table.append(df)

    def parse(self, response: HtmlResponse):
        
        page_src = response.meta['driver']
        wait = WebDriverWait(page_src, 20)

        from_date_box = page_src.find_element_by_id('form-from-date')
        from_date_box.clear()
        from_date_box.send_keys((date.today() - timedelta(days=1)).strftime("%d.%m.%Y"))

        # dwl_btn = page_src.find_element_by_id('form-download')
        # dwl_btn.click()

        dt_dd = page_src.find_element_by_id('form-type')
        dt_dd_options = [option.get_attribute('value') for option in dt_dd.find_elements_by_xpath('.//option')]

        for option in dt_dd_options:
            dt_dd = page_src.find_element_by_id('form-type')

            # Select the desired option in the drop-down
            dt_dd_op = Select(dt_dd)
            dt_dd_op.select_by_value(option)

            # Click the show button
            submit_btn = page_src.find_element_by_id('submit-button')
            submit_btn.click()
            wait.until(EC.visibility_of_element_located((By.ID, 'data-table_wrapper')))
            tbl_element = Selector(text=page_src.page_source).xpath('//div[@id="data-table_wrapper"]')

            # Parse the table into Numpy Array
            self.parse_table(tbl_element, option)
            wait.until(EC.visibility_of_element_located((By.ID, 'form-type')))
        
        self.data_table.to_csv('/home/sarthak/Visual Studio/Web Scraping Projects/MeritGroup/regelleistung/downloads/consolidated.csv')
        yield {
            'data' : self.data_table
        }