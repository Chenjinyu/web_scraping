import os, sys
import urllib2
from bs4 import BeautifulSoup
from re import sub
from decimal import Decimal
import time
import base64
import datetime
import traceback
import shutil
import threading
from os.path import basename, exists, dirname, isfile, abspath, join as pathjoin
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from UserLibs import settings, ComFun
from UserLibs.Config import ParserConfigFile, FileAndConsoleLogConfig

__version__ = '0.0.4'

'''
version: 0.0.1
export_prod_price_xml.py reads product store link and put the price into xml file.
xml format:
<?xml version="1.0" encoding="UTF-8" ?>
<product>
    <product_id>5484</product_id>
    <date>2017-02-01</date>
    <stores>
        <store>
            <store_name>AntaresPro.com with b64encode</store_name>
            <trusted_store>0</trusted_store>
            <seller_rating>No rating</seller_rating>
            <reviews>1,454</reviews>
            <base_price>389.99</base_price>
            <tax>0.0</tax>
            <shipping_fee>43.0</shipping_fee>
            <total_price>432.99</total_price>
        </store>
    </stores>
</product>

version 0.0.2
add the time, script will be stop after the day or night time. 
script will detect if present is day, it will run the threading instance with the days;
if preset is night, it will run the threading instance with the night.

version 0.0.3
use Beautifulsoup scraping web information.  
def launch_beautifulsoup()

version 0.0.4
change datetime column to date. price_metrics table and uspProductPriceInsertXml SP.


@author:    jinyuc@fireracker.com
@date:      01/31/2017
'''

def get_review_number(review_str):
    if '(' in review_str and ')' in review_str:
        return (review_str.strip())[1:-1]
    else:
        return review_str
    
    
def get_price_numeric(money_str):
    tax = '0.0'
    shipping = '0.0'
    if 'and' in money_str:
        tmp_list = money_str.split('and')
        tmp_list = [ t.strip() for t in tmp_list]
        for tmp in tmp_list:
            if '+' in tmp:
                float_price = (tmp.split(' ')[0])[2:]
            else:
                float_price = (tmp.split(' ')[0])[1:]
                
            if 'tax' in tmp:
                tax = float_price
            elif 'shipping' in tmp:
                shipping = float_price
    else:
        if '+' in money_str:
            float_price = (money_str.split(' ')[0])[2:]
        else:
            float_price = (money_str.split(' ')[0])[1:]
            
        if 'tax' in money_str:
            tax = float_price
        elif 'shipping' in money_str:
            shipping = float_price

    return float(Decimal(sub(r'[^\d.]', '', str(tax)))), float(Decimal(sub(r'[^\d.]', '', str(shipping))))


def get_prices_from_paging_store_list(driver_instance, maunfacturer_id, product_id, part_num):
    
    store_count_flag = 0
    prod_store_price_xml = '<?xml version="1.0" encoding="UTF-8" ?><product><product_id>' + str(product_id) + '</product_id><date>' + ComFun.getCurrentDateFormatDash() + '</date><stores>' 
    store_count, store_xml = get_prices_from_online_stores(driver_instance, maunfacturer_id, product_id)
    next_btn_xpath = "//div[@class='pag-bottom-links']//div[@id='online-pagination']//div[@id='online-next-btn' and contains(@href, '/shopping/product/')]"
    prod_store_price_xml += store_xml
    
    while(True):
        store_count_flag +=  store_count
        if store_count_flag >= STORE_COUNT_PER_PROD:
            break
        try:
            next_btn_link = driver_instance.find_element_by_xpath(next_btn_xpath)
            next_prod_list_link = next_btn_link.get_attribute('href')
            
            if GOOGLE_BASE_LINK not in next_prod_list_link:
                 next_prod_list_link = GOOGLE_BASE_LINK + next_prod_list_link
                 
            driver_instance.get(next_prod_list_link)
            time.sleep(3)
            
            store_count, store_xml = get_prices_from_online_stores(driver_instance, maunfacturer_id, product_id)
            
            prod_store_price_xml += store_xml
        except Exception, e:
            LOGGING.info('No paging anymore.')
            break
    
    prod_store_price_xml += '</stores></product>'
    return export_price_xml(PRICE_XML_BASE_PATH, part_num, prod_store_price_xml)
    

def export_price_xml(write_xml_path, part_num, price_xml_str):
    if not exists(write_xml_path): os.makedirs(write_xml_path)
    
    try:
        xml_writer = open(pathjoin(write_xml_path, part_num + '_' + CUR_DATE + '.xml'), 'wb')
        xml_writer.write(price_xml_str)
        
        if xml_writer:
            xml_writer.close()
        return True
    except Exception, e:
        LOGGING.error('Save XML file failed. %s' % e)
        return False


def get_prices_from_online_stores(driver_instance, maunfacturer_id, product_id):
    try:
        online_store_rows = driver_instance.find_elements_by_xpath("//table[@id='os-sellers-table']//tr[@class='os-row']")
        store_count = len(online_store_rows)
    except Exception, e:
        LOGGING.error('Cannot find the element, Script exits! [%s]' % traceback.format_exc())
        return 0, ''
     
    td_class_seller_name_col = 'os-seller-name'
    span_class_trusted_store = '_Fuj'
    td_class_rating_col = 'os-rating-col'
    div_class_rating_label = '_OBj'
    a_class_rating_reviews = 'shop__secondary sh-rt__seller'
    span_class_no_rating = 'shop__secondary'
    td_class_detail_col = 'os-details-col'
    td_class_price_col = 'os-price-col'
    span_class_base_price = 'os-base_price'
    div_class_base_price_description = 'os-total-description'
    td_class_total_price = 'os-total-col'
    
    all_stores_node_xml = ''
    
    for store in online_store_rows:
        # store name, column 1.
        store_node_xml = '<store>'
        store_name_str = ''
        try:
            store_name = store.find_element_by_xpath(".//td[@class='%s']//span[@class='os-seller-name-primary']/a" % td_class_seller_name_col)
            if 'ebay' in str(store_name.text).lower():
                continue
            store_name_str = store_name.text
            LOGGING.info('Store Name: %s' % store_name_str )
        except Exception, e:
            LOGGING.error('Cannot find store name element:. %s.'  % traceback.format_exc())
        store_node_xml += '<store_name>' + base64.b64encode(store_name_str) + '</store_name>'
        
        is_trusted_store = 0
        try:
            trusted_store = store.find_element_by_xpath(".//td[@class='%s']//span[@class='%s']" % (td_class_seller_name_col, span_class_trusted_store))
            if str(trusted_store.text).lower() == 'trusted store':
                is_trusted_store = 1
                LOGGING.info('Trusted Store.')
        except Exception, e:
            LOGGING.info('It is not Trusted Store.')
        
        store_node_xml += '<trusted_store>' + str(is_trusted_store) + '</trusted_store>'
        
        # store seller rating, column 2.
        store_rating_str = ''
        review_str = ''
        try:
            store_no_rating = store.find_element_by_xpath(".//td[@class='%s']//span[@class='%s']" % (td_class_rating_col, span_class_no_rating))
            store_rating_str = store_no_rating.text
            LOGGING.info('Store Rating: %s' % store_rating_str)
        except Exception, e:
            try:
                store_seller_rating = store.find_element_by_xpath(".//td[@class='%s']//span/div[@class='%s']" % (td_class_rating_col, div_class_rating_label))
                store_rating_str = str(store_seller_rating.get_attribute('aria-label'))
                LOGGING.info('Store Rating: %s' % store_rating_str)
            except Exception, e:
                LOGGING.error('Cannot find store seller rating element. %s.'  % traceback.format_exc())
            
            try:
                store_seller_rating_reviews = store.find_element_by_xpath(".//td[@class='%s']//a[@class='%s']" % (td_class_rating_col, a_class_rating_reviews))
                review_str = get_review_number(store_seller_rating_reviews.text)
                LOGGING.info('Store Reviews: %s' % review_str)
            except Exception, e:
                review_str = 'No Review'
                LOGGING.info('Store No Review.')
         
        store_node_xml += '<seller_rating>' + store_rating_str + '</seller_rating><reviews>' + review_str + '</reviews>'
        # store base price, column 4.
        store_base_price_float = 0.0
        store_tax_float = 0.0
        store_shipping_float = 0.0
        try:
            store_price = store.find_element_by_xpath(".//td[@class='%s']//span[@class='%s']" % (td_class_price_col, span_class_base_price))
            store_base_price_float = float(Decimal(sub(r'[^\d.]', '', (store_price.text)[1:])))
            LOGGING.info('Store Base Price: %s' % store_price.text)
            store_price_description = store.find_element_by_xpath(".//td[@class='%s']//div[@class='%s']" % (td_class_price_col, div_class_base_price_description))
            store_tax_float, store_shipping_float = get_price_numeric(store_price_description.text)
            LOGGING.info('Store Base Price for fee: %s' % store_price_description.text)
        except Exception, e:
            LOGGING.error('Cannot find store base price element.. %s.'  % traceback.format_exc())
        
        store_node_xml += '<base_price>' + str(store_base_price_float) + '</base_price><tax>' + str(store_tax_float) + '</tax><shipping_fee>' + str(store_shipping_float) + '</shipping_fee>'
        # store total price column 5.
        store_total_price_float = 0.0
        try:
            store_total_price = store.find_element_by_xpath(".//td[@class='%s']" % (td_class_total_price))
            LOGGING.info('Store Total Price: %s' % store_total_price.text)
            if (store_total_price.text).strip():
                store_total_price_float = float(Decimal(sub(r'[^\d.]', '', (store_total_price.text)[1:])))
            else:
                # because in the google shopping page, total price maybe is empty, but it has base price.
                if store_base_price_float > 0.0:
                    store_total_price_float = store_base_price_float
                    
        except Exception, e:
            LOGGING.error('Cannot find store total price element.. %s.'  % traceback.format_exc())
        
        store_node_xml += '<total_price>' + str(store_total_price_float) + '</total_price></store>'
        all_stores_node_xml += store_node_xml
    
    
    return store_count, all_stores_node_xml


def launch_beautifulsoup(product_id, manuf_id, prod_store_link, part_num):
    
    prod_store_price_xml = '<?xml version="1.0" encoding="UTF-8" ?><product><product_id>' + str(product_id) + '</product_id><date>' + ComFun.getCurrentDateFormatDash() + '</date><stores>'
    try:
        html = urllib2.urlopen(prod_store_link).read()
        bsObj = BeautifulSoup(html, "html.parser")
        online_stores_trs = bsObj.find('table', {'id' : 'os-sellers-table'}).find_all('tr', {'class': 'os-row'})
        
        for store in online_stores_trs:
            store_xml = '<store>'
            store_name_str = store.find('span', {'class': 'os-seller-name-primary'}).get_text().strip()
            if 'ebay' in store_name_str.lower():
                continue
            store_xml += '<store_name>' + base64.b64encode(store_name_str) + '</store_name>'
            store_xml += '<trusted_store>0</trusted_store>'
            try:
                store_xml += '<seller_rating>' + store.find('td', {'class': 'os-rating-col'}).find('div', {'class': '_OBj'}).get('aria-label') + '</seller_rating>'
            except Exception, e:
                store_xml += '<seller_rating>0</seller_rating>'
            try:
                review_str = get_review_number(store.find('td', {'class': 'os-rating-col'}).find('a', {'class': 'shop__secondary sh-rt__seller'}).get_text())
                store_xml += '<reviews>' + review_str + '</reviews>'
            except Exception, e:
                store_xml += '<reviews>0</reviews>'
            try:
                base_price = store.find('td', {'class': 'os-price-col'}).find('span', {'class': 'os-base_price'}).get_text()
                store_base_price_float = float(Decimal(sub(r'[^\d.]', '', base_price[1:])))
                store_xml += '<base_price>' + str(store_base_price_float) + '</base_price>'
            except Exception, e:
                store_xml += '<base_price>0.0</base_price>'
            try:
                store_tax_float, store_shipping_float =  get_price_numeric(store.find('td', {'class': 'os-price-col'}).get_text())
                store_xml += '<tax>' + str(store_tax_float) + '</tax><shipping_fee>' + str(store_shipping_float) + '</shipping_fee>' 
            except Exception, e:
                store_xml += '<tax>0.0</tax><shipping_fee>0.0</shipping_fee>'
            try:
                total_price = store.find('td', {'class': 'os-total-col'}).get_text()
                store_total_price_float = float(Decimal(sub(r'[^\d.]', '', total_price[1:])))
                store_xml += '<total_price>' + str(store_total_price_float) + '</total_price>'
            except Exception,e:
                store_xml += '<total_price>0.0</total_price>'
                
            store_xml += '</store>'
            prod_store_price_xml += store_xml
        prod_store_price_xml += '</stores></product>'
        return export_price_xml(PRICE_XML_BASE_PATH, part_num, prod_store_price_xml)
    except Exception, e:
        LOGGING.error('BeautifulSoup export xml file failed. %s' % str(e))

    
def launch_webdriver(product_id, manuf_id, prod_store_link, part_num):
    try:
        driver = webdriver.Chrome(executable_path=r'driver/chromedriver.exe')  # Optional argument, if not specified will search path.
    except Exception, e:
        LOGGING.error('Part Number [%s], Error happens: %s' % (part_num, traceback.format_exc()))
        return
     
    try:
        driver.get(prod_store_link)
        LOGGING.info('Get Product Store Link provided by DB: [%s].' % prod_store_link)
        time.sleep(3)
        get_prices_from_paging_store_list(driver, manuf_id, product_id, part_num)
        driver.quit()
    except Exception, e:
        LOGGING.error('Part Number [%s], Error happens: %s' % (part_num, traceback.format_exc()))
        if driver:
            driver.quit()


def pickup_a_prod_file():
    for subdir, dirs, files in os.walk(NEW_PROD_BASE_INFO_FOLDER):
        for pn_f in files:
            if pn_f.endswith('.txt'):
                try:
                    ongoing_path = pathjoin(ONGOING_PROD_BASE_INFO_FOLDER, pn_f)
                    shutil.move(pathjoin(NEW_PROD_BASE_INFO_FOLDER, pn_f), ongoing_path)
                    return ongoing_path
                except Exception, e:
                    LOGGING.error('Move file to going file %s error' % ongoing_path)
                    continue
                
    return False


def begin_read_prod_prices():
    
    if TIME_FLAG == 'day_time': break_time_str = NIGHT_TIME 
    elif TIME_FLAG == 'night_time': break_time_str = DAY_TIME
    else:
        break_time_str = False
    if not ComFun.is_day_night_time_switch(break_time_str):
        LOGGING.info('The time is close to the time node. break.')
        return
    
    ongoing_path = pickup_a_prod_file()
    LOGGING.info('Pick up the file: %s' % ongoing_path)

    while(ongoing_path):
        if not ongoing_path:
            LOGGING.error('No part num file to read in new foler')
            continue 
         
        try:
            FILE_HANDLER = open(ongoing_path, 'r')
            part_num_list = FILE_HANDLER.readlines()
            for pn in part_num_list:
                part_num_list = (pn.strip()).split('||')
                product_id = part_num_list[0]
                manuf_id = part_num_list[1]
                prod_store_link = part_num_list[2]
                part_num = part_num_list[3]
                LOGGING.info('--------------------- Part Num [ %s ], Product ID [%s] ---------------------->>>' % (part_num, str(product_id)))
                launch_beautifulsoup(product_id, manuf_id, prod_store_link, part_num)
                LOGGING.info('<<<--------------------- Part Num [ %s ], Product ID [%s] ----------------------' % (part_num, str(product_id)))
            
            FILE_HANDLER.close()
            
            try:
                # moving txt file to succ folder.
                shutil.move(ongoing_path, pathjoin(SUCC_PROD_BASE_INFO_FOLDER, basename(ongoing_path)))
                LOGGING.info('Prod store link file[%s] has been moved to SUCC folder!' % basename(ongoing_path))
            except Exception, e:
                LOGGING.error('Move file from [%s] to [%s] error: %s' % (ongoing_path, pathjoin(SUCC_PROD_BASE_INFO_FOLDER, basename(ongoing_path)), traceback.format_exc()))
          
        except Exception, e:
            LOGGING.error('begin_read_prod_prices Error %s' % e)
            
        if not ComFun.is_day_night_time_switch(break_time_str):
            LOGGING.info('The time is close to the time node. break.')
            break
        
        ongoing_path = pickup_a_prod_file()


if __name__ == "__main__":
    try:
        '''
        read products table and output part number and product store link into txt file, 
        these threading will read each txt file.
        '''
        TIME_FLAG = False
        script_file_name = '.'.join(basename(__file__).split('.')[:-1])
        LOGGING = FileAndConsoleLogConfig(file_name = script_file_name, level = 'INFO')
        
        status, msg = ComFun.grant_script_running(script_file_name)
        if not status:
            LOGGING.error(msg + ', exit!')
            sys.exit()
        
        GOOGLE_BASE_LINK = 'https://www.google.com'
        READ_CONFIG = ParserConfigFile(r'./etc/price_metric_config.cfg')
        
        if (READ_CONFIG.get_item_value('EMAIL', 'is_send_alert')).lower() == 'true':
            IS_SEND_ALERT = True
        else:
            IS_SEND_ALERT = False
        
        # how many stores' price data of the related product will be pulled out.
        STORE_COUNT_PER_PROD = int(READ_CONFIG.get_item_value('DEFAULT', 'store_count_per_prod'))
        prod_sub_base_info_folder_from = pathjoin(READ_CONFIG.get_item_value('TXT_FOLDER', 'txt_base_path'), 
                                                READ_CONFIG.get_item_value('TXT_FOLDER', 'prod_sub_base_info_folder'))
        
        CUR_DATE = ComFun.getCurrentDate()
        NEW_PROD_BASE_INFO_FOLDER     = pathjoin(prod_sub_base_info_folder_from, 'new')
        ONGOING_PROD_BASE_INFO_FOLDER = pathjoin(prod_sub_base_info_folder_from, 'ongoing')
        SUCC_PROD_BASE_INFO_FOLDER    = pathjoin(prod_sub_base_info_folder_from, 'succ', CUR_DATE)
        FAILED_PROD_BASE_INFO_FOLDER  = pathjoin(prod_sub_base_info_folder_from, 'failed', CUR_DATE)
        
        if not exists(ONGOING_PROD_BASE_INFO_FOLDER): os.makedirs(ONGOING_PROD_BASE_INFO_FOLDER)
        if not exists(SUCC_PROD_BASE_INFO_FOLDER):    os.makedirs(SUCC_PROD_BASE_INFO_FOLDER)
        if not exists(FAILED_PROD_BASE_INFO_FOLDER):  os.makedirs(FAILED_PROD_BASE_INFO_FOLDER)
        
        try:
            DAY_TIME = READ_CONFIG.get_item_value('DEFAULT', 'day_time')
            NIGHT_TIME = READ_CONFIG.get_item_value('DEFAULT', 'night_time')
        except Exception, e:
            DAY_TIME = NIGHT_TIME = False
        
        
        if not DAY_TIME and not NIGHT_TIME:
            PROD_EXPORT_XML_THREAD_COUNT = int(READ_CONFIG.get_item_value('DEFAULT', 'prod_export_xml_thread_count_day'))
        else:
            if ComFun.compareCurTimewithGivenTime(DAY_TIME) and not ComFun.compareCurTimewithGivenTime(NIGHT_TIME):
                PROD_EXPORT_XML_THREAD_COUNT = int(READ_CONFIG.get_item_value('DEFAULT', 'prod_export_xml_thread_count_day'))
                TIME_FLAG = 'day_time'
            else:
                PROD_EXPORT_XML_THREAD_COUNT = int(READ_CONFIG.get_item_value('DEFAULT', 'prod_export_xml_thread_count_night'))
                TIME_FLAG = 'night_time'
        
        PRICE_XML_BASE_PATH = pathjoin(READ_CONFIG.get_item_value('XML_FOLDER', 'price_xml_base_path'), 'new')
        
        LOGGING.info('Script will be run with threading count[%s]' % str(PROD_EXPORT_XML_THREAD_COUNT))
        
        threads = []
        for file_index in range(PROD_EXPORT_XML_THREAD_COUNT):
            t = threading.Thread(target=begin_read_prod_prices)
            threads.append(t)
            t.start()
              
        for t in threads:
            t.join()
        
        if exists(script_file_name): ComFun.remove(script_file_name)
        LOGGING.info('DONE!.')
        
    except Exception, e:
        if LOGGING: LOGGING.error('Something wrong in the main: %s.' % traceback.format_exc())
        else: print traceback.format_exc()
        if exists(script_file_name): ComFun.remove(script_file_name)
