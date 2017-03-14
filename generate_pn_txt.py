import os
import sys
import math
import traceback
import shutil
from os.path import basename, exists, dirname, isfile, abspath, join as pathjoin

from sqlalchemy.orm import sessionmaker

from UserLibs import settings, ComFun
from UserLibs.Config import ParserConfigFile, Gmail, FileAndConsoleLogConfig
from UserLibs.Database import DBEngine, Products, UnstablePartNums, FailedPartNums


'''
this script will be trigger every single day.
1. generate product_with_base_info.txt file which is the main txt file which will used to split into sub files.
2. split product_with_base_info.txt depends on the prod_vm_count and prod_thread_count into sub files, script can read one of sub files 
    and generate a flag which means currrent sub file has been used, try to select other one.
3. generate new_part_num.txt file which is the main txt file which will used to split into sub files.
4. split new_part_num.txt file depends on the pn_vm_count and pn_thread_count.

@author:  jinyuc@fireracker.com
@date: 1/31/2017
'''

def split_txt_into_subfolder(base_info_txt_path, sub_txt_folder_path, each_file_part_num_lines_len):
    cur_date = ComFun.getCurrentDate()
    sub_txt_folder_path = pathjoin(sub_txt_folder_path, 'new')
    if exists(base_info_txt_path):
        if not exists(sub_txt_folder_path): 
            os.makedirs(sub_txt_folder_path)
        else:
            shutil.rmtree(sub_txt_folder_path)
            os.makedirs(sub_txt_folder_path)
        try:
            txt_file_handler = open(base_info_txt_path, 'rb')
        except Exception, e:
            print 'Error raised when attempts to open txt file [%s].' % base_info_txt_path
            return False
        
        all_lines = txt_file_handler.readlines()
        main_part_num_total_len = len(all_lines)
        split_to_file_count = int(math.ceil(main_part_num_total_len / float(each_file_part_num_lines_len)))
        
        read_begin_index = 0
        read_end_index = each_file_part_num_lines_len if main_part_num_total_len > each_file_part_num_lines_len else main_part_num_total_len
        try:
            write_file_handler = ''
            for file_index in range(int(split_to_file_count)):
                write_file_handler = open(os.path.join(sub_txt_folder_path, str(file_index) + '_' + cur_date + '.txt'), 'wb')
                write_file_handler.write(''.join(all_lines[read_begin_index : read_end_index]))
                read_begin_index += each_file_part_num_lines_len
                read_end_index += each_file_part_num_lines_len
                if read_end_index > main_part_num_total_len:
                    read_end_index = main_part_num_total_len
                    
        except Exception, e:
            print 'Error happens when wrting part num to sub txt file. %s' % e
            return False
            
        if write_file_handler: write_file_handler.close()
        if txt_file_handler: txt_file_handler.close()
        if exists(base_info_txt_path): os.remove(base_info_txt_path)
            
        return True
    else:
        LOGGING.error('The main part num txt file cannot be find.')
        return False


def generate_prod_info_from_db_to_txt(prod_write_path):
    
    try:
        prod_writer = open(prod_write_path, 'wb')
    except Exception, e:
        LOGGING.error('Create product txt file failed. %s' % e)
        return False
    
    try:
        all_part_num_in_db_list = SESSION.query(Products).all()
        for prod_item in all_part_num_in_db_list:
            if prod_item.product_store_link:
                write_str = str(prod_item.product_id) + '||' + str(prod_item.manufacturer_id) + '||' + prod_item.product_store_link + '||' + prod_item.part_number + '\n'
                prod_writer.write(write_str)
            else:
                LOGGING.warn('Product stores link is empty. part_num: [%s]' % prod_item.part_number)
    
        prod_writer.close()
    except Exception, e:
        LOGGING.error('Insert product into txt file error: %s' % e)
        return False
    
    return True
    

def get_pn_existed_in_db(pn_existed_write_path):
    try:
        pn_writer = open(pn_existed_write_path, 'wb')
    except Exception, e:
        LOGGING.error('Create part number txt file failed. %s' % e)
        return False
    
    try:
        all_prod_pn_in_db_list = SESSION.query(Products).all()
        for prod_item in all_prod_pn_in_db_list:
            pn_writer.write(prod_item.part_number + '\n')
    
        all_unstable_pn_in_db_list = SESSION.query(UnstablePartNums).all()
        for unstable_item in all_unstable_pn_in_db_list:
            pn_writer.write(unstable_item.part_num + '\n')
            
        all_failed_pn_in_db_list = SESSION.query(FailedPartNums).all()
        for failed_item in all_failed_pn_in_db_list:
            pn_writer.write(failed_item.part_num + '\n')
    
        pn_writer.close()
    except Exception, e:
        LOGGING.error('Insert part number into txt file error: %s' % e)
        return False
    
    return True


def generate_unstable_pn_chk_txt(unstable_write_path):
    try:
        unstable_pn_writer = open(unstable_write_path, 'wb')
    except Exception, e:
        LOGGING.error('Create unstable part num txt file failed. %s' % e)
        return False
    
    try:
        all_unstable_pn_in_db_list = SESSION.query(UnstablePartNums).all()
        for unstable_item in all_unstable_pn_in_db_list:
            unstable_pn_writer.write(unstable_item.part_num + '\n')
        
        unstable_pn_writer.close()
    except Exception, e:
        LOGGING.error('Generate unstable part num file failed. %s' % e)
        return False
    
    return True


def generate_pn_chk_txt(main_part_num_file_path, part_num_in_db_path, part_num_compared_path):
    try:
        main_part_num_reader = open(main_part_num_file_path, 'rb')
    except Exception, e:
        LOGGING.error('Cannot find the mian part number txt file, please check the path: [%s]' % main_part_num_file_path)
        return None
    
    try:
        in_db_part_num_reader = open(part_num_in_db_path, 'rb')
    except Exception, e:
        LOGGING.error('Cannot find the part num in db txt file, please check the path: [%s]' % part_num_in_db_path)
        in_db_part_num_reader = None

    if in_db_part_num_reader:
        
        try:
            part_num_writer = open(part_num_compared_path, 'wb')
        except Exception, e:
            LOGGING.error('Cannot create a txt file for compared new part num. %s' % str(e))
            return main_part_num_file_path
        
        in_db_part_num_list = in_db_part_num_reader.readlines()
        in_db_part_num_list = [in_db_item.strip() for in_db_item in in_db_part_num_list if in_db_item]
        main_part_num_list = main_part_num_reader.readlines()
        main_part_num_list = [main_item.strip() for main_item in main_part_num_list if main_item]
        for main_pn in main_part_num_list:
            if main_pn in in_db_part_num_list:
                pass
            else:
                part_num_writer.write(main_pn + '\n')
        
        part_num_writer.close()
        in_db_part_num_reader.close()
        main_part_num_reader.close()
        
        if exists(part_num_in_db_path): os.remove(part_num_in_db_path)
        
        return part_num_compared_path
    else:
        return main_part_num_file_path


if __name__ == "__main__":
    
    try:
        LOGGING = FileAndConsoleLogConfig(file_name = '.'.join(basename(__file__).split('.')[:-1]), level = 'INFO')
        
        READ_CONFIG = ParserConfigFile(r'./etc/price_metric_config.cfg')
        
        txt_base_path = READ_CONFIG.get_item_value('TXT_FOLDER', 'txt_base_path')
        
        if not exists(txt_base_path):
            LOGGING.error('The path [%s] does not existed, stop!' % txt_base_path)
        
        prod_split_count      = int(READ_CONFIG.get_item_value('DEFAULT', 'prod_split_txt_count'))
        pn_split_count        = int(READ_CONFIG.get_item_value('DEFAULT', 'pn_split_txt_count'))
        pn_unstable_txt_count = int(READ_CONFIG.get_item_value('DEFAULT', 'pn_unstable_txt_count'))
        
        # the products base information will export to prod_base_info_txt
        prod_base_info_txt           = pathjoin(txt_base_path, READ_CONFIG.get_item_value('TXT_FOLDER', 'prod_base_info_txt'))
        prod_sub_base_info_folder    = pathjoin(txt_base_path, READ_CONFIG.get_item_value('TXT_FOLDER', 'prod_sub_base_info_folder'))
        seller_info_chk_folder    = pathjoin(txt_base_path, READ_CONFIG.get_item_value('TXT_FOLDER', 'seller_info_chk_folder'))
        unstable_sub_part_num_folder = pathjoin(txt_base_path, READ_CONFIG.get_item_value('TXT_FOLDER', 'unstable_sub_part_num_folder'))
        
        pn_existed_txt      = pathjoin(txt_base_path, 'pn_existed.txt')
        pn_chk_txt          = pathjoin(txt_base_path, 'pn_chk.txt')
        unstable_pn_chk_txt = pathjoin(txt_base_path, 'unstable_pn_chk.txt')
        all_part_nums_txt   = pathjoin(txt_base_path, READ_CONFIG.get_item_value('TXT_FOLDER', 'all_part_nums_txt'))
        pn_sub_chk_folder   = pathjoin(txt_base_path, READ_CONFIG.get_item_value('TXT_FOLDER', 'pn_sub_chk_folder'))
        
        read_config      = ParserConfigFile(DBEngine.get_common_db_config_full_path())
        db_config_list   = read_config.get_items_list('PRICE_METRICS')
        DBEngine_handler = DBEngine(*db_config_list)
        DBSession        = sessionmaker(autocommit=False, autoflush=False, bind=DBEngine_handler.get_engine())
        
        SESSION = DBSession()
        prod_txt_is_ready = False
        #1. generate the product base informtion, and split into sub txt files.
        if READ_CONFIG.get_item_value('TXT_FOLDER', 'prod_price_check') == 'True':
            if generate_prod_info_from_db_to_txt(prod_base_info_txt):
                split_txt_into_subfolder(prod_base_info_txt, prod_sub_base_info_folder, prod_split_count)
                prod_txt_is_ready = True
                LOGGING.info('Generate product base info txt files DONE.')
        
        if READ_CONFIG.get_item_value('TXT_FOLDER', 'seller_info_check') == 'True':
            # if seller folder is empty, generate it.
            is_ready_to_generate = True
            seller_copy_to_path = pathjoin(seller_info_chk_folder, 'new')
            if exists(seller_copy_to_path):
                src_files = os.listdir(seller_copy_to_path)
                for f in src_files:
                    if isfile(pathjoin(seller_copy_to_path, f)):
                        is_ready_to_generate = False
                        LOGGING.info("seller folder still has files, skip it.")
                        break
            
            if is_ready_to_generate:
                if prod_txt_is_ready:
                    # copy prod info txt to seller folder.
                    LOGGING.info("It's ready copy prod price txt file to seller folder.")
                    copy_from_path = pathjoin(prod_sub_base_info_folder, 'new')
                    scr_files = os.listdir(copy_from_path)
                    for file_name in scr_files:
                        full_file_name = pathjoin(copy_from_path, file_name)
                        if isfile(full_file_name):
                            shutil.copy(full_file_name, seller_copy_to_path)
                    LOGGING.info("Copy seller txt files DONE.")
                else:
                    if generate_prod_info_from_db_to_txt(prod_base_info_txt):
                        split_txt_into_subfolder(prod_base_info_txt, seller_info_chk_folder, prod_split_count)
                        
            
        # generate the new part numbers.
        if READ_CONFIG.get_item_value('TXT_FOLDER', 'new_part_num_check') == 'True':
            if get_pn_existed_in_db(pn_existed_txt):
                pn_chk_txt_path = generate_pn_chk_txt(all_part_nums_txt, pn_existed_txt, pn_chk_txt)
                split_txt_into_subfolder(pn_chk_txt_path, pn_sub_chk_folder, pn_split_count)
                LOGGING.info('Generate new part num txt files DONE.')
        
        # generate the unstable part numbers.
        if READ_CONFIG.get_item_value('TXT_FOLDER', 'unstable_part_num_check') == 'True':
            if generate_unstable_pn_chk_txt(unstable_pn_chk_txt):
                split_txt_into_subfolder(unstable_pn_chk_txt, unstable_sub_part_num_folder, pn_unstable_txt_count)
                LOGGING.info('Generate unstable part num txt files DONE.')
        
        SESSION.close()
        
    except Exception, e:
        LOGGING.error('Error happens in main: [%s]' % traceback.format_exc())
        