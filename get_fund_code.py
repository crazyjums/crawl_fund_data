import demjson,re
import logging,sys
import os,time
from MysqlDB import MysqlFundCode
from toolkit import LOG_FORMAT,DATE_FORMAT,get_year_mon_day,get_class_name,get_HTML_content

def get_fund_code_lists_by_page(page):
    try:
        '''https://fundapi.eastmoney.com/fundtradenew.aspx
        ?ft=zs&sc=1n&st=desc&pi=1&pn=100&cp=&ct=&cd=&ms=&fr=&plevel=&fst=&ftype=&fr1=&fl=0&isab=1
        https://fundapi.eastmoney.com/fundtradenew.aspx?ft=zs&sc=1n&st=desc&pi=3&pn=100&cp=&ct=&cd=&ms=&fr=&plevel=&fst=&ftype=&fr1=&fl=0&isab=1'''
        url = "https://fundapi.eastmoney.com/fundtradenew.aspx?ft=zs&sc=1n&st=desc&pi={}&pn=100&cp=&ct=&cd=&ms=&fr=&plevel=&fst=&ftype=&fr1=&fl=0&isab=1".format(page)
        content = get_HTML_content(url)
        _ = re.sub("\|","  ",content[15:-1])
        d = demjson.decode(_)
        fund_info_lists = []
        for i in d["datas"]:
            i = i.split("  ")
            temp_dict = {}
            temp_dict["fund_code"] = i[0]
            temp_dict["fund_name"] = i[1]
            temp_dict["fund_type"] = i[2]
            temp_dict["date"] = i[3]
            temp_dict["nvalue_pu"] = i[4]
            temp_dict["day_growth_rate"] = i[5]
            temp_dict["a_week_rate"] = i[6]
            temp_dict["a_month_rate"] = i[7]
            temp_dict["_3_month_rate"] = i[8]
            temp_dict["_6_month_rate"] = i[9]
            temp_dict["a_year_rate"] = i[10]
            temp_dict["_2_year_rate"] = i[11]
            temp_dict["_3_year_rate"] = i[12]
            temp_dict["from_this_year"] = i[13]
            temp_dict["from_found_year"] = i[14]
            temp_dict["poundage"] = i[-2]
            temp_dict["purchase_money"] = i[-5]
            fund_info_lists.append(temp_dict)
        logging.info("{} | {}".format("第 {} 页数据抓取完成。".format(page),sys._getframe().f_code.co_name))
        return fund_info_lists
    except Exception as e:
        logging.error("{} | {}".format(e, sys._getframe().f_code.co_name))



def get_total_page_num():
    try:
        url = "https://fundapi.eastmoney.com/fundtradenew.aspx?ft=zs&sc=1n&st=desc&pi=1&pn=100&cp=&ct=&cd=&ms=&fr=&plevel=&fst=&ftype=&fr1=&fl=0&isab=1"
        content = get_HTML_content(url)
        _ = re.sub("\|", "  ", content[15:-1])
        d = demjson.decode(_)
        total_page = (int(d["allPages"]))
        logging.info("{} pages | {}".format(total_page, sys._getframe().f_code.co_name))
        return total_page
    except Exception as e:
        logging.error("{} | {}".format(e, sys._getframe().f_code.co_name))


def get_all_fund_lists():
    all_fund_lists = []
    for page in range(1,get_total_page_num() + 1):
        all_fund_lists.append(get_fund_code_lists_by_page(page))

    return all_fund_lists


def write_all_fund_lists_into_file(filename="all_fund_lists.txt"):
    if os.path.exists(filename):
        with open(filename,"a+",encoding="utf-8") as file:
            logging.info("{} 文件存在，正在追加... | {}".format(filename, sys._getframe().f_code.co_name))
            file.write("\n\n")
            file.write("-"*20 + "这是新加的数据，时间：{}".format(time.ctime()) + "\n\n")
            for fund_list in get_all_fund_lists():
                file.write(str(fund_list))
                file.write("\n")
            file.write("\n" + "-"*20)
    else:
        with open(filename,"w",encoding="utf-8") as file:
            logging.info("{} 文件不存在，正在创建并写数据... | {}".format(filename, sys._getframe().f_code.co_name))
            for fund_list in get_all_fund_lists():
                file.write(str(fund_list))
                file.write("\n")


def get_name_data():
    s = '''fund_dict["date"] = date
            fund_dict["latest_nvalue_pu"] = latest_nvalue_pu
            fund_dict["latest_sum_nvalue"] = latest_sum_nvalue
            fund_dict["last_nvalue_pu"] = last_nvalue_pu
            fund_dict["last_sum_nvalue"] = last_sum_nvalue
            fund_dict["daily_growth"] = daily_growth
            fund_dict["daily_growth_rate"] = daily_growth_rate'''
    li = s.split("\n")
    tt = ""
    data = ""
    values = ""
    import re
    for i in li:
        t = i.split("=")[0].strip()
        t = re.sub("fund_dict\[\"", "", t)
        t = re.sub("\"\]", "", t)
        tt += t + ","
        d = "{}=each_data[\"{}\"],".format(t, t)
        data += d
        v = r"\'{" + t + r"}\'"
        values += v + ","
        sql = r"insert into {table_name} " + "({}) values({})".format(tt,values)
    print(sql)
    print(values)
    print(data)
    print(tt)

def get_sql():
    s = "date,fund_name,latest_nvalue_pu,latest_sum_nvalue,last_nvalue_pu,last_sum_nvalue,daily_growth,daily_growth_rate"
    li = s.split(",")
    s = ""
    for i in li:
        '''`fund_code` VARCHAR(50)  NULL,'''
        t = "`" + i + "`" + "VARCHAR(50)  NULL," + "\n"
        s += t
    print(s)

def save_to_mysql():
    mysql = MysqlFundCode()
    table_name = get_fund_code_lists_by_page(1)[1]["fund_type"]
    all_fund_lists = get_all_fund_lists()
    if not mysql.check_table_if_exist(table_name=table_name):
        logging.info("{}表没有创建，正在创建... | {}".format(table_name, sys._getframe().f_code.co_name))
        mysql.create_table(table_name)
        logging.info("创建成功！正在将数据写入{}中... | {}".format(table_name,sys._getframe().f_code.co_name))
        for fund_list in all_fund_lists:
            for each_data in fund_list:
                # print("now --> {}".format(each_data))
                mysql.insert_into_table(table_name,each_data)
                # logging.info("{} | {}".format(each_data, sys._getframe().f_code.co_name))
        logging.info("{}。写入成功。 | {}".format(table_name, sys._getframe().f_code.co_name))
    else:
        logging.info("表已存在，正在将数据写入{}中... | {}".format(table_name, sys._getframe().f_code.co_name))
        for fund_list in all_fund_lists:
            for each_data in fund_list:
                # print("now --> {}".format(each_data))
                mysql.insert_into_table(table_name,each_data)
        logging.info("{}。写入成功。 | {}".format(table_name, sys._getframe().f_code.co_name))




if __name__ == '__main__':
    get_fund_code_lists_by_page(1)

