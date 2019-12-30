import logging
import sys
from bs4 import BeautifulSoup as bs
from MysqlDB import MysqlFundDetailData
from toolkit import LOG_FORMAT,DATE_FORMAT,get_year_mon_day,get_class_name,get_HTML_content

def get_refer_fund_detail_data(fund_code,start_date="2019-09-28",end_date="2019-12-28"):
    '''
    :param fund_code:
    :param start_date:
    :param end_date:
    :return:
    date,
    fund_name,
    latest_nvalue_pu,
    latest_sum_nvalue,
    last_nvalue_pu,
    last_sum_nvalue,
    daily_growth,
    daily_growth_rate
    '''
    try:
        url = "https://www.dayfund.cn/fundvalue/{}.html?sdate={}&edate={}".format(fund_code, start_date, end_date)
        resp = get_HTML_content(url)
        soup = bs(resp,"lxml")
        trs = soup.find_all("table",attrs={"class":"mt1 clear"})[0]
        # fund_name = soup.find("h1",attrs={"class":"myfundTitle"}).string
        # t = re.sub(r"\(","_",fund_name)
        # fund_name = re.sub(r"\)","",t)
        _soup = bs(str(trs),"lxml")
        lis = _soup.find_all("tr")
        fund_lists = []
        count = 0
        for i in lis:
            fund_dict = {}
            if count == 1:
                count += 1
                pass
            t = list(i)
            if len(t) >= 17:
                date = t[1].string
                latest_nvalue_pu = t[7].string
                latest_sum_nvalue = t[9].string
                last_nvalue_pu = t[11].string
                last_sum_nvalue = t[13].string
                daily_growth = t[15].string
                daily_growth_rate = t[17].string
                fund_dict["date"] = date
                fund_dict["latest_nvalue_pu"] = latest_nvalue_pu
                fund_dict["latest_sum_nvalue"] = latest_sum_nvalue
                fund_dict["last_nvalue_pu"] = last_nvalue_pu
                fund_dict["last_sum_nvalue"] = last_sum_nvalue
                fund_dict["daily_growth"] = daily_growth
                fund_dict["daily_growth_rate"] = daily_growth_rate
                fund_lists.append(fund_dict)
                # logging.info("{} | {} appended into fund_lists".format(date,latest_nvalue_pu))
        return fund_lists[1:]
    except Exception as e:
        logging.error("{} | {}".format(e,sys._getframe().f_code.co_name))


def save_to_mysql(start_time=None,end_time=None, fund_code_lists=[]):
    if start_time == None:
        start_time = get_year_mon_day(y=1)
    if end_time == None:
        end_time = get_year_mon_day()

    if len(fund_code_lists) != 0:
        mysql = MysqlFundDetailData()
        for info in fund_code_lists:
            fund_code = info[0]
            table_name = info[-1]
            fund_lists = get_refer_fund_detail_data(fund_code,start_time,end_time)
            if not mysql.check_table_if_exist(table_name):
                logging.info("{}表没有创建，正在创建... | {}".format(table_name, sys._getframe().f_code.co_name))
                mysql.create_table(table_name)
                logging.info("创建成功！正在将数据写入{}中... | {}".format(table_name,sys._getframe().f_code.co_name))
                for i in fund_lists:
                    mysql.insert_into_table(table_name,i)
                logging.info("{}。写入成功。 | {}".format(table_name,sys._getframe().f_code.co_name))
            else:
                logging.info("表已存在，正在将数据写入{}中... | {}".format(table_name, sys._getframe().f_code.co_name))
                for i in fund_lists:
                    mysql.insert_into_table(table_name, i)
                logging.info("{}。写入成功。 | {}".format(table_name, sys._getframe().f_code.co_name))
                # for i in fund_lists:
                #     mysql.insert_into_table(table_name,i)
                #     print("{},done".format(table_name))

    else:
        logging.info("列表为空，没有爬取到数据。| {}".format(sys._getframe().f_code.co_name))



def get_name_data():
    s = '''
        fund_type = i[2]
            date = i[3]
            nvalue_pu = i[4]
            day_growth_rate = i[5]
            a_week_rate = i[6]
            a_month_rate = i[7]
            _3_month_rate = i[8]
            _6_month_rate = i[9]
            a_year_rate = i[10]
            _2_year_rate = i[11]
            _3_year_rate = i[12]
            from_this_year = i[13]
            from_found_year = i[14]
            poundage = i[-2]
            purchase_money = i[-5]
        '''
    l = s.split("\n")
    for i in l:
        li = i.strip().split("=")
        if len(li) == 2:
            name = li[0].strip()
            data = li[-1].strip()
            print("temp_dict[\"{}\"] = {}".format(name, data))

# def main():
#     mysql_code = MysqlFundCode()
#     fund_code_lists = mysql_code.get_code_and_name_and_type("指数型")
#     save_to_mysql(fund_code_lists=fund_code_lists)
#
# if __name__ == '__main__':
#     main()