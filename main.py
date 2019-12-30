from get_fund_code import *
from get_fund_data import *
from MysqlDB import MysqlFundCode
import time

def main():
    s_time = time.time()
    print("程序正在运行....")
    mysql_code = MysqlFundCode()
    fund_code_lists = mysql_code.get_code_and_name_and_type("指数型")
    save_to_mysql(fund_code_lists=fund_code_lists)

    e_time = time.time()
    print("一共运行了{}秒".format(e_time-s_time))

if __name__ == '__main__':
    main()