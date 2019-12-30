import pymysql,logging,sys
from toolkit import LOG_FORMAT,DATE_FORMAT,get_year_mon_day,get_class_name


class MysqlFundCode():
    def __init__(self):
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "root"
        self.database = "fund_data"
        self.charset = "utf8mb4"
        self.port = 3306
        self.count = 0

    def DB(self):
        return pymysql.connect(self.host,self.user, self.password, self.database, self.port, charset=self.charset)

    def insert_into_table(self, table_name,each_data):
        mysqlDB = self.DB()
        # logging.info("{} | {} | {}".format(mysqlDB, get_class_name(self), sys._getframe().f_code.co_name))
        # if not isinstance(each_data,list):
        #     each_data = eval(each_data)
        sql = '''
            insert into {table_name}(fund_code,fund_name,fund_type,date,nvalue_pu,day_growth_rate,
            a_week_rate,a_month_rate,_3_month_rate,_6_month_rate,a_year_rate,_2_year_rate,_3_year_rate,
            from_this_year,from_found_year,poundage,purchase_money)
            values(\'{fund_code}\',\'{fund_name}\',\'{fund_type}\',\'{date}\',\'{nvalue_pu}\',
            \'{day_growth_rate}\',\'{a_week_rate}\',\'{a_month_rate}\',\'{_3_month_rate}\',
            \'{_6_month_rate}\',\'{a_year_rate}\',\'{_2_year_rate}\',\'{_3_year_rate}\',
            \'{from_this_year}\',\'{from_found_year}\',\'{poundage}\',\'{purchase_money}\')
        '''.format(table_name=table_name,fund_code=each_data["fund_code"],fund_name=each_data["fund_name"],
                   fund_type=each_data["fund_type"],date=each_data["date"],nvalue_pu=each_data["nvalue_pu"],
                   day_growth_rate=each_data["day_growth_rate"],a_week_rate=each_data["a_week_rate"],
                   a_month_rate=each_data["a_month_rate"],_3_month_rate=each_data["_3_month_rate"],_6_month_rate=each_data["_6_month_rate"],
                   a_year_rate=each_data["a_year_rate"],_2_year_rate=each_data["_2_year_rate"],_3_year_rate=each_data["_3_year_rate"],
                   from_this_year=each_data["from_this_year"],from_found_year=each_data["from_found_year"],poundage=each_data["poundage"],
                   purchase_money=each_data["purchase_money"])
        # print(sql)
        try:
            with mysqlDB.cursor() as cursor:
                info = cursor.execute(sql)
                mysqlDB.commit()
                if cursor.rowcount >= 1:
                    self.count += 1
                else:
                    pass
        except Exception as e:
            logging.error("{} | {} | {}".format(e, get_class_name(self), sys._getframe().f_code.co_name))
        finally:
            mysqlDB.close()

    def create_table(self,table_name):
        mysqlDB = self.DB()
        # logging.info("{} | {} | {}".format(mysqlDB, get_class_name(self), sys._getframe().f_code.co_name))
        '''
        date,fund_name,latest_nvalue_pu,latest_sum_nvalue,last_nvalue_pu,
    last_sum_nvalue,daily_growth,daily_growth_rate
        '''
        '''
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
        '''
        sql = '''
            CREATE TABLE IF NOT EXISTS `{table_name}`(
               `id`  bigint NOT NULL AUTO_INCREMENT ,
               `fund_code` VARCHAR(40)  NULL,
               `fund_name` VARCHAR(100)  NULL,
               `fund_type` VARCHAR(40) NULL,
               `date` VARCHAR(40) NULL,
               `nvalue_pu`  VARCHAR(40) NULL,
               `day_growth_rate` VARCHAR(40) NULL,
               `a_week_rate` VARCHAR(40) NULL,
               `a_month_rate` VARCHAR(40) NULL,
               `_3_month_rate` VARCHAR(40) NULL,
               `_6_month_rate` VARCHAR(40) NULL,
               `a_year_rate` VARCHAR(40) NULL,
               `_2_year_rate` VARCHAR(40) NULL,
               `_3_year_rate` VARCHAR(40) NULL,
               `from_this_year` VARCHAR(40) NULL,
               `from_found_year` VARCHAR(40) NULL,
               `poundage` VARCHAR(40)  NULL,
               `purchase_money` VARCHAR(40)  NULL,
               PRIMARY KEY ( `id` )
            )ENGINE=InnoDB DEFAULT CHARSET=utf8;
        '''.format(table_name=table_name)
        try:
            with mysqlDB.cursor() as cursor:
                cursor.execute(sql)
                return True
        except Exception as e:
            logging.error("{} | {} | {}".format(e, get_class_name(self), sys._getframe().f_code.co_name))
            return False
        finally:
            mysqlDB.close()

    def check_table_if_exist(self,table_name):
        mysqlDB = self.DB()
        # logging.info("{} | {} | {}".format(mysqlDB,get_class_name(self),sys._getframe().f_code.co_name))
        sql = "show tables"
        try:
            with mysqlDB.cursor() as cursor:
                cursor.execute(sql)
                _tables = cursor.fetchall()
                table_lists = []
                for i in _tables:
                    table_lists.append(i[0])
                # print("all tables:{}".format(len(table_lists)))
                for _ in table_lists:
                    if table_name in _:
                        return True
                return False
        except Exception as e:
            logging.error("{} | {} | {}".format(e, get_class_name(self), sys._getframe().f_code.co_name))
        finally:
            mysqlDB.close()

    def get_code_and_name_and_type(self,table_name):
        mysqlDB = self.DB()
        # logging.info("{} | {} | {}".format(mysqlDB, get_class_name(self), sys._getframe().f_code.co_name))
        sql = "SELECT fund_code,fund_name,fund_type FROM {}".format(table_name)
        try:
            with mysqlDB.cursor() as cursor:
                cursor.execute(sql)
                info = cursor.fetchall()
                '''
                info是一个<class 'tuple'>类型的数据
                '''
                return_info = []
                for each in info:
                    _ = []
                    fund_code = each[0]
                    _table_name = "{}_{}_{}".format(each[0],each[1],each[2])
                    _.append(fund_code)
                    _.append(_table_name)
                    return_info.append(_)
                return return_info

        except Exception as e:
            logging.error("{} | {} | {}".format(e, get_class_name(self), sys._getframe().f_code.co_name))
        finally:
            mysqlDB.close()

    def show_data_rows(self):
        mysqlDB = self.DB()
        # logging.info("{} | {} | {}".format(mysqlDB, get_class_name(self), sys._getframe().f_code.co_name))
        sql_1 = "show tables"
        total_count = 0
        try:
            with mysqlDB.cursor() as cursor:
                cursor.execute(sql_1)
                _tables = cursor.fetchall()
                table_lists = []
                for i in _tables:
                    table_lists.append(i[0])
                for i in table_lists:
                    sql_2 = "select count(*) from {}".format(i)
                    cursor.execute(sql_2)
                    res = cursor.fetchall()
                    num = res[0][0]
                    total_count = total_count + num

                print("_"*20)
                print("from now on,there are {} lines data in database.".format(self.good_to_show(total_count)))
                print("_" * 20)

        except Exception as e:
            logging.error("{} | {} | {}".format(e, get_class_name(self), sys._getframe().f_code.co_name))
        finally:
            mysqlDB.close()

    def show_insert_rows(self):

        print("_" * 20)
        print("there total insert {} lines data in database.".format(self.good_to_show(self.count)))
        print("_" * 20)

    @staticmethod
    def good_to_show(num):
        _s = str(num)
        if len(_s) == 5:
            head = _s[0]
            tail = _s[1]
            total = head + "." + tail + "万"
            return total
        elif len(_s) == 6:
            head = _s[0:2]
            tail = _s[2]
            total = head + "." + tail + "万"
            return total
        elif len(_s) == 7:
            head = _s[0]
            tail = _s[1]
            total = head + "." + tail + "百万"
            return total
        elif len(_s) == 8:
            head = _s[0]
            tail = _s[1]
            total = head + "." + tail + "千万"
            return total
        elif len(_s) == 9:
            head = _s[0]
            tail = _s[1]
            total = head + "." + tail + "亿"
            return total
        elif len(_s) > 9:
            head = _s[0:-8]
            tail = _s[1]
            total = head + "." + tail + "亿"
            return total
        else:
            return str(num)



class MysqlFundDetailData():
    def __init__(self):
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "root"
        self.database = "fund_data"
        self.charset = "utf8mb4"
        self.port = 3306
        self.count = 0

    def DB(self):
        return pymysql.connect(self.host,self.user, self.password, self.database, self.port, charset=self.charset)

    def insert_into_table(self, table_name,each_data):
        mysqlDB = self.DB()
        # logging.info("{} | {} | {}".format(mysqlDB, get_class_name(self), sys._getframe().f_code.co_name))
        # if not isinstance(each_data,list):
        #     each_data = eval(each_data)
        sql = '''insert into {table_name}(date, latest_nvalue_pu, latest_sum_nvalue, last_nvalue_pu, last_sum_nvalue,
        daily_growth,daily_growth_rate) 
        values(\'{date}\',\'{latest_nvalue_pu}\',\'{latest_sum_nvalue}\',\'{last_nvalue_pu}\',
        \'{last_sum_nvalue}\',\'{daily_growth}\',\'{daily_growth_rate}\')
        '''.format(table_name=table_name, date=each_data["date"], latest_nvalue_pu=each_data["latest_nvalue_pu"],
           latest_sum_nvalue=each_data["latest_sum_nvalue"], last_nvalue_pu=each_data["last_nvalue_pu"],
           last_sum_nvalue=each_data["last_sum_nvalue"], daily_growth=each_data["daily_growth"],
           daily_growth_rate=each_data["daily_growth_rate"])

        # print(sql)
        try:
            with mysqlDB.cursor() as cursor:
                info = cursor.execute(sql)
                mysqlDB.commit()
                if cursor.rowcount >= 1:
                    self.count += 1
                else:
                    pass
        except Exception as e:
            logging.error("{} | {} | {}".format(e, get_class_name(self), sys._getframe().f_code.co_name))
        finally:
            mysqlDB.close()

    def create_table(self,table_name):
        mysqlDB = self.DB()
        # logging.info("{} | {} | {}".format(mysqlDB, get_class_name(self), sys._getframe().f_code.co_name))
        '''
        date,fund_name,latest_nvalue_pu,latest_sum_nvalue,last_nvalue_pu,
    last_sum_nvalue,daily_growth,daily_growth_rate
        '''
        '''
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
        '''
        sql = '''
            CREATE TABLE IF NOT EXISTS `{table_name}`(
               `id`  bigint NOT NULL AUTO_INCREMENT ,
               `date`VARCHAR(50)  NULL,
                `latest_nvalue_pu`VARCHAR(50)  NULL,
                `latest_sum_nvalue`VARCHAR(50)  NULL,
                `last_nvalue_pu`VARCHAR(50)  NULL,
                `last_sum_nvalue`VARCHAR(50)  NULL,
                `daily_growth`VARCHAR(50)  NULL,
                `daily_growth_rate`VARCHAR(50)  NULL,
               PRIMARY KEY ( `id` )
            )ENGINE=InnoDB DEFAULT CHARSET=utf8;
        '''.format(table_name=table_name)
        try:
            with mysqlDB.cursor() as cursor:
                cursor.execute(sql)
                return True
        except Exception as e:
            logging.error("{} | {} | {}".format(e, get_class_name(self), sys._getframe().f_code.co_name))
            return False
        finally:
            mysqlDB.close()

    def check_table_if_exist(self,table_name):
        mysqlDB = self.DB()
        # logging.info("{} | {} | {}".format(mysqlDB, get_class_name(self), sys._getframe().f_code.co_name))
        sql = "show tables"
        try:
            with mysqlDB.cursor() as cursor:
                cursor.execute(sql)
                _tables = cursor.fetchall()
                table_lists = []
                for i in _tables:
                    table_lists.append(i[0])
                # print("all tables:{}".format(len(table_lists)))
                for _ in table_lists:
                    if table_name in _:
                        return True
                return False
        except Exception as e:
            logging.error("{} | {} | {}".format(e, get_class_name(self), sys._getframe().f_code.co_name))
        finally:
            mysqlDB.close()

# if __name__ == '__main__':
#     MysqlFundCode().check_table_if_exist("ss")