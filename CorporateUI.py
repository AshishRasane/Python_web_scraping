import requests
from bs4 import BeautifulSoup
import mysql.connector
import time
import pymongo


def mongo_insert(dict_1, dict2):
    try:
        myclient = pymongo.MongoClient('mongodb://localhost:27017/')
        database = myclient['corporateoneline']
        coll_1 = database["uhbvn"]
        coll_2 = database["dhbvn"]

        result_1 = coll_1.insert_one(dict_1)
        result_2 = coll_2.insert_one(dict2)
        print(result_1, result_2)
    except Exception as e:
        print("Error Occurred " + str(e))


def sql_insert(dict_1, dict_2):
    connection = mysql.connector.connect(user='root', database='corporateoneline', host='localhost')
    if connection:
        print("Connected")
    cursor = connection.cursor()
    for key, value in dict_1.items():
        cursor.execute("INSERT INTO uhbvn(operation_circle, load_mw,timestamp) VALUES(%s,%s,%s)",
                       (key, value, time.strftime('%Y-%m-%d %H:%M:%S')))

        connection.commit()

    for key, value in dict_2.items():
        cursor.execute("INSERT INTO dhbvn(operation_circle, load_mw,timestamp) VALUES(%s,%s,%s)",
                       (key, value, time.strftime('%Y-%m-%d %H:%M:%S')))
        connection.commit()

    connection.close()


if __name__ == '__main__':
    login_details = {
        'Username': 'corporate',
        'password': 'corporate'
    }
    data_list = ["NULL"]
    count = 1
    with requests.Session() as s:
        login_page = s.post('https://haryanasldc.org.in/CorporateUI/logon.do?reason=application.baseAction.noSession',
                            data=login_details)
        data_page = s.get(
            'https://haryanasldc.org.in/CorporateUI/displayController.do?menuID=oneline.display&pageAction=null&uid'
            '=1433051&newWindow=true&popupWithNoMenu=true&menuID=oneline.display&uid=1433051&PFC_viewId=1833'
            '&preloader=2&pageId=DISPLAY_559765')

        soup = BeautifulSoup(data_page.content, 'html.parser')
        text_data = soup.find_all('text')

        for text_v in soup.find_all('text'):
            data_list.append(text_v.text)
            count += 1

    Table_One_dict = {
        "Ambala": int(data_list[7]),
        "Yamuna Nagar": int(data_list[6]),
        "Kurushetra": int(data_list[16]),
        "Karnal": int(data_list[1]),
        "Panipat": int(data_list[17]),
        "Sonipat": int(data_list[2]),
        "Kaithal": int(data_list[18]),
        "Rohtak": int(data_list[4]),
        "Jhajjar": int(data_list[19]),
        "Net Drawl": int(data_list[50]),
        "UHBVN OD/UD": int(data_list[46])
    }

    Table_Two_dict = {
        "Hissar": int(data_list[8]),
        "Bhiwani": int(data_list[9]),
        "Sirsa": int(data_list[10]),
        "Narnaul": int(data_list[13]),
        "Jind": int(data_list[3]),
        "Rewari": int(data_list[15]),
        "Palwal": int(data_list[5]),
        "Faridabad": int(data_list[11]),
        "Gurgaon": int(data_list[12]),
        "Net Drawl": int(data_list[14]),
        "DHBVN OD/UD": int(data_list[47]),
    }

    sql_insert(Table_One_dict, Table_Two_dict)
    mongo_insert(Table_One_dict, Table_Two_dict)
