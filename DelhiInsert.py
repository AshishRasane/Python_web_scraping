import mysql.connector
import requests
from bs4 import BeautifulSoup
    

def scrape_data(url):
    subs, rtu, mw, mvar, voltage, all_data = [], [], [], [], [], []
    connection = mysql.connector.connect(user='root', database='python_sample', host='localhost')

    response = requests.get(url, timeout=10)
    soup = BeautifulSoup(response.content, 'html.parser')

    table = soup.find("table", attrs={"id": "ContentPlaceHolder3_dgrid"})
    # print ( table)

    for row in table.findAll("tr"):
        cells = row.findAll("td")
        if len(cells) == 5:
            subs.append(cells[0].find(text=True))
            rtu.append(cells[1].find(text=True))
            mw.append(cells[2].find(text=True))
            mvar.append(cells[3].find(text=True))
            voltage.append(cells[4].find(text=True))

    for sub, rtus, mws, mvars, volts in zip(subs, rtu, mw, mvar, voltage):
        cursor = connection.cursor()
        cursor.execute("INSERT INTO grid_loading(sub_station,rtu,mw,mvar,voltage) VALUES(%s,%s,%s,%s,%s)",
                       (sub, rtus, mws, mvars, volts))
        connection.commit()
        # all_data.append(sub +','+ rtus+','+mws+','+mvars+','+volts)

    # print(all_data)g


link = "http://www.delhisldc.org/Redirect.aspx?Loc=0804"
scrape_data(link)
