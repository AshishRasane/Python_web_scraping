import os
import requests
from bs4 import BeautifulSoup
import linecache
import sys
import email_alert as email
import DB_Helper as Mongo
from datetime import datetime
import datetime
import pandas as pd

os.environ['TZ'] = "Asia/Kolkata"

Exception_counter = 0

db = Mongo.mongo_connection()
coll_name = "entitlement_discoms_data"

to = subject = body = "NULL"

headers = {
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Origin': 'https://www.delhisldc.org',
    'Upgrade-Insecure-Requests': '1',
    'Content-Type': 'application/x-www-form-urlencoded',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 '
                  'Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Referer': 'https://www.delhisldc.org/Entwebpage.aspx?id=NDMC',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
}

current_date = datetime.datetime.now()
all_time = datetime.datetime.now()

date = all_time.today()
date = date.strftime('%Y-%m-%d')
date = date + " " + "00:00:00"

date_object = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")

cwd = os.path.dirname(os.path.realpath(__file__))


def get_exception():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    line_no = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, line_no, f.f_globals)
    return filename, line_no, line.strip(), exc_obj


def fetch_data(url, value, revision_no):
    try:
        df = pd.DataFrame()
        global Exception_counter

        discom_id = Mongo.fetch_discom_id(value)

        day = str(datetime.date.today().day)

        day = '0' + day if len(day) == 1 else day

        month = str(datetime.date.today().month)

        month = '0' + month if len(month) == 1 else month

        year = str(datetime.date.today().year)

        get_response = requests.get(url, verify=False)
        get_soup = BeautifulSoup(get_response.content, "html.parser")

        view_state = get_soup.find('input', {'id': '__VIEWSTATE'}).attrs
        view_state = view_state['value']

        event_validation = get_soup.find('input', {'id': '__EVENTVALIDATION'}).attrs
        event_validation = event_validation['value']

        print("Inserting data for", value, day, "/", month, "/", year, "for", "revision number", revision_no)
        data = {
            "__EVENTTARGET": "ctl00$ContentPlaceHolder2$ddrevnon",
            "__VIEWSTATE": str(view_state),
            "__EVENTVALIDATION": str(event_validation),
            "ctl00$ContentPlaceHolder2$cmbdiscom": str(value),
            "ctl00$ContentPlaceHolder2$ddmonth": str(month),
            "ctl00$ContentPlaceHolder2$ddday": str(day),
            "ctl00$ContentPlaceHolder2$txtyear": str(year),
            "ctl00$ContentPlaceHolder2$ddrevnon": str('0' + str(revision_no)) if len(
                str(revision_no)) == 1 else revision_no
        }

        if value == "BRPL":
            response = requests.post(url, verify=False)

        else:
            response = requests.post(url, data=data, headers=headers, verify=False)

        soup = BeautifulSoup(response.content, "html.parser")
        script = str(soup.find('table', id='demoTable1'))

        if not script:
            Exception_counter += 1
            if Exception_counter >= 3:
                raise Exception("Data is not available on website")
            main()
        issued_on = soup.find("span", id="ContentPlaceHolder2_Label3").text

        headings = [[cell.text for cell in row("th")]
                    for row in BeautifulSoup(script, features="html.parser")("tr")][0]

        if not headings:
            Exception_counter += 1
            print("Retrying for", Exception_counter, "time")
            if Exception_counter >= 3:
                raise Exception("Data is not available on website")
            main()

        df["headings"] = headings

        table_data = [[cell.text for cell in row("td")]
                      for row in BeautifulSoup(script, features="html.parser")("tr")]

        if not table_data:
            Exception_counter += 1
            print("Retrying for", Exception_counter, "time")
            if Exception_counter >= 3:
                raise Exception("Data is not available on website")
            main()

        for i in range(1, 97):
            df["Column" + str(i)] = table_data[i]
            df["discom_id"] = discom_id
            df["revision_number"] = revision_no
            df["issued_on"] = issued_on

        set_data(df)

    except Exception:
        raise Exception("Data is not available on website")


def set_data(df):
    try:
        col = db.entitlement_discoms_data
        bulk_op = col.initialize_ordered_bulk_op()

        for i in range(1, len(df) - 3):
            for j in range(1, len(df.columns) - 3):
                bulk_op.find({
                    'date': date_object,
                    'discom_id': int(df["discom_id"][0]),
                    'revision': int(df["revision_number"][0]),
                    'station_name': df["headings"][i],
                    'issued_on': datetime.datetime.strptime(str(df["issued_on"][0]),
                                                            "%d/%m/%Y %H:%M:%S")}).upsert().update(
                    {'$set': {
                        'data.' + df["Column" + str(j)][0].split("-")[0] + ":00":
                            {
                                'time': datetime.datetime.strptime(str(str(current_date.date()) + " "
                                                                       + str(
                                    df["Column" + str(j)][0].split("-")[0] + ":00")),
                                                                   "%Y-%m-%d %H:%M:%S"),
                                'value': float(df["Column" + str(j)][i]),
                                'inserted_at': datetime.datetime.now()
                            }
                    }
                    }
                )

        bulk_op.execute()
        print("Data Inserted for revision number", df["revision_number"][0])

    except Exception as e:
        if hasattr(e, 'message'):
            raise e.message
        else:
            raise e


def main():
    try:
        global current_revision, Exception_counter
        if Exception_counter >= 3:
            raise Exception("Data is not available on website")

        form_data = ['BRPL', 'BYPL', 'NDMC', 'NDPL']

        for i in range(len(form_data)):
            url = "http://www.delhisldc.org/Entwebpage.aspx?id=" + form_data[i]
            value = form_data[i]

            discom_id = Mongo.fetch_discom_id(value)
            db_max_revision = Mongo.get_db_max_revision(date_object, False, db, coll_name, False, False, discom_id)
            print("DB max Revision is", db_max_revision)

            response = requests.post(url, verify=False)
            soup = BeautifulSoup(response.content, "html.parser")

            # Getting Dropdown List
            revision_list = [int(x.text) for x in soup.find(id="ContentPlaceHolder2_ddrevnon").find_all('option')]

            selected_params = soup.find_all('option', {'selected': True})
            if len(selected_params) != 4:
                Exception_counter += 1
                if Exception_counter >= 3:
                    raise Exception("Data is not available on website")
                print("Retrying for", Exception_counter, "time")
                main()

            else:
                current_revision = int(selected_params[3].text)
                print("Current Revision is", current_revision)

                if current_revision is None:
                    continue

                while db_max_revision <= current_revision:
                    db_max_revision += 1
                    if db_max_revision in revision_list:
                        fetch_data(url, value, db_max_revision)

    except Exception as e:

        global to, subject, body
        filename, line_no, line_strip, exc_obj = get_exception()
        script_location = "script location : 139.59.47.69{a}/Entitlement.py, exception : {b}".format(a=cwd, b=e)

        filename_error = "Exception File : {a}".format(a=filename)
        line_no_error = "Exception Line No : {a}".format(a=line_no)

        line_syntax_error = "Exception Line Syntax : {a}".format(a=line_strip)
        ex_error = "Exception Reason : {a}".format(a=exc_obj)

        to = ["ashish.rasane@climate-connect.com"]
        subject = "EPM Alert..!!Entitlement scraping fail"

        body = script_location + "\n" + filename_error + "\n" + line_no_error + "\n" + line_syntax_error + "\n" + ex_error + "\n"


if __name__ == '__main__':
    print("Execution starts at", datetime.datetime.now())
    main()
    # email.send_notification(to, subject, body)
    print("Execution Ended at", datetime.datetime.now())

import pandas as pd

