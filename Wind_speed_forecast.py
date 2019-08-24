import ftplib
import csv
import api_hub as api
import os
import linecache
import sys
import datetime
import email_alert as email
import json
cwd = os.path.dirname(os.path.realpath(__file__))

import pandas as pd


# folder path for downloading csv file.
folder_path = "C:/Users/ASHISH/PycharmProjects/webscraping/Downloaded_Data/"

# Setting local timezone.
os.environ['TZ'] = 'Asia/Kolkata'


def get_exception():
    try:
        exc_type, exc_obj, tb = sys.exc_info()
        f = tb.tb_frame

        line_no = tb.tb_lineno
        filename = f.f_code.co_filename

        linecache.checkcache(filename)
        line = linecache.getline(filename, line_no, f.f_globals)

        return filename, line_no, line.strip(), exc_obj
    except Exception as e:
        print(e)


def download_file():
    try:

        print('Starting FTP Connection ')
        ftp = ftplib.FTP('ftp-customer.meteogroup.de')

        ftp.login(user='cct', passwd='sgnrk9k3')
        print('FTP Connection Successful')

        ftp.cwd('/')
        all_files = ftp.nlst()

        latest_file = all_files[-1]
        local_filename = os.path.join(folder_path + latest_file)

        file = open(local_filename, 'wb')
        ftp.retrbinary('RETR ' + latest_file, file.write)

        file.close()
        print(f'{latest_file} file is downloaded')

        return local_filename
    except Exception as e:
        print(e)


def main():
    try:
        renew_data_list, tptcl_data_list = [], []
        line_count, tptcl_count = 0, 0

        # Get Token
        token = api.get_token()

        renew_params = {'client_id': 56, 'plant-id': 1}
        tptcl_params = {'client_id': 57, 'plant-id': 2}

        # Get Turbine data from api.
        renew_turbine = api.get_turbine_data(token['access_token'], renew_params)
        tptcl_turbine = api.get_turbine_data(token['access_token'], tptcl_params)

        # Mapping with turbine ID
        for i in range(0, len(tptcl_turbine['data'])):
            if "BHE015" in renew_turbine['data'][i]['name']:
                renew_turbine_id = renew_turbine['data'][i]['turbineId']

        for j in range(0, len(tptcl_turbine['data'])):
            if "GSG-01" in tptcl_turbine['data'][j]['name']:
                tptcl_turbine_id = tptcl_turbine['data'][j]['turbineId']

        # Download Latest .csv File.
        latest_file = download_file()

        with open(latest_file) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',',)

            # For Skipping first two rows
            next(csv_reader)
            next(csv_reader)
            for row in csv_reader:

                utc_time = datetime.datetime.strptime(row[1], "%d.%m.%Y %H:%M")
                utc_time_ = utc_time.strftime("%d-%m-%Y %H:%M")+":00"

                local_time = utc_time + datetime.timedelta(hours=5, minutes=30)

                # condition for SEPARATING renew and tptcl data
                if tptcl_count < 194:
                    tptcl_count += 1

                    renew_data_list.append(
                        {"plant_id": "1",
                         "turbine_id": renew_turbine_id,
                         "utc_datetime": str(utc_time_),
                         "local_datetime": str(local_time),
                         "wind_speed": row[2]
                         }
                    )
                else:
                    tptcl_count += 1
                    tptcl_data_list.append(
                        {"plant_id": "2",
                         "turbine_id": tptcl_turbine_id,
                         "utc_datetime": str(utc_time_),
                         "local_datetime": str(local_time),
                         "wind_speed": row[2]
                         }
                    )

        # Making data.
        renew_set_params = {
            "client_id": 56,
            "source": 'wind_speed_forecast',
            "data": json.dumps(renew_data_list)
        }

        tptcl_set_params = {
            "client_id": 57,
            "source": 'wind_speed_forecast',
            "data": json.dumps(tptcl_data_list)
        }

        token = api.get_token()

        print(renew_set_params)
        # Set data
        result1 = api.set_data(token['access_token'], "wind", "setForecast", renew_set_params)
        result2 = api.set_data(token['access_token'], "wind", "setForecast", tptcl_set_params)
        print(result1, result2, sep='\n')

        # Delete downloaded file
        # os.unlink(latest_file)

    except Exception as e:

        filename, line_no, line_strip, exc_obj = get_exception()
        script_location = "script location : File Location/ Wind_speed_forecast.py, exception : {b}".format(a=cwd, b=e)

        filename_error = "Exception File : {a}".format(a=filename)
        line_no_error = "Exception Line No : {a}".format(a=line_no)

        line_syntax_error = "Exception Line Syntax : {a}".format(a=line_strip)
        ex_error = "Exception Reason : {a}".format(a=exc_obj)

        to = ["ashish.rasane@climate-connect.com"]
        subject = "Wind speed Forecast (FTP) fail"

        body = script_location + "\n" + filename_error + "\n" + line_no_error + "\n" + line_syntax_error + \
               "\n" + ex_error + "\n "
        # email.send_notification(to, subject, body)



if __name__ == '__main__':

    print(f'Execution started at {datetime.datetime.now()}')
    main()
    print(f'Execution Ended at {datetime.datetime.now()}')