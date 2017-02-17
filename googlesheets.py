#For now, manually copy translation able from google doc into CSV, and import manually into translation_table in trybe_stats.
#Need to automate eventually in the script below using Google Sheets API and then insert into MySQL

from __future__ import print_function
import httplib2
import os
import pandas as pd


from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools


# pymysql libraries
from urllib2 import Request, urlopen, URLError
from pprint import pprint
import urllib, json
#import _mysql
import pymysql.cursors
import sqlalchemy as sa


try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()

except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
CLIENT_SECRET_FILE = 'translation_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'


# Connect to the trybe_stats database
connection = pymysql.connect(host='xx',
                             user='xx',
                             password='xx',
                             db='xx',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)



# Import data from a MySQL database table into a Pandas DataFrame using the pymysql package.
#sa = create_engine('mysql+pymysql://trybe_stats:n43Z7e3yj2835iw@178.62.215.27:3306/trybe_stats')



def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')

    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def clean(data):
    data = data.replace('##',' ')
    data = data.replace(' ','-')
    data = data.replace(' ','-')
    return data


def main():
    """Shows basic usage of the Sheets API.

    Creates a Sheets API service object and prints the names and majors of
    students in a sample spreadsheet:
    https://docs.google.com/spreadsheets/d/1zZa2k1qT0TJhIw91hIioxEUYzKlj74wn1rdnWYLZI1o/edit
    """




    try:

        credentials = get_credentials()
        http = credentials.authorize(httplib2.Http())
        discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                        'version=v4')
        service = discovery.build('sheets', 'v4', http=http,
                                  discoveryServiceUrl=discoveryUrl)

        # Specify Google Translation Spreadsheet
        spreadsheetId = 'ID' # Enter ID
        rangeName = 'surveys_locale!A2:L'

        result = service.spreadsheets().values().get(spreadsheetId=spreadsheetId, range=rangeName).execute()
        values = result.get('values', [])       #column headers, table and specify rows

         #convert values into dataframe
        df = pd.DataFrame(values)

        #replace all non trailing blank values created by Google Sheets API with null values
        df_replace = df.replace([''], [None])        

        #convert back to list to insert into MySQL
        processed_dataset = df_replace.values.tolist()  


        if not values:
            print('No data found.')
        else:
            with connection.cursor() as cursor:        



                # CREATE translation table

                print('Creating translation_table table...')
                
                cursor.execute("""CREATE TABLE `translation_table` (
                                    `tokens` varchar(255) NULL,
                                    `survey_group_name` varchar(255) COLLATE utf8_bin NULL,
                                    `en-US` varchar(255) COLLATE utf8_bin NULL default null,
                                    `nb-NO` varchar(255) COLLATE utf8_bin NULL default null,
                                    `sv-SE` varchar(255) COLLATE utf8_bin NULL default null,
                                    `de-DE` varchar(255) COLLATE utf8_bin NULL default null,
                                    `es-ES` varchar(255) COLLATE utf8_bin NULL default null,
                                    `pt-PT` varchar(255) COLLATE utf8_bin NULL default null,
                                    `fr-FR` varchar(255) COLLATE utf8_bin NULL default null,
                                    `da-DK` varchar(255) COLLATE utf8_bin NULL default null,
                                    `fi-FI` varchar(255) COLLATE utf8_bin NULL default null,
                                    `zh-CN` varchar(255) COLLATE utf8_bin NULL default null
                                ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

                """)


                 # INSERT VALUES IN TABLE

                print('Inserting records into Translation table')

                #Iterate through the dataframe list and insert into MySQL row by row
                for keyrow, row in enumerate(processed_dataset):

                        insert_sql = """INSERT INTO trybe_stats.`translation_table` (`tokens`, `survey_group_name`, `en-US`, `nb-NO`, `sv-SE`, `de-DE`, `es-ES`, `pt-PT`, `fr-FR`, `da-DK`, `fi-FI`, `zh-CN`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                        cursor.execute(insert_sql, [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11]])

                else:
                    print('No rows found')

                print('Finished inserting.')


                # COUNT NUMBER OF ROWS

                cursor.execute("SELECT COUNT(*) from trybe_stats.`translation_table`")
                result=cursor.fetchone()
                print(result.values())   #returns a dictionary, so values() gets the values which is the row count


        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()


    finally:
        connection.close()        


if __name__ == '__main__':
    main()