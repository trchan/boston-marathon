'''
Retrieve weather data (by time) as specified by a .csv file.
csv Format:
id,date,start city,end city
bos15,04/20/2015,Worcester MA,Boston MA
'''

import requests
from requests.exceptions import ConnectionError
import lxml.html
from lxml.cssselect import CSSSelector
import pandas as pd
from time import sleep


def get_hour(hour_text):
    '''
    INPUT:
        hour_text: text, eg '1:07 PM'
    OUTPUT:
        float, eg '13.11667'
    Returns time in hours from time representation on wunderground.com.
    '''
    time, meridiem = hour_text.split(' ')
    hour, minute = map(int, time.split(':'))
    if meridiem == 'PM':
        hour += 12
    hour += minute/60.
    return hour


def find_closest_time(time_list, target_hour):
    '''
    Returns index of record corresponding closest to a specified time
    INPUT:
        list of string (eg. ['1:07 AM', '3:09 PM'])
        float (corresponding to hour of day, eg. 13.12667)
    OUTPUT:
        integer (index)
    '''
    hours = [get_hour(t) for t in time_list]
    distance = [abs(h-target_hour) for h in hours]
    return distance.index(min(distance))


def extract_header(response):
    # Get Header from response object
    tree = lxml.html.fromstring(response.text)
    sel = CSSSelector('#obsTable th')
    header = [item.text_content().strip() for item in sel(tree)]
    return header


def extract_weather_for_time(response, target_time):
    '''
    Finds the weather information from a response object which is closest to the specified time
    '''
    tree = lxml.html.fromstring(response.text)
    # CSSSelector for Time
    sel = CSSSelector('#obsTable td:nth-child(1)')
    time_list = [item.text_content() for item in sel(tree)]
    ix = find_closest_time(time_list, target_time)
    # CSSSelector for row that corresponds to closest time
    sel = CSSSelector('#obsTable :nth-child('+str(ix+1)+') td')
    # Convert text to an array
    row_data = [item.text_content().strip().encode('ascii','ignore') for item in sel(tree)]
    return row_data


def fetch_history_page(location, month, day, year):
    '''
    Requests historical data from wunderground.com and returns response object.
    INPUT:
        location: string - can be a city, zip code, or airport code
        month: integer
        day: integer
        year: integer
    OUTPUT:
        response object
    '''
    url = 'https://www.wunderground.com/history/'
    params = {'airportorwmo' : 'query',
            'historytype' : 'DailyHistory',
            'backurl' : '/history/index.html',
            'code' : location,
            'month' : month,
            'day' : day,
            'year' : year}
    # Try the request till it works (up to 3 times)
    count = 0
    success = False
    while not success:
        try:
            count += 1
            response = requests.get(url, params=params)
            success = True
        except ConnectionError:
            success = False
            print 'x',
            sleep(5)
        if count > 3:
            print 'Exceeded 3 attempts'
            return None
    print '.',
    return response


def fetch_by_csv(filename):
    '''
    Reads query information from a .csv file and returns scraped, extracted data as a DataFrame.
    INPUT:
        filename: string, corresponding to a .csv file
    OUTPUT:
        Pandas dataframe
    '''
    start_hour = 10
    end_hour = 16
    interval = 2

    query_df = pd.read_csv(filename)
    data_df = pd.DataFrame()
    headers = None
    for ix,query_row in query_df.iterrows():
        month, day, year = map(int, query_row['date'].split('/'))
        for cityname in [query_row['start city'], query_row['end city']]:
            response = fetch_history_page(cityname, month, day, year)
            if headers == None:
                headers = ['id','date','city']
                headers.extend(extract_header(response))
            for t in range(start_hour, end_hour+1, interval):
                data_row = [query_row['id'], query_row['date'], cityname]
                data_row.extend(extract_weather_for_time(response, t))
                data_df = data_df.append([data_row])
    print
    data_df.columns = headers
    return data_df


def query_by_csv_to_csv(inputfile, outputfile):
    '''
    Reads query information from a .csv file and saves scraped, extracted data as a .csv file.
    INPUT:
        inputfile: string   filename of .csv file with queries
        outputfile: string  filename of .csv file to save scraped data
    OUTPUT:
        None
    '''
    print 'Starting Query'
    df = fetch_by_csv(inputfile)
    print 'Saving Results'
    df.to_csv(outputfile, index=False)
    print 'Results saved to', outputfile


if __name__ == "__main__":
    #This is test code that tests whether the fetch_history_page() can scrape data from one query.
    response = fetch_history_page('Boston, MA', 04, 20, 2015)
    header = extract_header(response)
    data = []
    data.append(extract_weather_for_time(response, 10))
    data.append(extract_weather_for_time(response, 16))
    for h, d0, d1 in zip(header, *data):
        print h,'\t',d0,'\t',d1
