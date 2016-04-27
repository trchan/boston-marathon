'''
Retrieve weather data (by time) as specified by a .csv file.
input csv header:
marathon,year,date,startcity,endcity,starthour,endhour
Boston,2011,04/11/2011,Worcester MA, Boston MA,10,16
'''

import requests
from requests.exceptions import ConnectionError
import lxml.html
from lxml.cssselect import CSSSelector
import pandas as pd
from time import sleep
from sys import stdout


def get_hour(hour_text):
    """Returns time in hours from time representation on wunderground.com.

    Parameters
    ----------
    hour_text : text
        String representation of time in HH:MM AM/PM, Eg. '1:07 PM'

    Returns
    -------
    hour : float
        Time in hours.

    Example
    -------
    >>> get_hour('1:07 PM')
    13.116666666666667
    >>> get_hour('12:54 AM')
    0.9
    >>> get_hour('12:54 PM')
    12.9
    """
    time, meridiem = hour_text.split(' ')
    hour, minute = map(int, time.split(':'))
    if hour == 12:
        hour = 0
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
    '''Get Header from response object
    DEPRECATED
    '''
    tree = lxml.html.fromstring(response.text)
    sel = CSSSelector('#obsTable th')
    header = [item.text_content().strip() for item in sel(tree)]
    return header


def extract_weather_for_time(response, target_time):
    '''
    Finds the weather information from a response object which is closest to
    the specified time
    '''
    tree = lxml.html.fromstring(response.text)
    # CSSSelector for Time
    sel = CSSSelector('#obsTable td:nth-child(1)')
    time_list = [item.text_content() for item in sel(tree)]
    ix = find_closest_time(time_list, target_time)
    # CSSSelector for row that corresponds to closest time
    sel = CSSSelector('#obsTable :nth-child('+str(ix+1)+') td')
    # Convert text to an array
    row_data = [item.text_content().strip().encode('ascii', 'ignore')
                for item in sel(tree)]
    # Check if values are shifted due to dropped windchill heatindex column
    # Identify checking Dew Point column for a Humidity value (% instead of F)
    if row_data[3][-1] == '%':
        row_data = row_data[0:2] + ['-'] + row_data[2:]
    row_data = row_data[0:13]   # Trim, for error control
    return row_data


def fetch_weather_page(location, month, day, year):
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
    params = {'airportorwmo': 'query',
              'historytype': 'DailyHistory',
              'backurl': '/history/index.html',
              'code': location,
              'month': month,
              'day': day,
              'year': year}
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
            sleep(5)
        if count > 3:
            print 'Exceeded 3 attempts'
            return None
    return response


def fetch_by_csv(filename):
    '''Reads query information from a .csv file and returns scraped, and
    extracted data as a DataFrame.  One row in a query file results in
    multiple rows of the output dataframe

    Parameters
    ----------
    filename : string
        Name of query .csv file.  Should have the following header:
        marathon,year,date,startcity,endcity,starthour,endhour

    Output
    ------
    weather_df : DataFrame
        Weather data, one row per observation.
        Starts with the following header:
        marathon,year,date,city,...
    '''
    INTERVAL = 1    # sampling interval in hours
    ERROR_URL = 'https://www.wunderground.com/history/index.html?error='

    query_df = pd.read_csv(filename)
    weather_df = pd.DataFrame()
    # headers = None
    headers = ['marathon', 'year', 'date', 'city', 'Time', 'Temp.',
               'Windchill', 'Dew Point', 'Humidity', 'Pressure', 'Visibility',
               'Wind Dir', 'Wind Speed', 'Gust Speed', 'Precip', 'Events',
               'Conditions']
    n = len(query_df)
    # iterate through the rows of the query file
    for ix, query_row in query_df.iterrows():
        print '\r{0:.0f}%'.format(ix*100. / n),
        print query_row['date'],
        print query_row['marathon'][0:8],
        stdout.flush()
        start_hour = query_row['starthour']
        end_hour = query_row['endhour']
        month, day, year = map(int, query_row['date'].split('/'))
        for cityname in [query_row['startcity'], query_row['endcity']]:
            response = fetch_weather_page(cityname, month, day, year)
            if response.url[0:54] == ERROR_URL:
                print 'error:',response.url.split('?')[1].split('&')[0]
                break
            if response.text.find('No daily or hourly history data') > 0:
                print 'error: no hourly data available'
                break
            # if headers is None:
                # headers = ['marathon', 'year', 'date', 'city']
                # headers.extend(extract_header(response))
            for t in range(start_hour, end_hour+1, INTERVAL):
                data_row = [query_row['marathon'], query_row['year'],
                            query_row['date'], cityname]
                data_row.extend(extract_weather_for_time(response, t))
                weather_df = weather_df.append([data_row])
        print weather_df.shape
    weather_df.columns = headers
    return weather_df


def query_by_csv_to_csv(inputfile, outputfile):
    '''
    Reads query information from a .csv file and saves scraped, extracted data
    as a .csv file.
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


def test_fetch_weather():
    response = fetch_weather_page('Boston MA', 04, 20, 2015)
    header = ['marathon', 'year', 'date', 'city', 'Time', 'Temp.',
              'Windchill', 'Dew Point', 'Humidity', 'Pressure', 'Visibility',
              'Wind Dir', 'Wind Speed', 'Gust Speed', 'Precip', 'Events',
              'Conditions']
    data = []
    data.append(extract_weather_for_time(response, 10))
    data.append(extract_weather_for_time(response, 16))
    print
    for h, d0, d1 in zip(header, *data):
        print '{0:15}{1:10}{2:10}'.format(h, d0, d1)


if __name__ == "__main__":
    query_by_csv_to_csv('data/marathon_dates.csv', 'data/marathon_weather.csv')
