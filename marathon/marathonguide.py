'''
Scrapes marathon results from marathonguide.com.  Uses an input CSV file to
specify which marathons to scrape.

Sample input csv header:
marathon,year,date,MIDD
Houston,2016,2160117'''

import requests
import lxml.html
from lxml.cssselect import CSSSelector
from bs4 import BeautifulSoup
import pandas as pd
from collections import deque
from string import punctuation
from datetime import datetime
import time


def get_runners_searchpage(s, midd, params):
    rp = 'http://www.marathonguide.com/results/makelinks.cfm'
    data = {'RaceRange': params,
            'RaceRange_Required': 'You must make a selection before viewing \
            results.',
            'MIDD': midd,
            'SubmitButton': 'View'}
    headers = {
        "Referer":
        "http://www.marathonguide.com/results/browse.cfm?MIDD"+str(midd),
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4)\
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36"}

    results = s.post(rp, data=data, headers=headers)
    soup = BeautifulSoup(results.text, 'lxml')

    runners = []
    table = soup.find('table', attrs={'border': 1, 'cellspacing': 0,
                                      'cellpadding': 3})
    rows = table.find_all('tr')
    for row in rows:
        cells = row.find_all('td')
        row_data = [cell.text.strip() for cell in cells]
        if len(row_data) == 9:
            runners.append(row_data)
    return runners


def find_search_params(html):
    """Finds and returns all the possible search queries
    Parameters
    ----------
    html

    Returns
    -------
    params : list of string
    """
    params = []
    soup = BeautifulSoup(html, 'lxml')
    for tag in soup.find_all(name='select', attrs={'name': 'RaceRange'}):
        for option_tag in tag.find_all(name='option'):
            if len(option_tag.attrs['value']) > 0:
                if option_tag.attrs['value'][0] == 'B':
                    params.append(option_tag.attrs['value'])
    return params


def get_marathon_info(html):
    """Extracts identifying information about the marathon from the html text.

    Parameters
    ----------
    html : text

    Returns
    -------
    marathon_name,
    marathon_city,
    marathon_date : string
    """
    tree = lxml.html.fromstring(html)
    sel = CSSSelector('.BoxTitleOrange b')
    items = []
    for item in sel(tree):
        items.append(item.text_content())
    marathon_name = items[0]
    marathon_city = items[1]
    marathon_date = items[2]
    return marathon_name, marathon_city, marathon_date


def find_midds(html):
    """Returns a list of MIDD (marathon identification numbers) from the html.
    Parameters
    ----------
    html : string

    Returns
    -------
    list of string
    """
    midd_list = []
    search_phrase = 'browse.cfm?MIDD='
    search_length = len(search_phrase)
    soup = BeautifulSoup(html, 'lxml')
    for tag in soup.find_all(name='a'):
        if 'href' in tag.attrs.keys():
            search_index = tag.attrs['href'].find(search_phrase)
            if search_index >= 0:
                midd_list.append(int(tag.attrs['href'][search_index +
                                                       search_length:]))
    return midd_list


def fetch_marathon_runners(midd):
    """Retrieves the home search page for a given marathon
    """
    home_url = 'http://www.marathonguide.com/results/browse.cfm'
    home_parameters = {'MIDD': midd}

    s = requests.Session()
    response = s.get(home_url, params=home_parameters)
    marathon_name, marathon_city, marathon_date \
        = get_marathon_info(response.text)

    print 'Fetching', marathon_name
    print marathon_city, ',', marathon_date

    search_params = find_search_params(response.text)
    runners = [['Last Name/First Name (Sex/Age)', 'Time', 'OverallRank',
                'GenderRank/DivRank', 'DIV', 'NetTime', 'State/Country',
                'AGTime', 'BQ']]
    for params in search_params:
        runners.extend(get_runners_searchpage(s, midd, params))
        print runners[-1]
        time.sleep(1)
    s.close()
    return runners


def clean_marathon_name(name):
    in_bracket = False
    clean_name = ''
    for c in name:
        if c == '(':
            in_bracket = True
        if c not in punctuation and not in_bracket:
            clean_name += c
        if c == ')':
            in_bracket = False
    name = clean_name
    name = name.lower()
    name = name.replace('marathon', '')
    name = name.replace('series', '')
    name = name.strip()
    name = name.replace('   ', ' ')
    name = name.replace('  ', ' ')
    name = name.replace(' ', '_')
    return name


def clean_marathon_city(name):
    name = name.replace(',', '')
    name = name.replace('USA', '')
    name = name.strip()
    return name


def clean_date(date_string):
    """
    >>> clean_date('January 1, 2010')
    '01/01/2010'
    >>> clean_date('November 25, 2016')
    '11/25/2016'
    """
    date_object = datetime.strptime(date_string, '%B %d, %Y')
    return date_object.strftime('%m/%d/%Y')


def get_year(date):
    """
    Parameters
    ----------
    date : string
        expecting format of 'November 15, 2015' as found on marathonguide.com

    Returns
    -------
    year : integer

    Example
    -------
    >>> get_year('November 15, 2015')
    2015
    """
    year_string = date.split(',')[1]
    return int(year_string)


def find_all_midds(searchyear):
    """Compiles MIDD and weather index files for a given year.  These files
    provide a list of marathons/weather to scrape.

    To generate a list of all the marathons/MIDDs for 2015:
    > marathonguide.find_all_midds(2015)
    """
    csv_folder = 'data/'
    weather_filename = csv_folder+str(searchyear)+'marathon_weather.csv'
    midd_filename = csv_folder+str(searchyear)+'midd_list.csv'

    url = 'http://www.marathonguide.com/results/browse.cfm?Year=' + \
          str(searchyear)
    s = requests.Session()
    response = s.get(url)
    midds = deque(find_midds(response.text))

    weather_df = pd.DataFrame()
    midd_df = pd.DataFrame()
    # Go to search page for each MIDD and find other MIDDs
    home_url = 'http://www.marathonguide.com/results/browse.cfm'
    visited = set([5987150912])
    while len(midds) > 0:
        midd = midds.popleft()
        if midd not in visited:
            home_parameters = {'MIDD': midd}
            time.sleep(1)
            response = s.get(home_url, params=home_parameters)
            visited.add(midd)
            marathon_name, city, date = get_marathon_info(response.text)
            marathon_name = clean_marathon_name(marathon_name)
            city = clean_marathon_city(city)
            year = get_year(date)
            date = clean_date(date)
            # Use line below if you want to recursively pull all years
            # midds.append(find_midds(response.text))
            midd_df = midd_df.append([[marathon_name, year, midd]])
            weather_df = weather_df.append([[marathon_name, year, date, city,
                                            city, 10, 16]])
            print marathon_name, year, midd, date, city
    s.close()
    print 'Saving', len(midd_df), 'records.'
    midd_df.columns = ['marathon', 'year', 'midd']
    midd_df.to_csv(midd_filename, index=False)
    weather_df.columns = ['marathon', 'year', 'date', 'startcity', 'endcity',
                          'starthour', 'endhour']
    weather_df.to_csv(weather_filename, index=False)


if __name__ == "__main__":
    scrape_df = pd.read_csv('data/2015midd_list.csv')
    for marathon in scrape_df.iterrows():
        marathon_name = marathon[1]['marathon']
        year = marathon[1]['year']
        midd = marathon[1]['midd']
        runners = fetch_marathon_searchpage(midd)
        marathon_df = pd.DataFrame(runners)
        marathon_df.to_csv('data/'+marathon_name+year+'raw.csv')
