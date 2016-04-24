'''
Scrapes marathon results from marathonguide.com.

Details
-------
Marathonguide.com uses MIDD numbers to index individual marathon events.  One
MIDD number corresponds to one event, hence Boston 2016 has a different MIDD
than Boston 2015.

Sample Use
----------
find_all_midds(2016, 'data/')
    - Generates a '2016midd_list.csv' file that contains all MIDD numbers found
    on marathonguide.com's list of 2016 Marathons.
    - Generates a '2016_weather_dates.csv' file that contains a list of marathon dates, cities, and times.  This is for use by the wunderground scraper to lookup weather conditions for each marathon.

scrape_marathons('data/', '2016midd_list.csv')
    - Scrapes runner data from each marathon contained in 2016_middlist.csv.
    - Each marathon is saved as a unique .csv file.
'''

import requests
import lxml.html
from lxml.cssselect import CSSSelector
from bs4 import BeautifulSoup
import pandas as pd
from collections import deque
from string import punctuation
from datetime import datetime
from time import sleep
from sys import stdout


def get_searchpage(s, midd, params):
    """Returns the HTML page for a single request for marathon data (max 100
    runners, as specified by params)
    Parameters
    ----------
    s : requests.session() object
    midd : integer
        numerical index of the marathon
    params : string
        search parameters to pass to the GET command

    Returns
    -------
    results.text : string
    """
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
    return results.text


def get_searchpage_header(s, midd, params):
    """Returns the header of the runners table
    Parameters
    ----------
    s : requests.session() object
    midd : integer
        numerical index of the marathon
    params : string
        search parameters to pass to the GET command

    Returns
    -------
    header : list of string
        corresponding to table headings
    """
    searchpage = get_searchpage(s, midd, params)
    soup = BeautifulSoup(searchpage, 'lxml')

    header = []
    table = soup.find('table', attrs={'border': 1, 'cellspacing': 0,
                                      'cellpadding': 3})
    headings = table.find_all('th')
    for heading in headings:
        header.append(heading.text.strip().encode('ascii', 'replace'))
    return header


def get_searchpage_runners(s, midd, params):
    """Returns the header of the runners table
    Parameters
    ----------
    s : requests.session() object
    midd : integer
        numerical index of the marathon
    params : string
        search parameters to pass to the GET command

    Returns
    -------
    runners : list of list of string
        list of marathon runners and their running data
    """
    searchpage = get_searchpage(s, midd, params)
    soup = BeautifulSoup(searchpage, 'lxml')

    runners = []
    table = soup.find('table', attrs={'border': 1, 'cellspacing': 0,
                                      'cellpadding': 3})
    rows = table.find_all('tr')
    for row in rows:
        cells = row.find_all('td')
        row_data = [cell.text.encode('ascii', 'replace').
                    replace('?', '').strip() for cell in cells]
        if len(row_data) >= 5:
            runners.append(row_data)
    return runners


def find_search_params(html):
    """Parses html and returns all possible search queries looking for both
    genders 'B'.

    Parameters
    ----------
    html : string

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
    Parameters
    ----------
    midd : integer
        marathonguide.com database id code

    Returns
    -------
    runners_df : DataFrame
    """
    home_url = 'http://www.marathonguide.com/results/browse.cfm'
    home_parameters = {'MIDD': midd}

    s = requests.Session()
    response = s.get(home_url, params=home_parameters)
    marathon_name, marathon_city, marathon_date \
        = get_marathon_info(response.text)

    print 'Fetching', marathon_name
    print marathon_city, marathon_date
    print 'MIDD:', midd

    runners = []
    search_params = find_search_params(response.text)
    header = None
    for params in search_params:
        total_runners = int(params.split(',')[-1])
        if not header:
            header = get_searchpage_header(s, midd, params)
        runners.extend(get_searchpage_runners(s, midd, params))
        print '\r{0:.0f}%'.format(len(runners)*100. / total_runners),
        stdout.flush()
        sleep(0.2)
    s.close()
    print '\r{0:.0f}%'.format(len(runners)*100. / total_runners)
    print '# of runners:', len(runners)
    runners_df = pd.DataFrame(runners)
    if len(runners) > 0:
        runners_df.columns = header
    return runners_df


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
    """Doesn't do much.  It used to, but I don't want to remove it.

    >>> clean_marathon_city('Las Cruces, NM USA')
    'Las Cruces, NM USA'
    >>> clean_marathon_city('Dublin, Ireland')
    'Dublin, Ireland'
    """
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


def find_all_midds(searchyear, csv_folder):
    """Compiles MIDD and weather index files for a given year.  These files
    provide a list of marathons/weather to scrape.

    To generate a list of all the marathons/MIDDs for 2015:
    > marathonguide.find_all_midds(2015)
    """
    weather_filename = csv_folder+str(searchyear)+'marathon_dates.csv'
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
            sleep(1)
            response = s.get(home_url, params=home_parameters)
            visited.add(midd)
            marathon_name, city, date = get_marathon_info(response.text)
            marathon_name = clean_marathon_name(marathon_name)
            city = clean_marathon_city(city)
            year = get_year(date)
            date = clean_date(date)
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


def scrape_marathons(folder, midd_file):
    """Scrapes marathon data from marathonguide.com using a list of MIDD
    indices (provided by midd_file) and saves each marathon to a separate .csv
    file.

    Parameters
    ----------
    folder : string
        Folder to find midd_file and save marathon raw.csv files
        Eg. 'data/'
    midd_file : string
        Csv file generated by find_all_midds().  Contains a list of MIDD
        indices to find marathons from marathonguide.com.

    Output
    ------
    raw.csv files
        One for each marathon.

    Returns
    -------
    None
    """
    scrape_df = pd.read_csv(folder+midd_file)
    for marathon in scrape_df.iterrows():
        marathon_name = marathon[1]['marathon']
        year = marathon[1]['year']
        midd = marathon[1]['midd']
        marathon_df = fetch_marathon_runners(midd)
        if len(marathon_df) > 0:
            marathon_df['midd'] = midd
            marathon_df.to_csv(folder+marathon_name+str(year)+'raw.csv',
                               index=False)


if __name__ == "__main__":
    # Command to test problem MIDDs found
    scrape_marathons('data/', 'problem_midd_list.csv')
