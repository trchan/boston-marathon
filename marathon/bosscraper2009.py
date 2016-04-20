import requests
import sys
from string import lowercase
from pymongo import MongoClient
from bs4 import BeautifulSoup
from itertools import product


class ScrapingEngine(object):
    '''
    v2.0
    Marathon Scraping Engine for Boston Marathon data, 2001-2015
    Scraper for one year of data at a time.
    Stores data in MongoDB
    '''

    def __init__(self, collection_name, year):
        self.fetch_limit = 25
        self.query_limit = 100000
        self.record_string = '{}_{:04}'
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['marathon']
        self.collection = self.db[collection_name]
        self.url = 'http://registration.baa.org/cfm_Archive/iframe_ArchiveSearch.cfm'
        self.year = year

    def store_marathon_data(self, document_id, data):
        '''
        Stores raw HTML marathon data into Mongo
        {'id':id, 'content':data}
        '''
        # Storing the document in MongoDB (be careful to not store duplicates!)
        if not self.collection.find({'id': document_id}).count():
            doc = {'id': document_id, 'content': data}
            self.collection.insert_one(doc)
        else:
            print 'duplicate id:{} exists.  Data not stored'.format(document_id)

    def get_record_name(self, lastname, start_record, gender=0):
        if gender == 0:
            record_name = self.record_string.format(lastname, start_record)
        else:
            record_name = self.record_string.format(lastname+str(gender),
                                                    start_record)
        return record_name

    def lookup_db(self, id):
        cursor = self.collection.find({'id': id})
        output = []
        for document in cursor:
            output.append(document['content'])
        if len(output) == 1:
            return output[0]
        else:
            return output

    def __len__(self):
        return self.collection.count()

    def request_by_lastname(self, lastname, start_num=1, gender=0):
        '''
        Issues a single POST request and returns response
        INPUT:
        lastname   : string that is entered into post request
        start_num  : integer, fetches 25 records starting from start_num
        gender     : 0 for both, 1 for Male, 2 for Female
        OUTPUT:
        response

        max # of records in a query = 1000.  Returns 25 at a time.
        '''
        data = {'start': start_num, 'next': 'Next 25 Records'}
        params = {'mode': 'results', 'criteria': '',
                  'StoredProcParamsOn': 'yes', '': '',
                  'VarRaceYearLowID': self.year, 'VarRaceYearHighID': 0,
                  'VarAgeLowID': 0, 'VarAgeHighID': 0, 'VarGenderID': gender,
                  'VarBibNumber': '', 'VarLastName': lastname,
                  'VarFirstName': '', 'VarStateID': 0,
                  'VarCountryOfResidenceID': 0, 'VarCity': '', 'VarZip': '',
                  'VarTimeLowHr': '', 'VarTimeLowMin': '',
                  'VarTimeLowSec': '00', 'VarTimeHighHr': '',
                  'VarTimeHighMin': '', 'VarTimeHighSec': '59',
                  'VarSortOrder': 'ByName', 'VarAddInactiveYears': 0,
                  'records': self.fetch_limit, 'headerexists': 'Yes',
                  'bordersize': 0, 'bordercolor': '#ffffff',
                  'rowcolorone': '#FFCC33', 'rowcolortwo': '#FFCC33',
                  'headercolor': '#ffffff',
                  'headerfontface': 'Verdana,Arial,Helvetica,sans-serif',
                  'headerfontcolor': '#004080', 'headerfontsize': '12px',
                  'fontface': 'Verdana,Arial,Helvetica,sans-serif',
                  'fontcolor': '#000099', 'fontsize': '10px', 'linkfield': '',
                  'linkurl': '', 'linkparams': '',
                  'queryname': 'SearchResults',
                  'tablefields': 'RaceYear,FullBibNumber,FormattedSortName,AgeOnRaceDay,GenderCode,City,StateAbbrev,CountryOfResAbbrev,ReportingSegment'}
        r = requests.post(self.url, data=data, params=params)
        return r

    def scrape_all_by_lastname(self):
        max_reached = []
        for c1, c2 in product(lowercase, lowercase):
            lastname = c1+c2
            total_runners = 0
            print "retrieving lastname:{}".format(lastname),
            for start_record in range(1, self.query_limit, self.fetch_limit):
                record_name = self.record_string.format(lastname, start_record)
                # Only proceed if record does not exist in database
                if not self.collection.find({'id': record_name}).count():
                    sys.stdout.write('.')
                    r = self.request_by_lastname(lastname, start_record)
                    self.store_marathon_data(record_name, r.content)
                    total_runners += self.get_num_runners(r.content)
                    if self.is_end_of_search(r.content):
                        break
                else:
                    sys.stdout.write('-')
            if total_runners >= self.query_limit:
                print 'Query Limit Reached'
                max_reached.append(record_name)
            print '({})'.format(total_runners)
        print 'Scraping Complete'
        if len(max_reached) > 0:
            print '-----------------'
            print 'The following queries reached the max # of records'
            print ' '.join(max_reached)

    def scrape_lastname_subset(self, lastname, gender):
        print "retrieving lastname:{}".format(lastname),
        for start_record in range(1, self.query_limit, self.fetch_limit):
            total_runners = 0
            record_name = self.record_string.format(lastname+str(gender),
                                                    start_record)
            if not self.collection.find({'id': record_name}).count():
                sys.stdout.write('.')
                r = self.request_by_lastname(lastname, start_record, gender)
                self.store_marathon_data(record_name, r.content)
                total_runners += self.get_num_runners(r.content)
                if self.is_end_of_search(r.content):
                    break
            else:
                sys.stdout.write('-')
        print '({})'.format(total_runners)

    def is_end_of_search(self, content):
        '''
        Returns False if the following tag is found in content
        <input class="submit_button" type="submit" name="next" value="Next 25
        Records"/>
        '''
        soup = BeautifulSoup(content, 'lxml')
        f = soup.find_all(name='input',
                          attrs={'class': 'submit_button', 'type': 'submit',
                                 'name': 'next', 'value': 'Next 25 Records'})
        if len(f) > 0:
            return False
        else:
            return True

    def get_num_runners(self, content):
        soup = BeautifulSoup(content, 'lxml')
        return len(soup.find_all(name='tr', attrs={'class': 'tr_header'}))

    def get_runner_names(self, content):
        # These names are for 2001 - 2009 records
        column_names = ['Year', 'Bib', 'Name', 'Age', 'M/F', 'City', 'State',
                        'Country', ' ']
        # These names are for 2010 - 2015 column_names = ['BIB', 'NAME', 'AGE',
        # 'M/F', 'CITY', 'ST', 'CTRY', 'CTZ']
        soup = BeautifulSoup(content, 'lxml')
        # Iterate through records
        names = []
        for tag in soup.find_all(name='tr', attrs={'class': 'tr_header'}):
            names.append(tag.find_all(name='td')[2].text.strip())
        return names
