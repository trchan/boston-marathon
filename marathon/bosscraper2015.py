import requests
from string import lowercase
from pymongo import MongoClient
from bs4 import BeautifulSoup


class ScrapingEngine(object):
    '''
    This scraper queries the BAA website for marathon running data, and stores
    the raw html in MongoDB (database name = marathon).
    Tested and found that this successfully scrapes 2010-2015.  2009 and
    earlier failed.

    sample use:
    scraper = ScrapingEngine('bos15', 2015)
    scraper.scrape_all_by_lastname()
    - This only returns up to 1000 records per lastname query.  If any lastname
    subset is too large, use:
    scraper.scrape_lastname_subset(self, lastname, gender)
    - where gender = 1 or 2, will divide the category.
    '''

    def __init__(self, collection_name='bos15', year=2015):
        self.fetch_limit = 25
        self.query_limit = 1000
        self.record_string = '{}_{:04}'
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['marathon']
        self.collection = self.db[collection_name]
        site = 'http://registration.baa.org/'
        url_suffix = '/cf/Public/iframe_ResultsSearch.cfm?mode=results'
        self.url = site+str(year)+url_suffix

    def _store_marathon_data(self, idx, data):
        '''
        Stores raw HTML marathon data into Mongo
        {'id': id, 'content':data}
        '''
        # Storing the document in MongoDB (be careful to not store duplicates!)
        doc = {'id': idx, 'content': data}
        if not self.collection.find({'id': idx}).count():
            self.collection.insert_one(doc)
        else:
            print 'duplicate id:{} exists.  Data not stored'.format(idx)

    def _lookup_db(self, id):
        """Returns a record from MongoDB
        Parameters
        ----------
        id : string
            Generated from self.record_string.format(lastname, record #)
        Returns
        -------
        list of string
            Typically only one record, in html format
        """
        cursor = self.collection.find({'id': id})
        output = []
        for document in cursor:
            output.append(document['content'])
        return output

    def _request_by_lastname(self, lastname, start_num=1, gender=0):
        '''
        max # of records in a query = 1000.  Returns 25 at a time.
        INPUT:
        lastname   : string that is entered into post request
        start_num  : integer, fetches 25 records starting from start_num
        gender     : 0 for , 1 for Male, 2 for Female
        OUTPUT:
        response
        '''
        param = {'StoredProcParamsOn': 'yes',
                 'LastName': lastname,
                 'GenderID': gender,
                 'VarTargetCount': self.query_limit,
                 'records': self.fetch_limit,
                 'start': start_num,
                 'next': 'Next+25+Records'}
        r = requests.post(self.url, data=param)
        return r

    def _get_num_runners(self, content):
        """Parses html and counts the number of runner records found
        """
        soup = BeautifulSoup(content, 'lxml')
        return len(soup.find_all(name='tr', attrs={'class': 'tr_header'}))

    def _get_runner_names(self, content):
        """Parses html and returns a list of runner names
        """
        column_names = ['BIB', 'NAME', 'AGE', 'M/F', 'CITY', 'ST', 'CTRY',
                        'CTZ']
        soup = BeautifulSoup(content, 'lxml')
        # Iterate through records
        names = []
        for tag in soup.find_all(name='tr', attrs={'class': 'tr_header'}):
            names.append(tag.find_all(name='td')[1].text.strip())
        return names

    def scrape_all_by_lastname(self):
        """Scrapes an entire year of marathon data.  Uses a search algorithm
        based on searches of two-letter combinations of letters (aa, ab, ...,
        zy, zz).  Returns a maximum of 1000 records for each search (website
        limitation).
        """
        max_reached = []
        for c1 in lowercase:
            for c2 in lowercase:
                lastname = c1+c2
                total_runners = 0
                print "retrieving lastname:{}".format(lastname),
                for start_record in range(1, self.query_limit,
                                          self.fetch_limit):
                    record_name = self.record_string.format(lastname,
                                                            start_record)
                    if not self.collection.find({'id': record_name}).count():
                        print '.',
                        r = self._request_by_lastname(lastname, start_record)
                        self._store_marathon_data(record_name, r.content)
                        num_runners = self._get_num_runners(r.content)
                        total_runners += num_runners
                        if num_runners < 25:
                            break
                    else:
                        print 'x',
                if total_runners >= self.query_limit:
                    max_reached.append(record_name)
                print '({})'.format(total_runners)
        print 'Scraping Complete'
        print '-----------------'
        print 'The following queries reached the max # of records'
        print ' '.join(max_reached)

    def scrape_lastname_subset(self, lastname, gender):
        """Scrapes a single lastname, with a gender specification.  Stores
        data into MongoDB
        Parameters
        ----------
        lastname : string
            lastname to use in query
        gender : integer
            0 = both, 1 = male, 2 = female

        Returns
        -------
        None
        """
        print "retrieving lastname:{}".format(lastname),
        for start_record in range(1, self.query_limit, self.fetch_limit):
            total_runners = 0
            record_name = self.record_string.format(lastname+str(gender),
                                                    start_record)
            if not self.collection.find({'id': record_name}).count():
                print '.',
                r = self._request_by_lastname(lastname, start_record, gender)
                self._store_marathon_data(record_name, r.content)
                num_runners = self._get_num_runners(r.content)
                total_runners += num_runners
                if num_runners < 25:
                    break
            else:
                print 'x',
        print '({})'.format(total_runners)

    def __len__(self):
        return self.collection.count()
