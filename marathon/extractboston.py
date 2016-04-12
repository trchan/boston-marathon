from pymongo import MongoClient
from bs4 import BeautifulSoup
import sys
import pandas as pd


def get_num_runners(content):
    soup = BeautifulSoup(content, 'lxml')
    return len(soup.find_all(name='tr', attrs={'class':'tr_header'}))


def get_runner_names(content):
    column_names = ['bib', 'name', 'age', 'gender', 'city', 'state', 'country', 'ctz']
    soup = BeautifulSoup(content, 'lxml')
    # Iterate through records
    names = []
    for tag in soup.find_all(name='tr', attrs={'class':'tr_header'}):
        names.append(tag.find_all(name='td')[1].text.strip())
    return names


def get_runners_data(content, geturl=True):
    soup = BeautifulSoup(content, 'lxml')
    output = []
    get_next_tr = False
    #iterate over rows, each corresponding to a runner
    for tr in soup.find_all(name='tr'):
        # Boston 2010-2015 has 2-line running data
        if get_next_tr:
            run_data = []
            for field in tr.find_all(name='td'):
                run_data.append(field.text.strip())
            runner.extend(run_data[1:16])
            output.append(tuple(runner))
            get_next_tr = False
        if tr.get('class') == ['tr_header']:
            runner = []
            url = ''
            for field in tr.find_all(name='td'):
                runner.append(field.text.strip())
                # Look for URL inside name cell
                # eg. <a href="javascript:OpenDetailsWindow('30556')">April, Lusapho</a>
                if geturl:
                    if field.find(name='a'):
                        url = field.find(name='a').attrs['href']
            if geturl:
                runner.append(url)
            get_next_tr = True
    return output


def extract_to_CSV(collections, column_names, directory='data/', geturl=True):
    client = MongoClient('mongodb://localhost:27017/')
    for collection_name in collections:
        collection = client['marathon'][collection_name]

        runners = []
        print 'Extracting runners from collection:',collection_name
        for document in collection.find():
            document_runners = get_runners_data(document['content'], geturl)
            runners.extend(document_runners)
            sys.stdout.write('.')
        print
        print 'Number of runners found:',len(runners)
        df = pd.DataFrame(runners)
        print 'Number of columns:', len(df.columns)
        df.columns = column_names
        filename = directory+collection_name+'_marathon.csv'
        print 'Saving to',filename
        df.to_csv(filename, index=False, encoding='UTF-8')

if __name__ == '__main__':
    print 'Script that extracts HTML stored in MongoDB collections to .csv file'
    print 'Press [ENTER] to start extractions'
    _ = raw_input()
    collections = ['bos15','bos14','bos13','bos12','bos11','bos10']
    column_names = ('bib', 'name', 'age', 'gender', 'city', 'state',
            'country', 'citizenship', 'subgroup', 'url', 'd5k', 'd10k',
            'd15k', 'd20k', 'half', 'd25k', 'd30k', 'd35k', 'd40k', 'pace',
            'projtime', 'offltime','overall','genderrank','division')
    extract_to_CSV(collections, column_names)

    #2009 and earlier have a different data format (14 columns)
    collections = ['bos09','bos08','bos07','bos06','bos05','bos04','bos03','bos02','bos01']
    column_names = ('year', 'bib', 'name', 'age', 'gender', 'city', 'state',
            'country', 'subgroup', 'overallrank', 'genderrank',
            'divisionrank', 'Officialtime', 'nettime')
    extract_to_CSV(collections, column_names, geturl=False)
