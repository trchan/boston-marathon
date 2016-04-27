# Boston Marathon - Winners and Cheaters

#### Timothy Chan
#### Created April 10th, 2016
#### Updated April 26th, 2016

## Overview

This repo contains an analysis of the Boston Marathon running data.  A dataset consisting of runners in the 2015 Boston marathon was compiled by scraping data from the Boston Athletics Association (BAA), marathonguide.com, and Weather Underground.  A model was successfully built that predicted a runner's finish time for the 2015 Boston Marathon with a standard error of 11 minutes.

A secondary model was built to flag outlier runners as possible cheaters.  A runner's deviation from their predicted time was found to be a good classifier when compared against a labelled data set provided by Derek Murphy (marathoninvestigations.com).  This classifier had good recall and earned an AUC of 0.944.

## Methodology

- bosscraper2015.py - for scraping 2010-2016 Boston Marathon data from the Boston Athletics Association.  Data is stored in MongoDB.
- bosscraper2009.py - for scraping 2001-2009 Boston Marathon data from the Boston Athletics Association.  Data is stored in MongoDB.
- extractboston.py - For converting scraped Boston Marathon data stored in MongoDB to a raw .csv file.
- cleanboston.py - For processing Boston Marathon raw.csv files into a clean format.
- marathonlib.py
- marathonguide.py - For scraping marathon data from marathonguide.com.
- cleanmarathonguide.py - For cleaning raw marathon data from marathonguide.com.
- wunderground.py - For scraping weather data from weatherunderground.com.
- combineboston.py - Takes clean.csv marathon data files (Boston Marathon, weatherunderground, and Marathonguide) and merges them together with extra features.
