# Boston Marathon - Winners and Cheaters

#### Timothy Chan, PhD MBA
#### Created April 10th, 2016
#### Updated April 26th, 2016

## Overview

This repo contains an analysis of the Boston Marathon running data.  A dataset consisting of runners in the 2015 Boston marathon was compiled by scraping data from the Boston Athletics Association (BAA), marathonguide.com, and Weather Underground.  A model was successfully built that predicted a runner's finish time for the 2015 Boston Marathon with a standard error of 11 minutes.

A secondary model was built to flag outlier runners as possible cheaters.  A runner's deviation from their predicted time was found to be a good classifier when compared against a labelled data set provided by Derek Murphy (marathoninvestigations.com).  This classifier had good recall and earned an AUC of 0.944.

## Files Listings

marathon/
- bosscraper2015.py - for scraping 2010-2016 Boston Marathon data from the Boston Athletics Association.  Data is stored in MongoDB.
- bosscraper2009.py - for scraping 2001-2009 Boston Marathon data from the Boston Athletics Association.  Data is stored in MongoDB.
- extractboston.py - For converting scraped Boston Marathon data stored in MongoDB to a raw .csv file.
- cleanboston.py - For processing Boston Marathon raw.csv files into a clean format.
- marathonlib.py
- marathonguide.py - For scraping marathon data from marathonguide.com.
- cleanmarathonguide.py - For cleaning raw marathon data from marathonguide.com.
- wunderground.py - For scraping weather data from weatherunderground.com.
- combineboston.py - Takes clean.csv marathon data files (Boston Marathon, weatherunderground, and Marathonguide) and merges them together with extra features.

scripts/
- EDA2015Marathon.ipynb - EDA on 2015 Boston Marathon dataset
- Features 2015Marathon.ipynb
- Matching Estimators1.ipynb
- scrapingengine2001-2009.ipynb - BAA Scraper for 2001-2009
- scrapingengine2010-2016.ipynb - BAA Scraper for 2010-2016

## Acknowledgements

I reached out to several individuals who graciously offered me advice and information regarding the workings of the Boston Marathon and marathon running in general.

- **Derek Murphy**, (marathoninvestigations.com) graciously provided his hand-curated set of cheaters from the 2015 Boston Marathon.  Data Science is often filled with regret.  "If only we had this data".  Thanks to Derek Murphy, I had access to the critical data set that completes this project.  It was crucial to score my outlier classification model, and reject the multiple failed models that preceded it.  Derek also shared his expertise regarding how he spots cheaters.
- **Joslynn Lee**, Runner and Data Science Educator at Cold Spring Harbor Laboratory.

I would also like to thank the Galvanize Inc. instructors, Benjamin Skrainka, Ming Huang, and Brian Mann for sharing their advice and wisdom during the Data Science Intensive program.
