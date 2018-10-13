#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from functools import reduce
from pandas import DataFrame
import re

# Directories where bom and imdb CSVs reside
BOM_DIR = "bom" # /data/picadolj/Datasets/movies/bom/
IMDB_DIR = "bk-set8-schema1" # /data/picadolj/Datasets/imdb/bk-set8-schema1/

MONTHS = {'January' : 1,
          'February' : 2,
          'March' : 3,
          'April' : 4,
          'May' : 5,
          'June' : 6,
          'July' : 7, 
          'August' : 8,
          'September' : 9,
          'October' : 10,
          'November' : 11,
          'December' : 12,
          'Fall' : 10,
          'Winter' : 11
}

def dateFormat(unformattedDate):
    splitDate = re.split("[,]{0,1} ", unformattedDate)
    
    if len(splitDate) == 1:
        month = 1
        day = 1
        year = splitDate[0]
    elif len(splitDate) == 2:
        month = MONTHS[splitDate[0]]
        day = 1
        year = splitDate[1]
    else:
        month = MONTHS[splitDate[0]]
        day = splitDate[1]
        year = splitDate[2]
        
    formattedString = "%s" % (year)
    return formattedString

# Assumes columns as x, y, z. Not always true (see actors)
def getJoinedTable(xTOy, yTOz, xTitle, zTitle):
    xTOyDataFrame = pd.read_csv(xTOy,
                        names=[xTitle,'y'],encoding='ISO-8859-1',skipinitialspace = True)
    yTOzDataFrame = pd.read_csv(yTOz,
                        names=['y',zTitle],encoding='ISO-8859-1',skipinitialspace = True)
    return pd.merge(xTOyDataFrame,yTOzDataFrame,how='left', on='y')


# In[2]:


'''
                      Create IMDB database file 
'''
    
# Read in IMDB CSVs (release date only read in to create bom_id)
# concatenate values in cases of 1-to-n relationships
titleYear = pd.read_csv('%s/%s' % (IMDB_DIR, 'movies.csv'),
                        names=['imdb_id','title','year'],encoding='ISO-8859-1',skipinitialspace = True)

directors = getJoinedTable('%s/%s' % (IMDB_DIR, 'movies2directors.csv'), '%s/%s' % (IMDB_DIR, 'directors.csv'),
                          'imdb_id', 'director')
directors = directors.groupby('imdb_id')['director'].apply(lambda x: "%s" % ' '.join(x)).to_frame()

genres = pd.read_csv('%s/%s' % (IMDB_DIR, 'movies2genres_2.csv'),
                     names=['imdb_id','genre'], encoding='ISO-8859-1')
genres = genres.groupby('imdb_id')['genre'].apply(lambda x: "%s" % ' '.join(x)).to_frame()

writers = getJoinedTable('%s/%s' % (IMDB_DIR, 'movies2writers.csv'), '%s/%s' % (IMDB_DIR, 'writers.csv'),
                          'imdb_id', 'writer')
writers = writers.groupby('imdb_id')['writer'].apply(lambda x: "%s" % ' '.join(x)).to_frame()

composers = getJoinedTable('%s/%s' % (IMDB_DIR, 'movies2composers.csv'), '%s/%s' % (IMDB_DIR, 'composers.csv'),
                          'imdb_id', 'composer')
composers = composers.groupby('imdb_id')['composer'].apply(lambda x: "%s" % ' '.join(x)).to_frame()

costDesigners = getJoinedTable('%s/%s' % (IMDB_DIR, 'movies2costdes.csv'), '%s/%s' % (IMDB_DIR, 'costdesigners.csv'),
                          'imdb_id', 'costume_designer')
costDesigners = costDesigners.groupby('imdb_id')['costume_designer'].apply(lambda x: "%s" % ' '.join(x)).to_frame()

# Join CSV dataFrames
dataFrames = [titleYear, directors, genres, writers, composers, costDesigners]
imdbJoined = reduce(lambda left,right: pd.merge(left,right,how='left', on='imdb_id'), dataFrames)


# In[3]:


'''
                      Create BOM database file 
'''
# Read in BOM CSVs (release date only read in to create bom_id)
releaseDate = pd.read_csv('%s/%s' % (BOM_DIR, 'movies2releasedate_bom.csv'),
                          names=['title','release_date'], encoding='ISO-8859-1')
distributors = pd.read_csv('%s/%s' % (BOM_DIR, 'movies2distributor_bom.csv'),
                           names=['title','distributor'], encoding='ISO-8859-1')
totalgross = pd.read_csv('%s/%s' % (BOM_DIR, 'movies2totalgross_bom.csv'),
                         names=['title','total_gross'], encoding='ISO-8859-1')
inrelease = pd.read_csv('%s/%s' % (BOM_DIR, 'movies2inrelease_bom.csv'),
                        names=['title','in_release'], encoding='ISO-8859-1')
theaters = pd.read_csv('%s/%s' % (BOM_DIR, 'movies2theaters_bom.csv'),
                       names=['title','theaters'], encoding='ISO-8859-1')
prodbudget = pd.read_csv('%s/%s' % (BOM_DIR, 'movies2prodbudget_bom.csv'),
                         names=['title','prod_budget'], encoding='ISO-8859-1')
series = releaseDate.groupby('title').size()
series[series > 1]

# Join CSV dataFrames
dataFrames = [releaseDate, distributors, totalgross, inrelease, theaters, prodbudget]
bomJoined = reduce(lambda left,right: pd.merge(left,right,how='left', on='title'), dataFrames)

# Form bom_id as "TITLE-YEAR" and insert as first column. Drop release_date--it is no longer needed.
bom_ids = bomJoined.apply((lambda x: "%s-%s" % (x['title'], dateFormat(x['release_date']))), axis=1)
# bom_ids = bomJoined.apply((lambda x: "%s" % (x['title'])), axis=1)
bomJoined.insert(loc=0, column='bom_id', value=bom_ids.values)
bomJoined = bomJoined.drop(columns=['release_date'])


# In[4]:


'''
                    Create truth_table.csv and move IMDB attributes to bom_Joined
'''

# Form bom_id as "TITLE-YEAR" from title atribute
bom_ids = titleYear.apply((lambda x: "%s-%s" % (
    re.sub("(?: \([0-9]{4}\)){1}(?: \(.*\)){0,1}(?: \{.*\}){0,1}", "", x['title']), # Remove "(YEAR) (EXTRA) {EPISODE_INFO}" from end of title
    re.sub(".*\(([0-9]{4})\){1}.*", "\g<1>", x['title']))), axis=1) # Grab "(YEAR)" from title
#bom_ids = titleYear.apply((lambda x: "%s" % (
#    re.sub("(?: \([0-9]{4}\)){1}(?: \(.*\)){0,1}(?: \{.*\}){0,1}", "", x['title']))), axis=1) # Grab "(YEAR)" from title
titleYear.insert(loc=0, column='bom_id', value=bom_ids.values)
imdbTemp = titleYear.drop(columns=['title', 'year'])


# In[5]:


# Add attributes to move to bomJoined from imdbTemp
imdbPlots = pd.read_csv('%s/%s' % (IMDB_DIR, 'plots.csv'),
                        names=['imdb_id','plot'], encoding='ISO-8859-1')

imdbTechnical = pd.read_csv('%s/%s' % (IMDB_DIR, 'technical.csv'),
                            names=['imdb_id','technical'], encoding='ISO-8859-1')
imdbTechnical = imdbTechnical.groupby('imdb_id')['technical'].apply(lambda x: "%s" % ' '.join(x)).to_frame()

imdbProducers = getJoinedTable('%s/%s' % (IMDB_DIR, 'movies2producers.csv'), '%s/%s' % (IMDB_DIR, 'producers.csv'),
                          'imdb_id', 'producer')
imdbProducers = imdbProducers.groupby('imdb_id')['producer'].apply(lambda x: "%s" % ' '.join(x)).to_frame()

# Join CSV dataFrames
dataFrames = [imdbTemp, imdbPlots, imdbTechnical, imdbProducers]
imdbTemp = reduce(lambda left,right: pd.merge(left,right,how='left', on='imdb_id'), dataFrames)


# In[6]:


# Create a new DataFrame that is the union of the attributes of bomJoined and imdbTemp
bom_imdbJoined = pd.merge(bomJoined,imdbTemp,how='left', on='bom_id')


# In[7]:


# Finalize bomJoined
bomJoined = bom_imdbJoined.drop(columns=['imdb_id']) 
bomJoined.to_csv('bom_joined.csv', index=False, header=True)

# Write out imdbJoined
imdbJoined.to_csv('imdb_joined.csv', index=False, header=True)

# Create truth_table
truthTable = bom_imdbJoined.drop(columns=['title', 'distributor', 'total_gross', 
                                         'in_release', 'theaters', 'prod_budget',
                                        'plot', 'technical', 'producer'])
truthTable.dropna(subset=['imdb_id'], inplace=True)
truthTable = truthTable.drop_duplicates(['bom_id', 'imdb_id'])
truthTable.to_csv('truth_table.csv', index=False, header=True)


# In[ ]:




