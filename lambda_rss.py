import mysql.connector
from mysql.connector import errorcode

import feedparser
from bs4 import BeautifulSoup

import re

import time
import datetime

import os

# RDS configuration details
config = {}
config['user'] = os.environ['db_username']
config['password'] = os.environ['db_password']
config['database'] = os.environ['db_name']
config['host'] = os.environ['db_host']

# remove papers from database after x days
outdate_treshold = os.environ['treshold']

# -------------------------------------------

# Get title, authors, abstract, link for each new paper on a given arxiv category
def parse_rss_arxiv(arxiv_category,rss_version='2.0'):

    rss_url = 'http://export.arxiv.org/rss/{}?version={}'.format(arxiv_category,rss_version)
    rss_dict = feedparser.parse(rss_url)
    
    # Check if HTTP status is successful
    if rss_dict.status != 200:
        raise RuntimeError('The HTTP response for the arXiv RSS is not succesfull.')
    
    # Date of publication of the feed
    published_date = time.strftime('%Y-%m-%d %H:%M:%S', rss_dict['feed']['published_parsed'])
    
    arxiv_list = []

    for paper in rss_dict['entries']:

        # We do not need to show old papers that have been updated
        if 'UPDATED' in paper['title']:
            continue
        
        entry = []
        
        # Get the title (remove arXiv id from it)
        entry.append(re.sub('\s\(arXiv.+\)','',paper['title']))

        soup = BeautifulSoup(paper['summary'],features="html.parser")

        # Get authors list
        entry.append(', '.join([authors.text for authors in soup.find_all('a')]))

        # Get the abstrct (remove new lines)
        summary_paragraphs = soup.find_all('p')
        abstract = summary_paragraphs[-1].text
        entry.append(re.sub('\n',' ', abstract))

        # Get the link
        entry.append(paper['link'])
        
        # Add the published date
        entry.append(published_date)
        
        arxiv_list.append(entry)
        
    return arxiv_list

# -------------------------------------------

def handler(event, context):

	try:
		cnx = mysql.connector.connect(**config)
	except mysql.connector.Error as err:
		if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
			print("Something is wrong with your credentials.")
		elif err.errno == errorcode.ER_BAD_DB_ERROR:
			print("Database does not exist")
		else:
			print(err)
	else:
		print('Succesfully connected to the database')
		
		cursor = cnx.cursor()

		# Insert new rows into the table
		sql_insert = ("REPLACE INTO papers (title, authors, abstract, link, date) "
					  "VALUES (%s, %s, %s, %s, %s)"
					 )

		for paper in parse_rss_arxiv('hep-th'):
			cursor.execute(sql_insert, paper)

		cnx.commit()
		print('New papers loaded into database')

		# Delete out-dated rows from the table
		sql_delete = "DELETE FROM papers WHERE date < %s"

		treshold = datetime.datetime.utcnow() - datetime.timedelta(days = outdate_treshold)
		treshold = treshold.strftime('%Y-%m-%d %H:%M:%S')

		cursor.execute(sql_delete, (treshold,))
		cnx.commit()
		print('Old papers removed from database')

		# Close connection to the database
		cnx.close()
