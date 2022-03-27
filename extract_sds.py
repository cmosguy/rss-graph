import feedparser
import pandas as pd
import re

def get_episode_no(x):
	"""Extract episode number from title"""
	search = re.search('SDS (\d+)', x)
	if search:
		return int(search.group(1))
	return None

def get_title(x):
	"""Extract episode number from title"""
	search = re.search('SDS \d+\s*:\s*(.*)', x)
	if search:
		return search.group(1)
	return None

# import rss feed from soundcloud into pandas
def rss_import(url):
	"""
	Function to import RSS from a URL
	"""
	# parse the rss feed
	feed = feedparser.parse(url)
	# create a list of dictionaries
	entries = []
	for entry in feed.entries:
		entries.append(entry)
	# create a dataframe from the list of dictionaries
	df = pd.DataFrame(entries)
	# return the dataframe
	return df

def get_weblink(x):
	"""
	Extract weblink from content
	"""
	search = re.search('www.([\w\-\.\/]+)', x)
	if search:
		return search.group(1)
	return None

def get_rss_data():
	url1 = "https://feeds.soundcloud.com/users/soundcloud:users:253585900/sounds.rss"
	url2 = "https://feeds.soundcloud.com/users/soundcloud:users:253585900/sounds.rss?before=319648311"

	data = pd.concat([rss_import(url1), rss_import(url2)])

	data['episode_no'] = data['title'].apply(lambda x: get_episode_no(x))
	data['title'] = data['title'].apply(lambda x: get_title(x))
	data['summary'] = data['summary']
	data['size_mb'] = data['links'].apply(lambda x: int(x[1]['length'])/1000000)
	data['content'] = data['content'].apply(lambda x: x[0]['value'])
	data['image'] = data['image'].apply(lambda x: x['href'])
	data['link_mp3'] = data['links'].apply(lambda x: x[1]['href'])
	data.drop(columns=['id', 'guidislink', 'title_detail', 'published_parsed', 'title_detail',
	'authors', 'author_detail', 'itunes_explicit', 'summary_detail','subtitle_detail','links'], inplace=True)

	return data