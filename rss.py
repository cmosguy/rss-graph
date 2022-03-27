#%%
"""
Function to import RSS from a URL
"""
from asyncio.windows_utils import pipe
from distutils.command.upload import upload
import re
from matplotlib.pyplot import get
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
load_dotenv()
import requests
from urllib.parse import urlparse
from google.cloud import speech
from google.cloud import storage
import os
import ffmpeg
import sys
from pathlib import Path
import subprocess
import dtale

from neo4j import GraphDatabase
import wikipedia
import time

import spacy
from spacy.lang.en.stop_words import STOP_WORDS
from spacy.matcher import Matcher
from spacy.tokens import Doc, Span, Token

from pywikibot.data import api
import pywikibot
import wikipedia
import pprint

import json
import urllib
from string import punctuation
import nltk
from extract_sds import get_rss_data


client = speech.SpeechClient()
storage_client = storage.Client()


#%% 
def ie_pipeline(text, relation_threshold=0.9, entities_threshold=0.8):
    # Prepare the URL.
    data = urllib.parse.urlencode([
        ("text", text), 
		("relation_threshold", relation_threshold),
        ("entities_threshold", entities_threshold),
        ("userkey", os.getenv('WIKIFIER_KEY')),
		])
    
    url = "http://localhost:5000?" + data
    req = urllib.request.Request(url, data=data.encode("utf8"), method="GET")
    with urllib.request.urlopen(req, timeout=150) as f:
        response = f.read()
        response = json.loads(response.decode("utf8"))
    # Output the annotations.
    return response


class Neo4jConnection:
    
    def __init__(self, uri, user, pwd, db):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        self.__db = db
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)
        
    def close(self):
        if self.__driver is not None:
            self.__driver.close()
        
    def query(self, query, parameters=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try: 
            session = self.__driver.session(database=self.__db) if self.__db is not None else self.__driver.session() 
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response

#%%
conn = Neo4jConnection(uri=os.getenv('NEO4J_URI'), user=os.getenv('NEO4J_USER'), pwd=os.getenv('NEO4J_PASSWORD'), db="rss")
def insert_data(query, rows, batch_size = 10000):
    # Function to handle the updating the Neo4j database in batch mode.

    total = 0
    batch = 0
    start = time.time()
    result = None

    while batch * batch_size < len(rows):

        res = conn.query(query, parameters={'rows': rows[batch*batch_size:(batch+1)*batch_size].to_dict('records')})
        try:
            total += res[0]['total']
        except:
            total += 0
        batch += 1
        result = {"total":total, "batches":batch, "time":time.time()-start}
        print(result)

    return result
	
def add_nodes(rows, batch_size=10000):
    # Adds author nodes to the Neo4j graph as a batch job.

    query = '''UNWIND $rows AS row
               MERGE (:Node {name: row.name, id: row.id, type: row.node_label})
               RETURN count(*) as total
    '''

def add_episodes(rows, batch_size=10000):

	query = """
	UNWIND $rows as row
	CREATE (a:Episode{content:row.content, title:row.title, episode_no:row.episode_no, summary:row.summary, size_mb:row.size_mb, image:row.image, link_mp3:row.link_mp3})
	FOREACH (rel in row.ie.relations | 
	MERGE (s:Entity{name:rel.source})
	MERGE (t:Entity{name:rel.target})
	MERGE (s)-[:RELATION]->(r:Relation{type:rel.type})-[:RELATION]->(t)
	MERGE (a)-[:MENTIONS_REL]->(r))
	WITH row, a
	UNWIND row.ie.entities as entity
	MERGE (e:Entity{name:entity.title})
	SET e.wikiId = entity.wikiId
	MERGE (a)-[:MENTIONS_ENT]->(e)
	WITH entity, e
	CALL apoc.create.addLabels(e,[entity.label]) YIELD node
	RETURN distinct 'done'
	"""

	return insert_data(query, rows, batch_size)

#%%

#%%
# uncommment if you want to spend time getting the wikififier data it takes a long time
#uncomment to read dat from excel read data from the text file
#print('Putting episode content into information extraction (ie) pipeline...')
# data['ie'] = data['content'].apply(lambda x: ie_pipeline(x))
# data = get_rss_data()
# data.to_excel('data.xlsx')
data = pd.read_excel('data.xlsx')

#%%
d = dtale.show(data)
d

#%%
tokens = pd.read_excel('tokens_super_data_science.xlsx')
#%%
tokens.info()

#%%
tokens.groupby("doc_id").size().hist(figsize=(14, 7), color="red", alpha=.4, bins=50);
#%%

#%% ADD TO NEO4J
print('Adding episodes to Neo4j...')
add_episodes(data, batch_size=100)

#%%
## display max width pandas output
pd.set_option('display.max_columns', None)
data.query('episode_no==503').iloc[0]['ie']

#%%
nlp = spacy.load('en_core_web_lg')
nlp.add_pipe('merge_noun_chunks')

print(nlp.pipe_names)
#%%
doc = nlp(data.query('episode_no==503').iloc[0]['content'])
doc
#%%
spacy.displacy.serve(doc, style='ent')
#%%
example_content = data.iloc[1].content
example_data = ie_pipeline(example_content)


#%%
import_direct_query = """
WITH $data as data
UNWIND data.entities as entity
MERGE (e:Entity{name:entity.title})
ON CREATE SET e.wikiId = entity.wikiId
WITH data, entity, e
CALL apoc.create.addLabels(e,[entity.label]) YIELD node
WITH data, count(*) as break_unwind
UNWIND data.relations as relation
MERGE (s:Entity{name:relation.source})
MERGE (t:Entity{name:relation.target})
WITH s,t,relation
CALL apoc.create.relationship(s, relation.type, {}, t) 
YIELD rel
RETURN distinct 'done'
"""

#%%
driver = GraphDatabase.driver('bolt://44.195.85.71:7687', auth=('neo4j', 'leadership-dependence-rushes'))

def run_query(query, params={}, db='neo4j'):
    with driver.session() as session:
        result = session.run(query, params, database=db)
        return pd.DataFrame([r.values() for r in result], columns=result.keys())

run_query(import_direct_query, {'data':example_data})
#%%
run_query("MATCH (n) DETACH DELETE n")
run_query("CREATE CONSTRAINT IF NOT EXISTS ON (e:Entity) ASSERT e.name IS UNIQUE;")
run_query("CREATE INDEX rels IF NOT EXISTS FOR (n:Relation) ON (n.type);")

#%%
import_refactored_query = """
UNWIND $params as value
CREATE (a:Episode{content:value.content, title:value.title, episode_no:value.episode_no, summary:value.summary, size_mb:value.size_mb, image:value.image, link_mp3:value.link_mp3})
FOREACH (rel in value.ie.relations | 
  MERGE (s:Entity{name:rel.source})
  MERGE (t:Entity{name:rel.target})
  MERGE (s)-[:RELATION]->(r:Relation{type:rel.type})-[:RELATION]->(t)
  MERGE (a)-[:MENTIONS_REL]->(r))
WITH value, a
UNWIND value.ie.entities as entity
MERGE (e:Entity{name:entity.title})
SET e.wikiId = entity.wikiId
MERGE (a)-[:MENTIONS_ENT]->(e)
WITH entity, e
CALL apoc.create.addLabels(e,[entity.label]) YIELD node
RETURN distinct 'done'
"""
with driver.session() as session:
	params = []
	last = 5
	for i,episode in tqdm(data[:last].iterrows(), total=data[:last].shape[0]):
		content = episode['content']
		ie_data = ie_pipeline(content)
		episode['ie'] = ie_data
		# params.append({'content':content, 'title':episode['title'], 'no':episode['episode_no'], 'ie':ie_data})
		params.append(episode.to_dict('records'))

		if (len(params) % 5 == 0):
			print('Importing episdes into Neo4J')
			session.run(import_refactored_query, {'params':params}, database='rss')
			params = []

    # session.run(update_query, {'params':params})

#%%
bbc_data = pd.read_csv('bbc-news-data.csv', '\t')
bbc = dtale.show(bbc_data)
bbc
#%%
non_nc = spacy.load('en_core_web_md')

nlp = spacy.load('en_core_web_md')
nlp.add_pipe('merge_noun_chunks')

print(nlp.pipe_names)

#%%
# ./bin/ffmpeg.exe -i \
# ./audio/1208982598-superdatascience-sds-546-daily-habit-4-alternate-nostril-breathing.mp3 \
# -filter_complex "[0:a]channelsplit=channel_layout=stereo[left][right]" \
# -map "[left]" ./audio/1208982598-superdatascience-sds-546-daily-habit-4-alternate-nostril-breathing_FL.flac \
# -map "[right]" ./audio/1208982598-superdatascience-sds-546-daily-habit-4-alternate-nostril-breathing_FR.flac
#%%
def upload_to_bucket(blob_name, path_to_file, bucket_name):
	bucket = storage_client.get_bucket(bucket_name)
	blob = bucket.blob(blob_name)
	blob.upload_from_filename(path_to_file)
	return blob

def decode_audio(speech_file, out_file, ffmpeg_bin = os.path.join('bin','ffmpeg.exe'), ffprobe_bin = './bin/ffprobe.exe'):
	stream_info = ffmpeg.probe(speech_file, cmd=ffprobe_bin)

	# stream = ffmpeg.input(speech_file)
	# audio = stream.audio

	# try:
	# 	# split = (
	# 	# 	ffmpeg.filter_multi_output(audio, 'split', channel_layout='stereo')  # or `.split()`
	# 	# )
	# 	out, err = (audio.output(out_file, format='s16le', acodec='pcm_s16le', ac=1, ar='16k')
	# 		.overwrite_output()
	# 		.run(capture_stdout=True, capture_stderr=True, cmd=ffmpeg_bin)
	# 	)
	# except ffmpeg.Error as e:
	# 	print(e.stderr, file=sys.stderr)
	# 	sys.exit(1)

	cmd = [ffmpeg_bin,
		'-y',
		'-i', speech_file,
		'-filter_complex', "[0:a]channelsplit=channel_layout=stereo[left][right]",
		'-map', "[left]", out_file,
		'-map', "[right]", out_file + '_FR.flac'
		]
	proc = subprocess.run(cmd, shell=True, capture_output=True)

	out = proc.stdout
	err = proc.stderr

	return out, err, stream_info
	
def get_transcripts(file_name):
	gcs_uri = 'gs://ds-lab/' + file_name
	audio = speech.RecognitionAudio(uri=gcs_uri)
	config = speech.RecognitionConfig(
		encoding=speech.RecognitionConfig.AudioEncoding.FLAC,
		sample_rate_hertz=44100,
		language_code='en-US'
	)

	output_config=speech.TranscriptOutputConfig(gcs_uri='gs://ds-lab/transcripts/' + file_name + '.json')

	# Compose the long-running request
	request = speech.LongRunningRecognizeRequest(
		audio=audio, config=config, output_config=output_config
	)

	# Detects speech in the audio file
	operation = client.long_running_recognize(request=request)

	# print("Waiting for operation to complete...")
	response = operation.result()
	# transcripts = [result.alternatives[0].transcript for result in response.results]
	# confidence = [result.alternatives[0].confidence for result in response.results]

	# return pd.DataFrame(list(zip(transcripts, confidence)), columns=['transcript', 'confidence'])
	return response

url = data.iloc[0].links[1].href
a = urlparse(url)
file_name = os.path.basename(a.path)
# check if audio file exists in the cloud
speech_file = os.path.join('audio', file_name)

out_file = os.path.join('audio', Path(speech_file).stem + '.flac').replace('\\','/')

if out_file in [blob.name for blob in storage_client.list_blobs('ds-lab')]:
	print("{} already exists.\n".format(out_file))
else:
	print("{} DOES NOT exists downloading and processing now.\n".format(out_file))

	if not os.path.exists(speech_file):
		r = requests.get(url)
		open(speech_file, 'wb').write(r.content)
	else:
		print('file already exists: {}\n'.format(speech_file))

	out, err, stream_info = decode_audio(speech_file, out_file)

	blob = upload_to_bucket(out_file, out_file, 'ds-lab')

#%%
# response = get_transcripts(out_file)
# response

#%%
# get bucket with name
out_file = 'audio/1211892361-superdatascience-sds-547-how-genes-influence-behavior-with-prof-jonathan-flint.flac'
bucket = storage_client.get_bucket('ds-lab')

# get blob from bucket
object_name = 'transcripts/' + out_file + '.json'
blob = bucket.get_blob(object_name)
print(blob)

# get content as string
results_string = blob.download_as_string()

# get transcript exported in storage bucket
storage_transcript = speech.LongRunningRecognizeResponse.from_json(
	results_string, ignore_unknown_fields=True
)

# Each result is for a consecutive portion of the audio. Iterate through
# them to get the transcripts for the entire audio file.
text = []
for result in storage_transcript.results:
	# The first alternative is the most likely one for this portion.
	print(f"Transcript: {result.alternatives[0].transcript}")
	print(f"Confidence: {result.alternatives[0].confidence}")
	text.append(result.alternatives[0].transcript)

#%%
print(text)

#%%
text_df = pd.DataFrame(text, columns=['transcript'])
text_df['char_count'] = text_df.transcript.str.len()
text_df.sort_values(by='char_count', ascending=False).head(20)

#%%
text_df['ie'] = text_df['transcript'].apply(lambda x: ie_pipeline(x))
#%%
d = dtale.show(text_df)
d
#%%
import json

info = json.loads(results_string)
info['results']
meta = speech.LongRunningRecognizeMetadata.from_json(results_string, ignore_unknown_fields=True)

#%%
type(meta)
#%%
data.iloc[0].summary
#%%
data.iloc[0].summary_detail
# %%
data.iloc[3].content
# %%
data.iloc[3].link
# %%
data.iloc[3].links

#%%
data.head()
#%%
data.iloc[5].summary


#%%
def extract_tokens_plus_meta(doc:spacy.tokens.doc.Doc):
    """Extract tokens and metadata from individual spaCy doc."""
    return [
        (i.text, i.i, i.lemma_, i.ent_type_, i.tag_, 
         i.dep_, i.pos_, i.is_stop, i.is_alpha, 
         i.is_digit, i.is_punct) for i in doc
    ]

def tidy_tokens(docs):
    """Extract tokens and metadata from list of spaCy docs."""
    
    cols = [
        "doc_id", "token", "token_order", "lemma", 
        "ent_type", "tag", "dep", "pos", "is_stop", 
        "is_alpha", "is_digit", "is_punct"
    ]
    
    meta_df = []
    for ix, doc in enumerate(docs):
        meta = extract_tokens_plus_meta(doc)
        meta = pd.DataFrame(meta)
        meta.columns = cols[1:]
        meta = meta.assign(doc_id = ix).loc[:, cols]
        meta_df.append(meta)
        
    return pd.concat(meta_df)   

# %%
# nlp = spacy.load("en_core_web_lg")
##/c/Users/kleinada/Anaconda3/envs/ds/python -m spacy download en_core_web_lg
# docs = list(nlp.pipe(info.summary))
# tokens = tidy_tokens(docs)
# ran the data on google colab here: https://colab.research.google.com/drive/1jvsOEZx4qcysdgBQIazI7P_fdpSR0QIQ?authuser=2#scrollTo=m-wP_gV71b7d
tokens = pd.read_excel('tokens_super_data_science.xlsx')
#%%
tokens.info()
#%%
tokens.groupby("doc_id").size().hist(figsize=(14, 7), color="red", alpha=.4, bins=50);
#%%
tokens.query("ent_type != ''").ent_type.value_counts()

#%%
tokens.query("(ent_type == 'ORG') and (token == 'NVIDIA')").head()

#%%
info.iloc[73].summary

#%%
tokens.ent_type.unique()

#%%
tokens.query("(token == 'Ocelot')")

info.iloc[25].summary
#%%
with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', None):
	print(tokens.query("(doc_id == 25) and (token_order == 70)"))
#%%
data[data.isin('NVIDIA').any(1)]
#%%
(tokens
 .groupby("doc_id")
 .apply(lambda x: x.assign(
     prev_token = lambda x: x.token.shift(1), 
     next_token = lambda x: x.token.shift(-1))
       )
 .reset_index(drop=True)
 .query("tag == 'POS'")
 .loc[:, ["doc_id", "prev_token", "token", "next_token"]]
)
#%%
(tokens
 .groupby("doc_id")
 .apply(lambda x: x.assign(
     prev_token = lambda x: x.token.shift(1), 
     next_token = lambda x: x.token.shift(-1))
       )
 .reset_index(drop=True)
 .query("tag == 'ORG'")
 .loc[:, ["doc_id", "prev_token", "token", "next_token"]]
)
#%%
tokens.query('(doc_id == 25)')
#%%
data.iloc[5].subtitle
#%%
data.iloc[5].content

#%%
from spacy import displacy
doc = nlp(data.iloc[5].subtitle)
# displacy.serve(doc, style="ent")
