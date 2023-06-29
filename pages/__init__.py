# Imports
from googleapiclient.discovery import build
from pymongo import MongoClient
import pymysql
from datetime import datetime
import isodate
import os

def get_secs(duration_str):
    '''
    Converts the duration from iso format to seconds.
    '''
    return isodate.parse_duration(duration_str).total_seconds()

def check_redundancy(channel_name):
    '''
    Checks if a particular channel already exists in the SQL DB.
    
    '''
    _,cursor = connect_to_sql()
    return cursor.execute(f"SELECT * FROM channels WHERE name = '{channel_name}';")

def check_if_sql_is_empty():
    """Checks if SQL DB is empty or not
    """
    conn,cursor = connect_to_sql()
    return cursor.execute("SHOW TABLES;")

# Params
def build_youtube_handle():
    """Establishes a connection with youtube API and returns that conenction handle
    """
    service_name = 'youtube'
    version = 'v3'
    api_key = 'AIzaSyDmHqqDoM6Hf-e1_QatPWY9ucxW8gWF4BI'
    youtube = build(
        serviceName=service_name,
        version=version,
        developerKey=api_key
    )
    return youtube


def fetch_channel_details(youtube,channel_id):
    """Get channel details using youtube API
    Keyword arguments:
    youtube -- The connection to youtube API,
    channel_id -- The ID of channel
    """
    request = youtube.channels().list(
        part = 'snippet,contentDetails,statistics',
        id = channel_id
    )
    response = request.execute()
    item = response['items'][0]
    channel_details = {
        'name': item['snippet']['title'],
        'id':channel_id,
        'description': item['snippet']['description'],
        'subscribers': item['statistics']['subscriberCount'],
        'videos': item['statistics']['videoCount'],
        'views': item['statistics']['viewCount']
    }
    playlist_details = {
        'playlist_id': item['contentDetails']['relatedPlaylists']['uploads'],
        'channel_id': channel_id
    }
    return channel_details,playlist_details

def fetch_video_ids(youtube,playlist_id):
    """Get all video IDs from the playlist id.
    Keyword arguments:
    youtube -- The connection to youtube API,
    playlist_id -- The playlist ID associated with the channel.
    """
    request = youtube.playlistItems().list(
        part = 'contentDetails',
        playlistId = playlist_id
    )
    response = request.execute()
    video_ids = []
    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])
    next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
        request = youtube.playlistItems().list(
            part = 'contentDetails',
            playlistId = playlist_id,
            pageToken = next_page_token
        )
        response = request.execute()
        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        next_page_token = response.get('nextPageToken')
    return video_ids

def fetch_video_details(youtube,video_id):
    """Get video details of a video
    Keyword arguments:
    youtube -- The connection to youtube API,
    video_id -- Video ID
    """
    req_cols = {'snippet':['title','description','publishedAt','tags'],
                'statistics':['commentCount','favoriteCount','likeCount','viewCount'],
                'contentDetails':['duration']}
    request = youtube.videos().list(
        part = 'snippet,contentDetails,statistics',
        id = video_id
    )
    response = request.execute()
    video = response['items'][0]
    video_details = {'video_id':video_id}
    for key in req_cols.keys():
        for value in req_cols[key]:
            try:
                video_details[value] = video[key][value]
            except:
                video_details[value] = None
    return video_details

def fetch_comment_details(youtube,video_id):
    """Get comments associated with a video
    Keyword arguments:
    youtube -- The connection to youtube API,
    video_id -- Video ID
    """
    try:
        request = youtube.commentThreads().list(
            part = 'snippet',
            videoId = video_id
        )
        response = request.execute()
        top_comments = {}
        for item in response['items'][:10]:
            top_comments[item['id']] = {
                'comment_id': item['id'],
                'comment_text': item['snippet']['topLevelComment']['snippet']['textOriginal'],
                'comment_author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'comment_published_date': item['snippet']['topLevelComment']['snippet']['publishedAt']
            } 
    except:
        top_comments = {}
    return top_comments

def fetch(channel_id):
    """Fetches all info of a channel using channel ID.
    Keyword arguments:
    channel_id -- Channel ID
    """
    main_info = {}
    youtube = build_youtube_handle()
    channel_details,playlist_details = fetch_channel_details(youtube,channel_id)
    main_info['channel_name'] = channel_details['name']
    main_info[channel_details['name']] = channel_details
    main_info['playlist_details'] = playlist_details
    video_ids = fetch_video_ids(youtube,playlist_details['playlist_id'])
    for video_id in video_ids:
        main_info[video_id] = fetch_video_details(youtube,video_id)
        main_info[video_id]['comments'] = fetch_comment_details(youtube,video_id)
    return main_info

# MongoDB Connection
def connect_to_mongodb():
    """Establishes a connection to MongoDB and returns the handle to `guvi_test` database.
    """
    #client = MongoClient('mongodb://localhost:27017')
    pwd = os.getenv("MONGO_DB_PWD")
    client = MongoClient(f'mongodb+srv://jayanth:{pwd}@clusterguvi.rzlbtlw.mongodb.net/')
    db = client['guvi_test']
    return db

def store_in_mongo_db(collection_details):
    """Stores the collection in MongoDB
    Keyword arguments:
    collection_details -- The collection which has to be stored.
    """
    db = connect_to_mongodb()
    db.youtube.insert_one(collection_details)

def fetch_from_mongo_db(channel_name):
    """Gets the collection from MongoDB of specified channel.
    Keyword arguments:
    channel_name -- Name of the channel
    """
    db = connect_to_mongodb()
    doc = db.youtube.find_one({'channel_name':channel_name})
    del doc['_id']
    channels_db,playlists_db,videos_db,comments_db = [],[],[],[]
    channels_db.append(doc[doc['channel_name']])
    del doc[doc['channel_name']],doc['channel_name']
    playlists_db.append(doc['playlist_details'])
    del doc['playlist_details']
    for video_id in doc.keys():
        comments = doc[video_id]['comments']
        del doc[video_id]['comments'],doc[video_id]['tags']
        doc[video_id]['playlist_id'] = playlists_db[0]['playlist_id']
        videos_db.append(doc[video_id])
        for comment_id in comments:
            comments[comment_id]['video_id'] = video_id
            comments_db.append(comments[comment_id])
    return channels_db,playlists_db,videos_db,comments_db

def fetch_channel_names():
    """Gets the channel names from MongoDB
    """
    db = connect_to_mongodb()
    result = db.youtube.find({},{'_id':0,'channel_name':1})
    return [channel['channel_name'] for channel in result]

# SQL Connection
def connect_to_sql():
    """Establishes a connection to SQL DB
    """
    #conn = pymysql.connect(host='localhost',user='root',password='root',db='guvi_projects_prac')
    conn = pymysql.connect(user="sql12629335",password=os.getenv("SQL_PWD"),host = "sql12.freesqldatabase.com",port=3306, database = "sql12629335")
    cursor = conn.cursor()
    return conn,cursor

def migrate_channels_db(channels_db):
    """Store channel details in SQL DB
    Keyword arguments:
    channels_db -- Details of the channel.
    """
    conn,cursor = connect_to_sql()
    sql = "INSERT INTO channels VALUES (%s,%s,%s,%s,%s,%s);"
    vals = (
        channels_db[0]['id'],
        channels_db[0]['name'],
        channels_db[0]['description'],
        int(channels_db[0]['subscribers']),
        int(channels_db[0]['videos']),
        int(channels_db[0]['views']),
        )
    cursor.execute(sql,vals)
    conn.commit()
    conn.close()

def migrate_playlist_db(channels_db):
    """Store playlist details in SQL DB
    Keyword arguments:
    playlist_db -- Details of the playlist.
    """
    conn,cursor = connect_to_sql()
    sql = "INSERT INTO playlists VALUES (%s,%s);"
    vals = (
        channels_db[0]['playlist_id'],
        channels_db[0]['channel_id']
        )
    cursor.execute(sql,vals)
    conn.commit()
    conn.close()

def migrate_videos_db(videos_db):
    """Store videos details in SQL DB
    Keyword arguments:
    videos_db -- Details of the videos.
    """
    conn,cursor = connect_to_sql()
    sql = "INSERT INTO videos VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    for video in videos_db:
        vals = (
            video['video_id'],video['playlist_id'],video['title'],video['description'],
            datetime.strptime(video['publishedAt'], '%Y-%m-%dT%H:%M:%SZ'),
            int(video['viewCount']),int(video['likeCount']),int(video['favoriteCount']),int(video['commentCount']),
            get_secs(video['duration'])
            )
        cursor.execute(sql,vals)
        conn.commit()
    conn.close()

def migrate_comments_db(comments_db):
    """Store comments details in SQL DB
    Keyword arguments:
    comments_db -- Details of the comments
    """
    conn,cursor = connect_to_sql()
    sql = "INSERT INTO comments VALUES (%s,%s,%s,%s,%s);"
    for comment in comments_db:
        vals = (
            comment['comment_id'],comment['video_id'],comment['comment_text'],comment['comment_author'],
            datetime.strptime(comment['comment_published_date'], '%Y-%m-%dT%H:%M:%SZ')
            )
        cursor.execute(sql,vals)
        conn.commit()
    conn.close()

queries = {
    'What are the names of all the videos and their corresponding channels?': 0,
    'Which channels have the most number of videos, and how many videos do they have?': 1,
    'What are the top 10 most viewed videos and their respective channels?': 2,
    'How many comments were made on each video, and what are their corresponding video names?': 3,
    'Which videos have the highest number of likes, and what are their corresponding channel names?': 4,
    'What is the total number of likes and dislikes for each video, and what are their corresponding video names?': 5,
    'What is the total number of views for each channel, and what are their corresponding channel names?': 6,
    'What are the names of all the channels that have published videos in the year 2022?': 7,
    'What is the average duration of all videos in each channel, and what are their corresponding channel names?': 8,
    'Which videos have the highest number of comments, and what are their corresponding channel names?': 9
    }

def apply_query(query):
    """Applies a query on the SQL DB.
    Keyword arguments:
    query -- The query that has to be applied.
    """
    conn,cursor = connect_to_sql()
    i = queries[query]
    if i == 0:
        sql = "SELECT v.video_name,c.name FROM channels c JOIN playlists p ON c.id = p.channel_id JOIN videos v on v.playlist_id = p.playlist_id;"
        cursor.execute(sql)
        return cursor,["Video Name","Channel Name"]
    elif i == 1:
        sql = "SELECT name,videos FROM channels ORDER BY videos DESC;"
        cursor.execute(sql)
        return cursor,["Channel Name","Video Count"]
    elif i == 2:
        sql = "SELECT c.name,v.video_name FROM channels c JOIN playlists p ON c.id = p.channel_id JOIN videos v on v.playlist_id = p.playlist_id ORDER BY v.view_count DESC LIMIT 10;"
        cursor.execute(sql)
        return cursor,["Channel Name","Video Name"]
    elif i == 3:
        sql = "SELECT video_name,comment_count FROM videos;"
        cursor.execute(sql)
        return cursor,["Video Name","Comment Count"]
    elif i == 4:
        sql = "SELECT v.video_name,v.like_count,c.name FROM channels c JOIN playlists p ON c.id = p.channel_id JOIN videos v on v.playlist_id = p.playlist_id ORDER BY v.like_count DESC;"
        cursor.execute(sql)
        return cursor,["Video Name","Like Count","Channel Name"]
    elif i == 5:
        sql = "SELECT video_name,like_count FROM videos;"
        cursor.execute(sql)
        return cursor,["Video Name","Like Count"]
    elif i == 6:
        sql = "SELECT name,views FROM channels;"
        cursor.execute(sql)
        return cursor,["Channel Name","Views"]
    elif i == 7:
        sql = "SELECT DISTINCT c.name FROM channels c JOIN playlists p ON c.id = p.channel_id JOIN videos v on v.playlist_id = p.playlist_id WHERE YEAR(v.published_date) = 2022;"
        cursor.execute(sql)
        return cursor,["Channel Name"]
    elif i == 8:
        sql = "SELECT c.name,AVG(v.duration) FROM channels c JOIN playlists p ON c.id = p.channel_id JOIN videos v on v.playlist_id = p.playlist_id GROUP BY c.name;"
        cursor.execute(sql)
        return cursor,["Channel Name","Average Duration(in secs)"]
    elif i == 9:
        sql = "SELECT c.name,v.video_name,v.comment_count FROM channels c JOIN playlists p ON c.id = p.channel_id JOIN videos v on v.playlist_id = p.playlist_id ORDER BY v.comment_count DESC;"
        cursor.execute(sql)
        return cursor,["Channel Name","Video Name","Comment Count"]