# YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit
# YouTube Channel Analyzer

YouTube Channel Analyzer is a Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application leverages the [Google API](https://developers.google.com/youtube/v3) to retrieve various information such as channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, and comments for each video. The retrieved data can be stored in a MongoDB database as a data lake, and users can also migrate specific channel data from the data lake to a SQL database for further analysis.

## Features

- **Retrieve YouTube Channel Data**: Users can input a YouTube channel ID and retrieve all relevant data using the Google API. The data includes channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, and comments for each video.

- **Data Storage in MongoDB**: The application provides an option to store the retrieved YouTube channel data in a MongoDB database as a data lake. This allows users to maintain a centralized repository of the collected data.

- **Data Collection for Multiple Channels**: Users can collect data for up to 10 different YouTube channels by simply clicking a button. This feature facilitates the efficient collection of data from multiple sources.

- **Migration to SQL Database**: Users can select a channel name and migrate its data from the data lake (MongoDB) to a SQL database. The data can be stored in the SQL database as tables, enabling more advanced analysis and querying capabilities.

- **Search and Retrieval from SQL Database**: The application offers the ability to search and retrieve data from the SQL database using different search options. Users can perform complex queries, including joining tables to obtain channel details and perform in-depth analysis.

## Application

The application is being hosted on `Streamlit` platform itself. It can be accessed via the  following link. [YouTube Harvesting Streamlit App](https://youtubedataharvesting.streamlit.app/)
