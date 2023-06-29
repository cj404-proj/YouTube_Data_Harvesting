import streamlit as st
import pages
import time
import pandas as pd

st.title("Youtube Data Harvesting")
with st.form("channel_id_form"):
    channel_id = st.text_input("Channel ID")
    submit = st.form_submit_button("Submit",use_container_width=True)
    if submit:
        with st.spinner("Scraping data..."):
            main_info = pages.fetch(channel_id)
        st.success("Data scrapped successfully.")
        with st.spinner("Loading data to MongoDB.."):
            time.sleep(5)
            pages.store_in_mongo_db(main_info)
        st.success("Data loaded successfully.")
    else:
        st.info("Please input the channel id and click submit")

with st.form("chanenl_names_form"):
    channel_name = st.selectbox("Channel Name",options=pages.fetch_channel_names())
    submit = st.form_submit_button("Submit")
    if submit:
        if pages.check_redundancy(channel_name) == 0:
            channels_db,playlists_db,videos_db,comments_db = pages.fetch_from_mongo_db(channel_name)
            with st.spinner("Loading data into SQL..."):
                pages.migrate_channels_db(channels_db)
                pages.migrate_playlist_db(playlists_db)
                pages.migrate_videos_db(videos_db)
                pages.migrate_comments_db(comments_db)
        else:
            st.warning("This data already exists in SQL")
    else:
        st.info("Please choose a channel to save to SQL.")

with st.form("queries_form"):
    query = st.selectbox("Query",options=list(pages.queries))
    submit = st.form_submit_button("Submit")
    if pages.check_if_sql_is_empty() == 0:
        st.warning("SQL is empty!!")
    else:
        cursor_data,columns = pages.apply_query(query)
        st.write(pd.DataFrame(cursor_data,columns=columns))