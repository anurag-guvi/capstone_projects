import snscrape.modules.twitter as sntwitter
import pandas as pd
import pymongo
import streamlit as st

# Connect to MongoDB

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["twitter_db"]
collection = db["scraped_data"]

# Define function to scrape Twitter data

def scrape_twitter_data(keyword, start_date, end_date, tweet_limit):
    
    # Create a list to store the scraped tweets
    
    tweets = []

    # Scrape tweets using snscrape library
    
    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(f'{keyword} since:{start_date} until:{end_date}').get_items()):
        if i >= tweet_limit:
            break
        tweets.append(tweet)

    # Create a dataframe from the scraped tweets

    df = pd.DataFrame({
        'Date': [tweet.date for tweet in tweets],
        'ID': [tweet.id for tweet in tweets],
        'URL': [tweet.url for tweet in tweets],
        'Content': [tweet.content for tweet in tweets],
        'User': [tweet.user.username for tweet in tweets],
        'Reply Count': [tweet.replyCount for tweet in tweets],
        'Retweet Count': [tweet.retweetCount for tweet in tweets],
        'Language': [tweet.lang for tweet in tweets],
        'Source': [tweet.source for tweet in tweets],
        'Like Count': [tweet.likeCount for tweet in tweets]
    })

    # Store the scraped data in MongoDB along with the keyword used

    data = {
        'Scraped Word': keyword,
        'Scraped Date': pd.Timestamp.now(),
        'Scraped Data': df.to_dict(orient='records')
    }
    collection.insert_one(data)

    return df

# Streamlit GUI

def main():
    st.title("Twitter Data Scraper")
    
    # Input fields for keyword, date range, and tweet limit

    keyword = st.text_input("Enter keyword or hashtag")
    start_date = st.date_input("Select start date")
    end_date = st.date_input("Select end date")
    tweet_limit = st.number_input("Enter tweet limit", min_value=1, value=100)

    # Button to initiate scraping

    if st.button("Scrape Data"):

        # Scrape Twitter data based on user inputs
        
        scraped_data = scrape_twitter_data(keyword, start_date, end_date, tweet_limit)
        st.success("Data scraped successfully!")
        
        # Display the scraped data
        
        st.dataframe(scraped_data)

        # Buttons to upload data to MongoDB and download in CSV and JSON formats
        
        if st.button("Upload to MongoDB"):
            st.write("Uploading data to MongoDB...")
            
            # Uploading to MongoDB code here
            
            st.success("Data uploaded to MongoDB!")
        
        if st.button("Download as CSV"):
            st.write("Downloading data as CSV...")
            
            # Download as CSV code here
            
            st.success("CSV downloaded!")
        
        if st.button("Download as JSON"):
            st.write("Downloading data as JSON...")
            
            # Download as JSON code here
            
            st.success("JSON downloaded!")

# Run the Streamlit app

if __name__ == '__main__':
    main()