import os
from pprint import pprint

import bson
from dotenv import load_dotenv
import pymongo

# Load config from a .env file:
load_dotenv(verbose=True)
MONGODB_URI = os.environ["MONGODB_URI"]

# Connect to your MongoDB cluster:
client = pymongo.MongoClient(MONGODB_URI)

# Get a reference to the "sample_mflix" database:
db = client["sample_mflix"]

# Get a reference to the "movies" collection:
movie_collection = db["movies"]

first_stage = { "$limit": 1000 }

# Look up related documents in the 'comments' collection:
stage_lookup_comments = {
   "$lookup": {
         "from": "comments", 
         "localField": "_id", 
         "foreignField": "movie_id", 
         "as": "related_comments",
   }
}

# Calculate the number of comments for each movie:
stage_add_comment_count = {
   "$addFields": {
         "comment_count": {
            "$size": "$related_comments"
         }
   } 
}

# Match movie documents with more than 2 comments:
stage_match_with_comments = {
   "$match": {
         "comment_count": {
            "$gt": 2
         }
   } 
}

# Limit to the first 5 documents:
stage_limit_5 = { "$limit": 5 }

pipeline = [
    first_stage,
   stage_lookup_comments,
   stage_add_comment_count,
   stage_match_with_comments,
   stage_limit_5,
]

results = movie_collection.aggregate(pipeline)

for movie in results:
   print(movie["title"])
   print("Comment count:", movie["comment_count"])

 # Loop through the first 5 comments and print the name and text:
for comment in movie["related_comments"][:5]:
    print(" * {name}: {text}".format(
    name=comment["name"],
    text=comment["text"]))