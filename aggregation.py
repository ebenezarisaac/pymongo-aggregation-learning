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

# Match title = "A Star Is Born":
stage_match_title = {
   "$match": {
         "title": "A Star Is Born"
   }
}

# Sort by year, ascending:
stage_sort_year_ascending = {
   "$sort": { "year": pymongo.ASCENDING }
}


# Limit to 1 document:
stage_limit_1 = { "$limit": 1 }

# Now the pipeline is easier to read:
pipeline = [
   stage_match_title, 
   stage_sort_year_ascending,
   stage_limit_1
]

results = movie_collection.aggregate(pipeline)


for movie in results:
   print(" * {title}, {first_castmember}, {year}".format(
         title=movie["title"],
         first_castmember=movie["cast"][0],
         year=movie["year"],
   ))