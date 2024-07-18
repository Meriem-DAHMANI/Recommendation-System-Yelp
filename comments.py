import pymongo
import json
import re

# Connect to MongoDB (assuming it's running locally on the default port)
client = pymongo.MongoClient()

# Create or access a database
#to change if you changed the document
database = client["MongodbProject"]

# Create or access a collection within the database
comments_db = database["comments"]

###########################################################
# Collection comments 
def process_comments_data(filename_review):

    with open(filename_review, 'r', encoding='utf-8') as file:
        data_review = json.load(file)
    
    for review in data_review:
            # Extract necessary information
            review_id = review['review_id']
            user_id = review['user_id']
            business_id = review['business_id']
            text = review['text']
            list_words = re.findall(r'\w+', review['text'].lower())
            stars = review['stars']

            # Create document
            comment_document = {
                'review_id': review_id,
                'user_id': user_id,
                'business_id': business_id,
                'text': text,
                'list_words' : list_words,
                'stars' : stars
            }

             # Insert into MongoDB
            comments_db.insert_one(comment_document)

pathData1 = 'yelp_review.json'

process_comments_data(pathData1)

print("#####################################")
# display users
cursor = comments_db.find()
print("All documents in the 'comments' collection:")
for document in cursor:
    print("comment = ", document)