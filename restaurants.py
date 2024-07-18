import pymongo
import json
import re

# Connect to MongoDB (assuming it's running locally on the default port)
client = pymongo.MongoClient()

# Create or access a database
#to change if you changed the document
database = client["MongodbProject"]

# Create or access a collection within the database
restaurants_db = database["restaurants"]

# Restaurant 

def process_restaurant_data(filename_restaurant, filename_review):

    with open(filename_restaurant, 'r', encoding='utf-8') as file:
        data_restaurant = json.load(file)

    with open(filename_review, 'r', encoding='utf-8') as file:
        data_review = json.load(file)

    # Extract necessary information
    for restaurant in data_restaurant:
        business_id = restaurant['business_id']
        name = restaurant['name']

        # calculate the ambiance list
        ambience_list = get_ambience(restaurant)
        nb_ambience = len(ambience_list)
        
        review_user_ids = []
        review_stars = []
        for review in data_review : 
          if review['business_id'] == restaurant['business_id']:
            review_user_ids.append(review['user_id'])
            review_stars.append(review['stars'])

        # Create document
        restaurant_document = {
            'business_id': business_id,
            'name': name,
            'Ambience': ambience_list,
            'nbAmbience': nb_ambience,
            'review_user_id': review_user_ids,
            'review_stars': review_stars
        }

        # Insert into MongoDB
        restaurants_db.insert_one(restaurant_document)

# function to retrieve the ambiance list
def get_ambience(restaurant):
    if (restaurant["attributes"] != None) and ("Ambience" in restaurant["attributes"].keys()) and (restaurant['attributes']['Ambience'] != "None" ) :
        ambience_dict = eval(restaurant['attributes']['Ambience'].replace("'", '"')) if 'Ambience' in restaurant['attributes'] else {}
        # Filter out False values in the Ambience dictionary
        ambience_list = [key for key, value in ambience_dict.items() if value is True]
    else :
         ambience_list = []
    
    return ambience_list
        
pathData1 = 'yelp_restaurants.json'
pathData2 = 'yelp_review.json'
process_restaurant_data(pathData1, pathData2)

print("#####################################")
# display restaurants
cursor = restaurants_db.find()
print("All documents in the 'restaurant' collection:")
for document in cursor:
    print("restaurant = ", document)

