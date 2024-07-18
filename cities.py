import pymongo
import json
import re

# Connect to MongoDB (assuming it's running locally on the default port)
client = pymongo.MongoClient()

# Create or access a database
#to change if you changed the document
database = client["MongodbProject"]

# Create or access a collection within the database
cities_db = database["cities"]

# cities

import json
def process_cities_data(filename_restaurant, filename_review):
    with open(filename_restaurant, 'r', encoding='utf-8') as file:
        data_restaurant = json.load(file)

    with open(filename_review, 'r', encoding='utf-8') as file:
        data_review = json.load(file)

    # use a set to store all unique columns
    all_columns = set()

    # extract the list of cities
    cities_liste = []
    for obj in data_restaurant :
      city = obj["city"].lower().strip()
      if city not in cities_liste :
        cities_liste.append(city)

    #  Extract necessary information
    for city in cities_liste :
      nbRestaurants = 0
      list_restaurants = []
      for restaurant in data_restaurant:
        if restaurant["city"].lower().strip() == city :
            nbRestaurants +=1 
          
                
            # Extract Ambience list
            ambience_list = []
            if (restaurant["attributes"] != None) and ("Ambience" in restaurant["attributes"].keys()) and (restaurant['attributes']['Ambience'] != "None"):
                ambience_dict = eval(restaurant['attributes']['Ambience'].replace("'", '"')) if 'Ambience' in restaurant['attributes'] else {}
                # Filter out False values in the Ambience dictionary
                ambience_list = [key for key, value in ambience_dict.items() if value is True]

             # Calculate cotegories
            categories = restaurant.get('categories', [])
            list_categories = [word.strip().lower() for word in categories.split(',')]


            # calculate rating
            total_rating, review_count = 0, 0
            for review in data_review : 
                if review['business_id'] == restaurant['business_id']:
                    review_count += 1

            # add the restaurant that we just defined
            list_restaurants.append({
                    "business_id": restaurant['business_id'],
                    "name": restaurant['name'],
                    "address": restaurant["address"],
                    "stars": restaurant["stars"],
                    "nbReviews": review_count,
                    "categories": list_categories,
                    "ambience": ambience_list
                })

                  
      # Create document
        city_document = {
            'name_city': city,
            'nbRestaurants' : nbRestaurants,
            'restaurants': list_restaurants
        }

      cities_db.insert_one(city_document)


# construct collection of cities
pathData1 = 'yelp_restaurants.json'
pathData2 = 'yelp_review.json'
process_cities_data(pathData1, pathData2)

# display cities
cursor = cities_db.find()
print("All documents in the 'cities' collection:")
for document in cursor:
    print("city = ", document)
