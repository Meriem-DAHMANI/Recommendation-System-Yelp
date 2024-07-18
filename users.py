import pymongo
import json
import re

# Connect to MongoDB (assuming it's running locally on the default port)
client = pymongo.MongoClient()

# Create or access a database
#to change if you changed the document
database = client["MongodbProject"]

# Create or access a collection within the database
users_db = database["users"]

# users
def process_user_data(filename_user, filename_review, filename_restaurant):
    with open(filename_user, 'r', encoding='utf-8') as file:
        data_user = json.load(file)
    
    with open(filename_review, 'r', encoding='utf-8') as file:
        data_review = json.load(file)

    with open(filename_restaurant, 'r', encoding='utf-8') as file:
        data_restaurant = json.load(file)
      

        for user in data_user:
            # Extract necessary information
            user_id = user['user_id']
            name = user['name']
            friends = user.get('friends', [])
            nb_friends = len(friends)


            evaluated_business_ids = []
            review_stars_given = []
            categories = []
            ambiences = []

            for review in data_review:
              if review['user_id'] == user_id:
                evaluated_business_ids.append(review['business_id'])
                review_stars_given.append(review['stars'])
                restaurant_id = review['business_id']
                new_categories, new_ambiences = get_categories_ambiences(restaurant_id, data_restaurant)
                categories += new_categories
                ambiences += new_ambiences 
            
            # get the rating of friends to visited restaurants 
            # first get the list of restaurants visited by friends
            friend_ratings = {}
            for friend_id in friends:
                for review in data_review:
                    if review['user_id'] == friend_id:
                        restaurant_id = review['business_id']
                        if restaurant_id in friend_ratings:
                            friend_ratings[restaurant_id].append(review['stars'])
                        else:
                            friend_ratings[restaurant_id] = [review['stars']]

            # then Calculate average ratings for each restaurant
            friend_avg_ratings = {}
            for restaurant_id, ratings in friend_ratings.items():
                friend_avg_ratings[restaurant_id] = sum(ratings) / len(ratings)

             # Create document
            user_document = {
                'user_id': user_id,
                'name': name,
                'friends': friends,
                'nbFriends': nb_friends,
                'friend_avg_ratings' : friend_avg_ratings,
                'categories' : list(set(categories)),
                'ambiences' : ambiences,
                'review_stars': review_stars_given,
                'evaluated_business_id': evaluated_business_ids
            }
            # Insert into MongoDB
            users_db.insert_one(user_document)


# function to retrieve the ambiance list
def get_ambience(restaurant):
    if (restaurant["attributes"] != None) and ("Ambience" in restaurant["attributes"].keys()) and (restaurant['attributes']['Ambience'] != "None" ) :
        ambience_dict = eval(restaurant['attributes']['Ambience'].replace("'", '"')) if 'Ambience' in restaurant['attributes'] else {}
        # Filter out False values in the Ambience dictionary
        ambience_list = [key for key, value in ambience_dict.items() if value is True]
    else :
         ambience_list = []
    
    return ambience_list

def get_categories_ambiences(restaurant_id, restaurant_data):
    for restaurant in restaurant_data:
        if restaurant['business_id'] == restaurant_id:
            categories = restaurant.get('categories', [])
            ambiences = get_ambience(restaurant)
            return [word.strip().lower() for word in categories.split(',')], ambiences

pathData1 = 'yelp_user.json'
pathData2 = 'yelp_review.json'
pathData3 = 'yelp_restaurants.json'

process_user_data(pathData1, pathData2, pathData3)