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
restaurants_db = database["restaurants"]
users_db = database["users"]
comments_db = database["comments"]

# facteurs
# 1- calculate appreciation factor

def calculate_appreciation_factor(business_id, city_name):
    # Get the specified city document
    city_document = cities_db.find_one({'name_city': city_name})

    if city_document is not None:
        # Find the restaurant in the city's list
        restaurant = next((r for r in city_document.get('restaurants', []) if r['business_id'] == business_id), None)

        if restaurant is not None:
            # Extract relevant information for calculation
            global_rating = restaurant['stars']
            #global_rating = restaurant['stars']
            nb_reviews = restaurant['nbReviews']

            if nb_reviews > 0:  # Ensure there are reviews before calculating the factor
                # Calculate the maximum number of reviews in the city
                max_reviews_in_city = max(r['nbReviews'] for r in city_document.get('restaurants', []))

                # Calculate the appreciation factor
                appreciation_factor = (global_rating * nb_reviews) / (5.0 * max_reviews_in_city)

                return appreciation_factor

    # Return None if the city or restaurant is not found or has no reviews
    return None


# 2- calculate preference factor

def preference_factor(user_doc, restaurant_doc):
    
    user_preferences = user_doc["categories"] + user_doc["ambiences"]
    restaurant_criteria = restaurant_doc["categories"] + restaurant_doc["ambience"]

    intersection = len(set(user_preferences) & set(restaurant_criteria))
    union = len(set(user_preferences) | set(restaurant_criteria))

    if union == 0:
        return 0.0  
    else:
        return intersection / union
    

# 3 - calculate social factor

def social_factor(user_id, restaurant_id):   
 # Retrieve the user document from MongoDB based on user_id
    user_document = users_db.find_one({'user_id': user_id})

    if user_document:
        friends_ratings = user_document.get('friend_avg_ratings', {})

        # Check if there are friends' ratings for the given restaurant
        if restaurant_id in friends_ratings:
            # Return the precalculated average rating given by friends
         return friends_ratings[restaurant_id]  

# otherwise we return 0 if the user doesn't have any fiend
    return 0

# 4- Calculate comment factor

from collections import Counter
import re

def jaccard_similarity(vocab1, vocab2):
    intersection = vocab1 & vocab2
    union = vocab1 | vocab2
    return len(intersection) / len(union) if union else 0

def extract_words(comment):
    words = re.findall(r'\w+', comment.lower())
    return Counter(words)


def comment_factor(user_id, restaurant_id):

    # get restaurants rated 5.0 by user
    user_top_reviews = comments_db.find({'user_id': user_id, 'stars': 5.0})
    user_top_vocab = Counter()
    for review in user_top_reviews:
        user_top_vocab +=  extract_words(review['text'])

    # get reviews from other users for the specified restaurant
    other_reviews = comments_db.find({'business_id': restaurant_id, 'user_id': {'$ne': user_id}})
    max_jaccard_score = 0
    for review in other_reviews:
        other_vocab = extract_words(review['text'])
        jaccard_score = jaccard_similarity(user_top_vocab, other_vocab)
        max_jaccard_score = max(max_jaccard_score, jaccard_score)

    return max_jaccard_score

# general fct that call all functions of factors, calculate the final score and return a sorted list
def calculate_factors(user_id, city_name):
    # simplify the city name in order to find it in the database
    city_name  = city_name.lower() 
    
    # get the user doc
    user_doc = users_db.find_one({"user_id": user_id})
    
    # init constants 
    alpha, beta, gamma, lambdaa = 0.30, 0.35, 0.20, 0.15
    
    # Get all restaurants in the specified city
    city_document = cities_db.find_one({"name_city": city_name})
    if not city_document:
        print(f"No data found for the city: {city_name}")
        return

    restaurants_in_city = city_document.get('restaurants', [])

    # calculate factors for each restaurant
    recommendation_list = []
    for restaurant in restaurants_in_city:
        
        # calculate appreciation factor
        appreciation_factor_value = calculate_appreciation_factor(restaurant['business_id'], city_name)
        
        # calculate preference factor
        preference_factor_value = preference_factor(user_doc, restaurant)

        # calculate social factor
        social_factor_value = social_factor(user_id, restaurant['business_id'])
        
        # calculate comments factor
        comment_factor_value = comment_factor(user_id, restaurant['business_id'])

        # calculate the score
        score = alpha * appreciation_factor_value + beta * preference_factor_value + gamma * social_factor_value + lambdaa * comment_factor_value

        # append the score to the recommendation list
        recommendation_list.append((restaurants_db.find_one({'business_id': restaurant['business_id']}).get('name'),appreciation_factor_value, preference_factor_value, social_factor_value, comment_factor_value ,score)) 
    
    # sort the recommandations
    sorted_recommendation = sorted(recommendation_list, key=lambda x: x[5], reverse=True)[:20]
    
    return sorted_recommendation
