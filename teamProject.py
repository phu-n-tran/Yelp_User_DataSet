# --------------------------------------------------------------------------
# Name:
# Author(s):   Phu Tran
# --------------------------------------------------------------------------

import pymongo
from datetime import datetime

def printFunc(cursor):
    counter = 0
    for i in cursor:
        print(i)
        counter += 1
        if counter > 25:
            break

def option1(collection):
    print("\nFind Online-Trolls (by review count, rating, usefulness)")
    reviewCount = int(input("Enter the review count: "))
    rating = int(input("Enter the rating: "))
    usefulness = int(input("Enter the usefulness: "))

    results = collection.aggregate([{"$project":{"user_id":1,"compliments":{
        "$add":["$compliment_hot","$compliment_more","$compliment_profile",
                "$compliment_cute","$compliment_list","$compliment_note",
                "$compliment_plain","$compliment_cool","$compliment_funny",
                "$compliment_writer","$compliment_photos"]},
                "review_count":"$review_count","useful":"$useful",
                "average_stars":"$average_stars"}},{"$match":{"compliments":{
                "$lte":20},"review_count":{"$gte":reviewCount},"useful":
                {"$lte":usefulness},"average_stars":{"$lte":rating}}}])

    printFunc(results)

    return True


def option2(collection):
    print("\nFind Online-Trolls (by review count, rating, friends)")
    reviewCount = int(input("Enter the review count: "))
    rating = int(input("Enter the rating: "))
    numFriends = int(input("Enter the number of friends: "))

    results = collection.aggregate([{"$project":{"user_id":1,
                                    "review_count":"$review_count",
                                    "average_stars":"$average_stars",
                                    "fans":"$fans","enoughFriends":
                                    {"$cond":{"if":{"$isArray":"$friends"},
                                              "then":{"$size":"$friends"},
                                              "else":0}},}},
                                    {"$match":{"review_count":
                                                   {"$gte":reviewCount},
                                               "average_stars":{"$lte":rating},
                                               "fans":{"$lt":numFriends},
                                               "enoughFriends":
                                                   {"$lt":numFriends}}}])

    printFunc(results)

    return True


def option3(collection):
    print("\nFind Online-Trolls (by given a list of user id)")
    friendList = input("Enter list of friends (use comma as separator): "
                       "").split(",")
    print(friendList)

    results = collection.find({"user_id":{"$in": friendList}},
                              {"_id":0,"user_id":1,"friends":1})

    printFunc(results)

    return True


def option4(collection):
    print("\nFind Online-Trolls (by year)")
    year = input("Enter year: ")

    results = collection.aggregate([{"$project":{"user_id":1, "date" : {
    "$arrayElemAt":[{"$split": ["$yelping_since" , "-"]}, 0]},"compliments":
        {"$add":["$compliment_hot","$compliment_more","$compliment_profile",
                 "$compliment_cute","$compliment_list","$compliment_note",
                 "$compliment_plain","$compliment_cool","$compliment_funny",
                 "$compliment_writer","$compliment_photos"]},
                                                 "review_count":"$review_count",
                                                 "useful":"$useful",
                                                 "average_stars":"$average_stars",
                                                 "date":"$yelping_since"}},
                                    {"$match":{"compliments":{"$lte":20},
                                               "review_count":{"$gte":50},
                                               "useful":{"$lte":10},
                                               "average_stars":{"$lte":3},
                                               "date":{"$gt":year}}}])
    printFunc(results)

    return True

def option5(collection):
    print("\nFind Online-Trolls (by review count, rating, friends, year")
    reviewCount = int(input("Enter the review count: "))
    rating = int(input("Enter the rating: "))
    numFriends = int(input("Enter the number of friends: "))
    year = input("Enter year: ")

    results = collection.aggregate([{"$project":{"review_count":"$review_count",
                                                 "date" : {"$arrayElemAt":
                                                     [{"$split": [
                                                         "$yelping_since" ,
                                                         "-"]}, 0]}, "average_stars":"$average_stars",
                                                 "fans":"$fans","enoughFriends":{"$cond":{"if":{"$isArray":"$friends"},
                                                                                          "then":{"$size":"$friends"},
                                                                                          "else":0}},
                                                 "date":"$yelping_since"}},
                                    {"$match":{"review_count":{"$gte":reviewCount},
                                               "average_stars":{"$lte":rating},
                                               "fans":{"$lt":numFriends},
                                               "enoughFriends":{"$lt":numFriends},
                                               "date":{"$gt":year}}}])
    printFunc(results)
    return True


def find_user(collection):
    print("\nFind Users by:")
    print("\t1. name")
    print("\t2. name and number of reviews")
    print("\t3. Back to main menu")

    ans = int(input("Enter the option number: "))

    if ans == 1:
        username = input("Enter the users' name: ")
        output = int(input("Number of output results: "))
        results = collection.find({"name": username})

        i = 0
        while i < output:
            print(results.next())
            i += 1

    elif ans == 2:
        pass
    elif ans == 3:
        return False
    else:
        print("wrong option input!")
        return False

    return True


def add_user(collection):
    ZERO = 0
    print("\nAdd User")
    username = input("Enter the user's name: ")

    # datetime object containing current date and time
    now = datetime.now()
    # dd/mm/YY H:M:S
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")

    collection.insert_one({"user_id": str(hash(username+dt_string)),
                           "name": username, "review_count": ZERO,
                           "yelping_since": dt_string, "useful": ZERO,
                           "funny": ZERO, "cool": ZERO, "elite": [],
                           "friends": [], "fans": ZERO, "average_stars": ZERO,
                           "compliment_hot": ZERO, "compliment_more": ZERO,
                           "compliment_profile": ZERO, "compliment_cute": ZERO,
                           "compliment_list": ZERO, "compliment_note": ZERO,
                           "compliment_plain": ZERO, "compliment_cool": ZERO,
                           "compliment_funny": ZERO, "compliment_writer": ZERO,
                           "compliment_photos": ZERO})
    print("User (" + username + "} added successfully!")
    return True


def update_user_info(collection):
    print("\nUpdate User Information (provide username, review_count, "
          "avg star) by:")
    print("\t1. Username")
    print("\t2. ...tba")
    print("\t3. Back to main menu")

    ans = int(input("Enter the option number: "))

    if ans == 1:
        username = input("Enter username: ")
        review = int(input("Enter review count: "))
        avg_stars = float(input("Enter average stars: "))
        new_name = input("Enter new username: ")

        collection.update_one({"name": username, "review_count": review,
                               "average_stars": avg_stars},
                              {"$set": {"name": new_name}})

    elif ans == 2:
        pass
    elif ans == 3:
        return False
    else:
        print("wrong option input!")
        return False

    return True


def delete_user(collection):
    print("\nDelete User by:")
    print("\t1. User ID")
    print("\t2. Username, review count, and average star")
    print("\t3. Back to main menu")

    ans = int(input("Enter the option number: "))

    if ans == 1:
        user_id = input("Enter user id: ")

        collection.delete_one({"user_id": user_id})

    elif ans == 2:
        pass
    elif ans == 3:
        return False
    else:
        print("wrong option input!")
        return False

    return True


def option9(collection):
    print("\nFind average review count")

    results = collection.aggregate([{"$group": {"_id":"null", "pop": {"$avg":"$review_count"} } }])

    printFunc(results)

    return True


def option10(collection):
    print("\nFind out standard deviation of review count")

    results = collection.aggregate([{"$group":{"_id": "null" ,
                                               "stdDev": {"$stdDevPop":
                                                              "$review_count"}}}])
    printFunc(results)
    return True


def option11(collection):
    print("\nFind out average number of reviews per year")

    results = collection.aggregate([{ "$project":{"date":
        {"$arrayElemAt":[{"$split": ["$yelping_since" , "-"]}, 0]} } },
                                    { "$unwind" : "$date" },
                                    { "$group" : { "_id": {"year" : "$date"},
                                                   "count" : { "$sum" : 1 } }},
                                    { "$sort" : { "count" : -1 } } ])
    printFunc(results)
    return True


def option12(collection):
    print("\nFind out most useful user")

    results = collection.find({},{"user_id": 1 , "name" :1,
                                  "useful":1}).sort(-1).limit(5)

    printFunc(results)
    return True


def option13(collection):
    print("\nFind out funniest user")

    results = collection.find({},{"user_id": 1 , "name" :1, "funny":1}).sort(
        "funny",-1).limit(5)

    printFunc(results)
    return True


def option14(collection):
    print("\nFind out coolest user")

    results = collection.find({},{"user_id": 1 , "name" :1, "cool":1}).sort(
        "cool",-1).limit(5)

    printFunc(results)
    return True


def option15(collection):
    print("\nFind out super user")

    results = collection.find({},{"user_id": 1 , "name" :1, "review_count":1,
                                  "cool": 1, "useful":1, "funny":1, "fans":1}
                              ).sort([("review_count", -1), ("cool",-1),
                                      ("useful",-1) , ("funny",-1),("fans",-1)]
                                     ).limit(5)

    printFunc(results)
    return True


def main():
    # for local mongodb
    client = pymongo.MongoClient("mongodb://localhost:27020/")
    # for connecting to docker toolbox with docker toolbox IP and exposing port
    # client = pymongo.MongoClient("192.168.99.100", 27119)
    db = client["yelpdb"]
    collection = db["yelpcollection"]

    options = {
        1: option1,
        2: option2,
        3:  option3,
        4:  option4,
        5:  option5,
        6:  add_user,
        7:  update_user_info,
        8:  delete_user,
        9:  option9,
        10: option10,
        11: option11,
        12: option12,
        13: option13,
        14: option14,
        15: option15
    }

    repeat = True

    print("***   *   ***")
    print(" *** *** ***")
    print("  *********elcome to the User Management for Yelp")

    while repeat:
        print("\nMain Menu:")
        print("\t1.  Find Online-Trolls (by review count, rating, usefulnes)")
        print("\t2.  Find Online-Trolls (by review count, rating, friends)")
        print("\t3.  Find Online-Trolls (by given a list of user id)")
        print("\t4.  Find Online-Trolls (by review count, compliments, "
              "rating, useful, year)")
        print("\t5.  Find Online-Trolls (by review count, rating, friends, "
              "year)")
        print("\t6.  Add a new User")
        print("\t7.  Update user information")
        print("\t8.  Delete a user or troll")
        print("\t9.  Find average review count")
        print("\t10. Find out standard deviation of review count")
        print("\t11. Find out average number of reviews per year")
        print("\t12. Find out most useful user")
        print("\t13. Find out funniest user")
        print("\t14. Find out coolest user")
        print("\t15. Find out super user")
        print("\t16. Quit")

        ans = int(input("Enter the option number: "))

        myFunction = options.get(ans, False)

        if myFunction:
            repeat = myFunction(collection)
        else:
            repeat = False


if __name__ == '__main__':
    main()
