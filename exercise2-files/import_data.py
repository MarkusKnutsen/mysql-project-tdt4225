from process_data import *
from datetime import datetime
from ActivityDB import ActivityDB
import warnings

#Used so to not have a warning that pollutes the output every time.
warnings.simplefilter(action='ignore', category=FutureWarning)

data_, transport_ = process_data('dataset\dataset\Data')

#The data list will only contain the datetime objects of all the tracking points.
data = []

#The transport list will contain all the activities that are valid, i.e all the activities that have
#registered an exact match in start- and end date.
transport = []

#Making the lists have a spot for all the 181 users in this dataset.
for i in range(182):
    data.append([])
    transport.append([])

print('Creating the data array of dates.')
for i in range(len(data_)):

#Removing the data from people who have not recoded activities.
  if transport_[i] == []:
    data_[i] = []
  else:
    for item in data_[i]:
      for time in item:

#Making a new datalist where we have a datetime obect, and not two separated date and time objects.
        data[i].append(datetime.combine(time["Date"], time["Time"]))

print("Filtering for only activities with proper dates.")
for i in range(len(transport_)):

#Problem with iteration if it was in size 1.
  if transport_[i] != []:

#Checking if the datetime of the start and stop of the activity is recorded in the trajectories
#and if they are then they are stored in the new transportation list, where only the trasportations
#with corresponding trajectory points are.
#We now have all the recorded activities.
    if (transport_[i]).size == 1:
      if (transport_[i]["start_date"] in data[i]) and (transport_[i]["end_date"] in data[i]):
        transport[i].append((transport_[i]["start_date"], transport_[i]["end_date"], transport_[i]["Transportation"]))
    else:
      for item in ((transport_[i])):
        if (item[0] in data[i]) and (item[1] in data[i]):
          transport[i].append(item)


#Function for inserting the data into the database created in ActivityDB.py.

def main():

  database = None

  try:
#Creating the tables for the ActivityDB database.
    database = ActivityDB()
    database.create_user_table()
    database.create_activity_table()
    database.create_trackpoint_table()
    
#Iterating through the users
    for user in range(len(data)):

#Creating a flat list of the valid tracking points, instead of the tracking points being in lists containing to the files they were read from.
      f_data = [item for sublist in data_[user] for item in sublist]

#f_transport = [item for sublist in transport for item in sublist]

#Inserting the current user int>o the table User.
      print("User number: ", user)
      database.insert_user_data(user=user)

#Iteration variable. Used for tracking the index in the trajectory list. By doing this we do not need to read the already read entries
#and save computational time by doing so.
      iter = 0

#Iterating through the activities of the user.
      if transport[user] != []:
        for activity in transport[user]:

#A truth variable for verifying if the tracking point is within the activity interval.
          check = False

#Inserting the activity in the table Activity.
          database.insert_activity_data(user_id=user ,transportation_mode=activity[2], start_date_time=activity[0], end_date_time=activity[1])

#Iterating the tracking points of the trajectory of the user.
          for trackpoint in data[user][iter:]:

#If the trackpoint we are looking at is equal to the start time of the activity, we know we are inside the activity interval, and
#we set the truth variable to True.
            if trackpoint == activity[0]:
              check = True
            
#Inserting the tracking point to the table TrackPoint, only if we are inside the activity interval
            if check:
              database.insert_trackpoint_data(lat=f_data[iter]["Latitude"], lon=f_data[iter]["Longitude"], altitude=f_data[iter]["Altitude"], date_days=f_data[iter]["Days"], date_time=trackpoint)
            
#Updating the index
            iter += 1

#If we have reached the end of the activity, i.a. the trackpoint is equal to the end time of the activity,
#we set the truth variable to False. We also break out of the loop, as there is nothing more to be found in this activity. 
            if trackpoint == activity[1]:
              print("Activity registered for user ", user)
              check = False
              break

  except Exception as e:
    print("ERROR: Failed to use database:", e)
    database.drop_table(table_name="TrackPoint")
    database.drop_table(table_name="Activity")
    database.drop_table(table_name="User")

#Preforming the main function and creating the database.
if __name__ == '__main__':
    main()