from process_data import *
from datetime import datetime
import warnings
import numpy as np

warnings.simplefilter(action='ignore', category=FutureWarning)

data_, transport_ = process_data('dataset\dataset\Data')
data = []
transport = []
for i in range(182):
    data.append([])
    transport.append([])

for i in range(len(data_)):
  #Removing the data from people who have not recoded activities
  if transport_[i] == []:
    data_[i] = []
  else:
    for item in data_[i]:
      for time in item:
      #Making a new datalist where we have a datetime obect, and not two separated date and time objects
        data[i].append(datetime.combine(time["Date"], time["Time"]))

for i in range(len(transport_)):
  if transport_[i] != []:
    #Problem with iteration if it was in size 1
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
