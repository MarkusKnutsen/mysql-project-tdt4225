def process_data(datapath):
  import numpy as np
  import os
  from datetime import datetime

  k=0

#For converting the date and time to datetime objects from tracjectory files
  date2str = lambda x: datetime.strptime(x.decode("utf-8"), '%Y-%m-%d').date()
  date2str_ = lambda x: datetime.strptime(x.decode("utf-8"), '%Y/%m/%d %H:%M:%S')
  time2str = lambda x: datetime.strptime(x.decode("utf-8"), '%H:%M:%S').time()

#For converting from bytes to string
  bytes2string = lambda x: x.decode("utf-8")

#Initializing names for the data
  d_headers = ["Latitude", "Longitude", "Zero", "Altitude", "Days", "Date", "Time"]
  t_headers = ["start_date", "end_date", "Transportation"]

#Specifying the datatypes
  data_dtypes = [(a, 'float') for a in d_headers[:-2]] + [(d_headers[5], 'object')] + [(d_headers[6], 'object')]
  transport_dtypes = [(a, 'object') for a in t_headers[:-1]] + [(t_headers[-1], "object")]

#Creating the data- and transport array for storing the users trajectories and transportation
  data = []
  transport = []

#The number of users in the research gets their own array of data for both transport and trajectories
  for i in range(182):
    data.append([])
    transport.append([])

  for (root,dirs,files) in os.walk(datapath, topdown=True):

#Slicing the path to know what user is currently being processed
    uid = ''.join(filter(str.isdigit, root))[-3:]

#This part is used for visualizing what user we are loading data for
    if k != uid:
      print("Loading data for user ", uid)
    k = uid

    for file in files:
#Path for each .plt file
      path = root + "\ "
      
#Counting the lines of the files
      with open((path+file).replace(" ",""), 'r') as fp:
        num_lines = sum(1 for line in fp)

#Generating the dataset for the trajectories
      if (file != "labels.txt") and num_lines <= 2506:
        entry = np.genfromtxt((path+file).replace(" ",""), delimiter=",", skip_header=6, dtype=data_dtypes, names=d_headers, converters = {"Date": date2str, "Time":time2str})
        data[int(uid)].append(entry) 

#Generating the dataset for the transportation
      if (file == "labels.txt"):
        entry = np.genfromtxt((path+file).replace(" ",""), delimiter="\t", skip_header=1, dtype=transport_dtypes, names=t_headers, converters = {"start_date": date2str_, "end_date":date2str_, "Transportation":bytes2string})
        transport[int(uid)] = entry
  return(data, transport)    