from DbConnector import DbConnector
from tabulate import tabulate

#Making the database for mysql

class ActivityDB:

  def __init__(self):
    self.connection = DbConnector()
    self.db_connection = self.connection.db_connection
    self.cursor = self.connection.cursor
  
#I do not use the boleean value of "Has labels", as all the users registered to the database 
#will have labels, as those are the users we will be investigating.
  def create_user_table(self):
    query = """CREATE TABLE IF NOT EXISTS User (
                id VARCHAR(3) NOT NULL PRIMARY KEY
                )
            """
#This executes the query and creates the User table 
    self.cursor.execute(query)
    self.db_connection.commit()

  def create_activity_table(self):
    query = """CREATE TABLE IF NOT EXISTS Activity (
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                user_id VARCHAR(3) NOT NULL,
                FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE ON UPDATE CASCADE,
                transportation_mode VARCHAR(10) NOT NULL,
                start_date_time DATETIME NOT NULL,
                end_date_time DATETIME NOT NULL
                )
            """
#This executes the query and creates the Activity table
    self.cursor.execute(query)
    self.db_connection.commit()
  
  def create_trackpoint_table(self):
    query = """CREATE TABLE IF NOT EXISTS TrackPoint (
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                activity_id INT NOT NULL,
                FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE ON UPDATE CASCADE,
                lat DOUBLE NOT NULL,
                lon DOUBLE NOT NULL,
                altitude INT NOT NULL,
                date_days DOUBLE NOT NULL,
                date_time DATETIME NOT NULL
                )
            """
#This executes the query and creates the TrackPoint table
    self.cursor.execute(query)
    self.db_connection.commit()

#Function for inserting data to the User table
  def insert_user_data(self, user):
    query = "INSERT INTO User(id) VALUES('%s')"
    self.cursor.execute(query % ('%03d' % user))
    self.db_connection.commit()
  
#Function for inserting data to the Activity table
  def insert_activity_data(self, user_id, transportation_mode, start_date_time, end_date_time):
    query = "INSERT INTO Activity(user_id, transportation_mode, start_date_time, end_date_time) VALUES('%s', '%s', '%s', '%s')"
    self.cursor.execute(query % ('%03d' % user_id, transportation_mode, start_date_time, end_date_time))
    self.db_connection.commit()
  
#Function for inserting data to the TrackPoint table.
#The foreign key to the Activity table is chosen by selecting the last id created, as we will be importing the data chronologically
  def insert_trackpoint_data(self, lat, lon, altitude, date_days, date_time):
    query = "INSERT INTO TrackPoint(activity_id, lat, lon, altitude, date_days, date_time) VALUES((SELECT MAX(id) FROM `Activity`), %s, %s, %s, %s, '%s')"
    self.cursor.execute(query % (lat, lon, altitude, date_days, date_time))
    self.db_connection.commit()
  
#Function for showing the tables
  def show_tables(self):
        self.cursor.execute("SHOW TABLES;")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
  
#Function for dropping the tables
  def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)