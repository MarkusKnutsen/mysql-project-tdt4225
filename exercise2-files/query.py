from DbConnector import DbConnector
from tabulate import tabulate

class query:

  def __init__(self):
    self.connection = DbConnector()
    self.db_connection = self.connection.db_connection
    self.cursor = self.connection.cursor
  
  def number_of_entries(self):
    query = """SELECT
            (SELECT COUNT(*) FROM User) as 'Number of Users', 
            (SELECT COUNT(*) FROM Activity) as 'Number of Activities',
            (SELECT COUNT(*) FROM TrackPoint) as 'Number of TrackPoints';
          """
    self.cursor.execute(query)
    rows = self.cursor.fetchall()
    print(tabulate(rows, headers=self.cursor.column_names, numalign="left"))
    print('\n')

  def average_number_of_activities(self):
    query = """SELECT 
              (SELECT COUNT(*) FROM Activity)/COUNT(*) as 'Average number of Activities per User'
              FROM User
          """
    self.cursor.execute(query)
    rows = self.cursor.fetchall()
    print(tabulate(rows, headers=self.cursor.column_names, numalign="left"))
    print('\n')
  
  def most_activities(self):
    query = """SELECT user_id AS User, COUNT(*) AS Activities
                FROM Activity
                GROUP BY user_id
                ORDER BY Activities DESC
                LIMIT 20     
          """
    self.cursor.execute(query)
    rows = self.cursor.fetchall()
    print("The 20 Users with the most Activities recorded:\n")
    print(tabulate(rows, headers=self.cursor.column_names, numalign="left"))
    print('\n')

  def has_taken_taxi(self):
    query = """SELECT DISTINCT user_id AS 'Users who has taken a taxi'
                FROM Activity
                WHERE transportation_mode
                LIKE 'taxi'    
          """
    self.cursor.execute(query)
    rows = self.cursor.fetchall()
    print(tabulate(rows, headers=self.cursor.column_names, numalign="left"))
    print('\n')

  def number_of_transportation_modes(self):
    query = """SELECT transportation_mode as 'Transportation mode',COUNT(*) as Activities 
              FROM Activity 
              GROUP BY transportation_mode 
              ORDER BY Activities DESC;
          """
    self.cursor.execute(query)
    rows = self.cursor.fetchall()
    print("Number of Activities the transportation modes were registered in:\n")
    print(tabulate(rows, headers=self.cursor.column_names, numalign="left"))
    print('\n')

  def year_with_activities(self):
    query = """SELECT YEAR(start_date_time) as Year,COUNT(*) as Activities 
              FROM Activity 
              GROUP BY Year 
              ORDER BY Activities DESC;
          """
    self.cursor.execute(query)
    rows = self.cursor.fetchall()
    print("Number of Activities per year:\n")
    print(tabulate(rows, headers=self.cursor.column_names, numalign="left"))
    print('\n')

  def hours_of_activities_per_year(self):
    query = """SELECT YEAR(start_date_time) as Year, ROUND(SUM(TIME_TO_SEC(TIMEDIFF(end_date_time, start_date_time))/3600),1) as Hours 
              FROM Activity 
              GROUP BY Year 
              ORDER BY Hours DESC;
          """
    self.cursor.execute(query)
    rows = self.cursor.fetchall()
    print("Hours of Activities per year:\n")
    print(tabulate(rows, headers=self.cursor.column_names, numalign="left"))
    print('\n')
  
  def distance_user_112(self):
    query = """SELECT SUM(ST_distance_sphere(POINT(x.Lon, x.Lat), POINT(x.Next_Lon, x.Next_Lat)))/1000 as 'Km walked by User 112 in 2008'
              FROM 
                (SELECT a.lat AS Lat, 
                        a.lon AS Lon, 
                        LAG(a.lat, 1) OVER (PARTITION BY b.user_id ORDER BY a.id) AS Next_Lat, 
                        LAG(a.lon, 1) OVER (PARTITION BY b.user_id ORDER BY a.id) AS Next_Lon
                  FROM TrackPoint a
                  JOIN Activity b ON a.activity_id=b.id 
                  WHERE b.user_id LIKE ('112') AND YEAR(b.start_date_time) LIKE (2008) AND b.transportation_mode LIKE ('walk')) x
              WHERE x.Next_Lat IS NOT NULL AND x.Next_Lon IS NOT NULL
            """


    self.cursor.execute(query)
    rows = self.cursor.fetchall()
    print(tabulate(rows, headers=self.cursor.column_names, numalign="left"))
    print('\n')

  def most_altitude(self):
    query = """SELECT x.User, FLOOR(SUM(GREATEST((x.next_alt - x.alt), 0))*0.3048) as Altitude
                FROM(
                  SELECT b.user_id AS User, 
                         a.altitude AS alt, 
                         LAG(a.altitude, 1) OVER (PARTITION BY b.user_id ORDER BY a.id) AS next_alt
                  FROM TrackPoint a
                  JOIN Activity b ON a.activity_id=b.id
                  WHERE a.altitude NOT LIKE (-777)
                ) x
                WHERE x.next_alt IS NOT NULL
                GROUP BY User
                ORDER BY Altitude DESC
                LIMIT 20
            """
    self.cursor.execute(query)
    rows = self.cursor.fetchall()
    print(tabulate(rows, headers=["User", "Meters gained in altitude"], numalign="left"))
    print('\n')
  
  def invalid_activities(self):
    query = """SELECT x.User, COUNT(DISTINCT(x.Activity)) as 'Invalid Activities'
                FROM
                  (SELECT b.user_id AS User,
                         b.id as Activity,
                         a.date_time AS timestamp, 
                         LAG(a.date_time, 1) OVER (PARTITION BY a.activity_id ORDER BY a.id) AS next_timestamp
                         
                  FROM TrackPoint a
                  JOIN Activity b ON a.activity_id=b.id
                  )x
                WHERE x.next_timestamp IS NOT NULL AND (TIMESTAMPDIFF(MINUTE,x.next_timestamp,x.timestamp) >=5) NOT LIKE (0)
                GROUP BY x.User
                ORDER BY COUNT(DISTINCT(x.Activity)) DESC
            """
    self.cursor.execute(query)
    rows = self.cursor.fetchall()
    print(tabulate(rows, headers=self.cursor.column_names, numalign="left"))
    print('\n')
  
  def forbidden_city(self):
    query = """SELECT x.User, COUNT(DISTINCT(x.Activity)) as 'Activities in the forbidden city of Beijing'
                FROM
                  (SELECT b.user_id AS User,
                         b.id as Activity,
                         a.lat as lat,
                         a.lon as lon       
                  FROM TrackPoint a
                  JOIN Activity b ON a.activity_id=b.id
                  )x
                WHERE x.lat LIKE('39.916') AND x.lon LIKE ('116.397')
                GROUP BY x.User
                ORDER BY COUNT(DISTINCT(x.Activity)) DESC
            """
    self.cursor.execute(query)
    rows = self.cursor.fetchall()
    print(tabulate(rows, headers=self.cursor.column_names, numalign="left"))
    print('\n')

  def most_used_transportation(self):
    query = """ SELECT x.user AS User, x.transport AS most_used_transportation_mode
                FROM
                  (SELECT a.user_id as user, 
                          a.transportation_mode as transport, 
                          COUNT(a.transportation_mode) as count,
                          ROW_NUMBER() OVER(PARTITION BY a.user_id ORDER BY COUNT(a.transportation_mode) DESC) AS number
                  FROM Activity a
                  GROUP BY a.user_id, a.transportation_mode
                  ORDER BY a.user_id, COUNT(a.transportation_mode) DESC) x
                WHERE x.number LIKE (1) 
            """
    self.cursor.execute(query)
    rows = self.cursor.fetchall()
    print(tabulate(rows, headers=self.cursor.column_names, numalign="left"))
    print('\n')

db = query()
db.number_of_entries()
db.average_number_of_activities()
db.most_activities()
db.has_taken_taxi()
db.number_of_transportation_modes()
db.year_with_activities()
db.hours_of_activities_per_year()
db.distance_user_112()
db.most_altitude()
db.invalid_activities()
db.forbidden_city()
db.most_used_transportation()

  