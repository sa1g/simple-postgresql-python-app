import random
import uuid
import psycopg2
import string
import sys
import psycopg2.extras 
from functools import wraps
from time import time


#public variables
N = 1000000

#useful functions
def get_random_numbers_in_list():
    """generates N unique random number in N range"""
    #used in step 3
    a = random.sample([x for x in range(N) if x != 185], N-1)
    a.append(185)
    #print(a)
    return a

def get_age():
    """generates N random ages"""
    #used in Step 3
    a = []
    for i in range(N):
        x = random.randint(18, 70)
        a.append(x)
    #print(a)
    return a

def measure(n_step):
    def wrapper(func):
        """this function measures the execution time in ns of a function """
        @wraps(func)
        def _time_it(*args, **kwargs):
            start = time()
            try:
                return func(*args, **kwargs)
            finally:
                end_ = time() - start
                print(f"Step {n_step} needs {round(end_*(10**9))  if end_ > 0 else 0} ns")     
        return _time_it
    return wrapper

def get_random_string_in_list(length):
    """return a list of random STRING of length elements """
    a = []
    for i in range(N):
        a.append(''.join(random.choice(string.ascii_uppercase) for i in range(length)))
    #print(a)
    return a

def get_random_bid_in_list():
    #not gonna use it: too slow
    """return a list of cryptographically okay random STRING of 25 elements """
    a = []
    for i in range(N):
        a.append(''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(25)))
    #print(a)
    return a

def get_random_uuid(string_length=10):
    """Returns a random string of length string_length."""
    random = str(uuid.uuid4()) # Convert UUID format to a Python string.
    random = random.upper() # Make all characters uppercase.
    random = random.replace("-","") # Remove the UUID '-'.
    return random[0:string_length] # Return the random string.

def uuid_list(string_length):
    """list "my_random_string for N"""
    a = []
    for i in range(N):
        a.append(get_random_uuid(string_length))
    return a

def sailor_list():
    #id:        sequenziali
    #name(50):  get_random_string_in_list
    #address:   ""
    #age:       get_age
    #captain:   get_random_number_in_list
    #~8.5s creating "data" with i7 laptop
    name = get_random_string_in_list(10)
    address = get_random_string_in_list(10)
    age = get_age()
    captain = get_random_numbers_in_list()
    data = []
    for i in range(N):
        data.append((i, name[i], address[i], age[i], captain[i]))
        
    return data

#!new function
def get_random_int_in_list():
    """return a list of random int between 0 and N"""
    a = []
    for i in range(N):
        a.append(random.randint(0, N-1))
    return a
#!new function
def boat_list():
    """return a list of "boat"'s tuples, we know that "captain"'s range is between 0 and N (because of how the DB is 
    developed, so we can generate captain directly in that range w/o needing to query that column from the DB) """
    bid = uuid_list(15)
    bname = get_random_string_in_list(20)
    size = get_random_string_in_list(20)
    captain = get_random_int_in_list()

    a = []
    for i in range(N):
        a.append((bid[i], bname[i], size[i], captain[i]))
    return a
#function here for test purpose only
#@measure
def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
    # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        
        conn = psycopg2.connect(database="assignment3", user="assignment3", host="localhost", password="assignment3")
    # create a cursor
        cur = conn.cursor()
        
	# execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
	# close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

#Step 1 done
@measure(1)
def drop_tables():
    """ drop tables if they already exist in the PostgreSQL database"""
    conn = None

    try:
        #connect to the PostgreSQL server
        conn = psycopg2.connect(database="assignment3", user="assignment3", host="localhost", password="assignment3")
        cur = conn.cursor()
        #drop tables one by one
        
        cur.execute("""DROP TABLE IF EXISTS "Sailor" CASCADE; DROP TABLE IF EXISTS "Boat";""")
        
        #close communication w/PSQL server
        cur.close()
        #commit the changes
        conn.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

#Step 2 done
@measure(2)
def create_tables():
    """create tables in the PSQL database"""
   
    commands = (
        """ CREATE TABLE "Sailor"(
            "id"      INT PRIMARY KEY,
            "name"    CHAR(50) NOT NULL,
            "address" CHAR(50) NOT NULL,
            "age"     INT NOT NULL,
            "level"   FLOAT NOT NULL
        ); """,
        """ CREATE TABLE "Boat"(
         "bid"     CHAR(25) PRIMARY KEY,
         "bname"   CHAR(50) NOT NULL,
         "size"    CHAR(30) NOT NULL,
         "captain" INT NOT NULL REFERENCES "Sailor"("id") );"""
    )

    conn = None

    try:
        #connect to the PGSQL Server
        conn = psycopg2.connect(database="assignment3", user="assignment3", host="localhost", password="assignment3")
        cur = conn.cursor()
        #create tables one by one
        

        for command in commands:
            #check for safety
            #print("Excecuted command " + command)
            cur.execute(command)
        
        #close comm with the PGSQL Server
        cur.close()
        #commit the changes
        conn.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

#Step 3 done 
@measure(3)
def insert_tuple_Sailor():
    """generates 1M tuples of Sailor."""
    sql = """INSERT INTO "Sailor" (id, name, address, age, level) VALUES %s;"""
    #ext 16 minutes at UNI, 20s in localhost
    argslist = sailor_list()
    conn = None
    try:  
        #connect to the PGSQL Server
        conn = psycopg2.connect(database="assignment3", user="assignment3", host="localhost", password="assignment3")
        cur = conn.cursor()
       
        psycopg2.extras.execute_values(cur, sql, argslist)
        #close comm with the PGSQL Server
        cur.close()
        #commit the changes
        conn.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print("shit")
        print(error)
    finally:
        if conn is not None:
            conn.close()

#Step 4 done
@measure(4)
def insert_tuple_Boat():
    """generates 1M tuples of Boat, fk captain from Sailor"""
    sql = """INSERT INTO "Boat" (bid, bname, size, captain) VALUES %s;"""
    argslist = boat_list()
    conn = None
    try:  
        #connect to the PGSQL Server
        conn = psycopg2.connect(database="assignment3", user="assignment3", host="localhost", password="assignment3")
        cur = conn.cursor()
       
        psycopg2.extras.execute_values(cur, sql, argslist)
        #close comm with the PGSQL Server
        cur.close()
        #commit the changes
        conn.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print("shit")
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    #TODO remove this function when finished
def prova():
    
    conn = None
    try:  
        #connect to the PGSQL Server
        conn = psycopg2.connect(database="assignment3", user="assignment3", host="localhost", password="assignment3")
        cur = conn.cursor()
        cur.execute("""SELECT * FROM "Sailor" ORDER BY 1""")
        print(cur.rowcount)
        a = cur.fetchall()
        for x in a:
            print(x)
        #close comm with the PGSQL Server
        cur.close()
        #commit the changes
        #conn.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print("shit")
        print(error)
    finally:
        if conn is not None:
            conn.close()

#!Step 5 ERROR IN OUTPUT, rest of the code is fine
@measure(5)
def id_sailor_stderr():
    """retrieves from the database and prints to stderr all the id of the 1M Sailors """
    conn = None
    try:  
        #connect to the PGSQL Server
        conn = psycopg2.connect(database="assignment3", user="assignment3", host="localhost", password="assignment3")
        cur = conn.cursor()
        
        cur.execute("""SELECT "id" FROM "Sailor" """)
        
        a = cur.fetchall()
        
        for x in a:
            #error here.
            #!sys.stderr.write(str(x))
            #qui solo per far eseguire il tutto
            print(x[0])
        
        #close comm with the PGSQL Server
        cur.close()
        #commit the changes
        #conn.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print("shit")
        print(error)
    finally:
        if conn is not None:
            conn.close()

#Step 6 done
@measure(6)
def update_tuples_185():
    """Updates tuples where level = 185 ==> level = 200 and prints them to stderr """
    conn = None
    try:  
        #connect to the PGSQL Server
        conn = psycopg2.connect(database="assignment3", user="assignment3", host="localhost", password="assignment3")
        cur = conn.cursor()
        
        cur.execute("""UPDATE "Sailor" SET "level" = 200 WHERE "level" = 185 """)
        
        #close comm with the PGSQL Server
        cur.close()
        #commit the changes
        #conn.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print("shit")
        print(error)
    finally:
        if conn is not None:
            conn.close()

#Step 7 done
@measure(7)
def print_tuples_200():
    """selects from the table sailor and prints to stderr the id and address fo the sailors with level = 200 """
    conn = None
    try:  
        #connect to the PGSQL Server
        conn = psycopg2.connect(database="assignment3", user="assignment3", host="localhost", password="assignment3")
        cur = conn.cursor()
        
        cur.execute("""SELECT "id", "address" FROM "Sailor" WHERE "level" = 200 """)
        a = cur.fetchall()
        for x in a:
            sys.stderr.write(str(x))
        #close comm with the PGSQL Server
        cur.close()
        #commit the changes
        #conn.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print("shit")
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
#?Step 8
#?Step 9
#Step 10 done
@measure(10)
def update_tuples_200():
    """Updates tuples where level = 200 ==> level = 210 """
    conn = None
    try:  
        #connect to the PGSQL Server
        conn = psycopg2.connect(database="assignment3", user="assignment3", host="localhost", password="assignment3")
        cur = conn.cursor()
        
        cur.execute("""UPDATE "Sailor" SET "level" = 210 WHERE "level" = 200 """)
        
        #close comm with the PGSQL Server
        cur.close()
        #commit the changes
        #conn.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print("shit")
        print(error)
    finally:
        if conn is not None:
            conn.close()
#Step 11 done
@measure(11)
def print_tuples_210():
    """selects from the table sailor and prints to stderr the id and address fo the sailors with level = 210 """
    conn = None
    try:  
        #connect to the PGSQL Server
        conn = psycopg2.connect(database="assignment3", user="assignment3", host="localhost", password="assignment3")
        cur = conn.cursor()
        
        cur.execute("""SELECT "id", "address" FROM "Sailor" WHERE "level" = 210 """)
        a = cur.fetchall()
        for x in a:
            sys.stderr.write(str(x))
        #close comm with the PGSQL Server
        cur.close()
        #commit the changes
        #conn.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print("shit")
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    #connect()
    drop_tables()
    create_tables()
    insert_tuple_Sailor()
    #prova() #?qui per debugging
    insert_tuple_Boat()
    #id_sailor_stderr()
    update_tuples_185()
    print_tuples_200()
    update_tuples_200()
    print_tuples_210()

    #dovrebbe funzionare, poi si sposta l'output di step 5 qua
    #sys.stderr.write("ciao")

