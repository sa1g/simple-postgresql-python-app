import random
import uuid
import psycopg2
import string
import sys
import psycopg2.extras
from functools import wraps
from time import time
import logging
logger = logging.getLogger(__name__)

# global variables
# G: not a good idea
N = 1000000


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
                # use logging for this
                print(
                    f"Step {n_step} needs {round(end_*(10**9))  if end_ > 0 else 0} ns")
        return _time_it
    return wrapper


def connect():
    """ Connect to the PostgreSQL database server, returns connectoin and cursor """
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(
            database="DB", user="USER", host="HOST", password="USR_PWD")

        cur = conn.cursor()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return(conn, cur)
# useful functions


def get_random_numbers_in_list():
    """generates N unique random number in N range, returns a list"""
    # used in step 3
    a = random.sample([x for x in range(N) if x != 185], N-1)
    a.append(185)

    return a


def get_age():
    """generates N random ages, returns a list"""
    # used in Step 3
    for i in range(N):
        x = random.randint(18, 70)
        yield x


def get_random_string_in_list(length):
    """return a list of random STRING of length elements """
    for i in range(N):
        yield (''.join(random.choice(string.ascii_uppercase) for i in range(length)))


def get_random_uuid(string_length=10):
    """Returns a random string of length string_length."""
    random = str(
        uuid.uuid4())          # Convert UUID format to a Python string.
    random = random.upper()             # Make all characters uppercase.
    random = random.replace("-", "")    # Remove the UUID '-'.
    return random[0:string_length]      # Return the random string.


def uuid_list(string_length):
    """list of get_random_uuid, length: N"""
    for i in range(N):
        yield get_random_uuid(string_length)


def sailor_list():
    #id:        sequenziali
    # name(50):  get_random_string_in_list
    #address:   ""
    #age:       get_age
    #captain:   get_random_number_in_list
    # ~8.5s creating "data" with i7 laptop

    name = get_random_string_in_list(10)
    address = get_random_string_in_list(10)
    age = get_age()
    captain = get_random_numbers_in_list()
    data = []

    for i, (nm, addr, ag, cp) in enumerate(zip(name, address, age, captain)):
        data.append((i, nm, addr, ag, cp))

    return data


def get_random_int_in_list():
    """return a list of random int between 0 and N"""

    for i in range(N):
        yield (random.randint(0, N-1))


def boat_list():
    """return a list of "boat"'s tuples, we know that "captain"'s range is between 0 and N (because of how the DB is
    developed, so we can generate captain directly in that range w/o needing to query that column from the DB) """
    bid = uuid_list(15)
    bname = get_random_string_in_list(20)
    size = get_random_string_in_list(20)
    captain = get_random_int_in_list()

    for a, b, c, d in zip(bid, bname, size, captain):
        yield (a, b, c, d)


# Step 1 done


@measure(1)
def drop_tables(cur):
    """ drop tables if they already exist in the PostgreSQL database"""
    cur.execute(
        """DROP TABLE IF EXISTS "Sailor" CASCADE; DROP TABLE IF EXISTS "Boat";""")


# Step 2 done


@measure(2)
def create_tables(cur):
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

    for command in commands:
        cur.execute(command)


# Step 3 done


@measure(3)
def insert_tuple_Sailor(cur):
    """generates 1M tuples of Sailor."""
    sql = """INSERT INTO "Sailor" (id, name, address, age, level) VALUES %s;"""
    # ext 16 minutes at UNI, 20s in localhost
    argslist = sailor_list()

    psycopg2.extras.execute_values(cur, sql, argslist)

# Step 4 done


@measure(4)
def insert_tuple_Boat(cur):
    """generates 1M tuples of Boat, fk captain from Sailor"""
    sql = """INSERT INTO "Boat" (bid, bname, size, captain) VALUES %s;"""
    argslist = list(boat_list())

    psycopg2.extras.execute_values(cur, sql, argslist)


# Step 5 done


@measure(5)
def id_sailor_stderr5(cur):
    """retrieves from the database and prints to stderr all the id of the 1M Sailors """
    cur.execute("""SELECT "id" FROM "Sailor" """)

    a = cur.fetchall()
    for x in a:
        print(x[0], file=sys.stderr)


# Step 6 done


@measure(6)
def update_tuples_185(cur):
    """Updates tuples where level = 185 ==> level = 200 and prints them to stderr """

    cur.execute("""UPDATE "Sailor" SET "level" = 200 WHERE "level" = 185 """)


# Step 7 done


@measure(7)
def print_tuples_200(cur):
    """selects from the table sailor and prints to stderr the id and address fo the sailors with level = 200 """

    cur.execute(
        """SELECT "id", "address" FROM "Sailor" WHERE "level" = 200 """)
    a = cur.fetchall()
    for x in a:
        sys.stderr.write(str(x))

# Step 8 done


@measure(8)
def create_btree_index(cur):
    """creates btree index on the attribute "level" """

    cur.execute("""CREATE INDEX level_idx ON "Sailor"(level); """)


# Step 9 done


@measure(9)
def id_sailor_stderr9(cur):
    """retrieves from the database and prints to stderr all the id of the 1M Sailors """

    cur.execute("""SELECT "id" FROM "Sailor" """)

    a = cur.fetchall()
    for x in a:
        print(x[0], file=sys.stderr)


# Step 10 done


@measure(10)
def update_tuples_200(cur):
    """Updates tuples where level = 200 ==> level = 210 """

    cur.execute(
        """UPDATE "Sailor" SET "level" = 210 WHERE "level" = 200 """)

# Step 11 done


@measure(11)
def print_tuples_210(cur):
    """selects from the table sailor and prints to stderr the id and address fo the sailors with level = 210 """

    cur.execute(
        """SELECT "id", "address" FROM "Sailor" WHERE "level" = 210 """)
    a = cur.fetchall()
    for x in a:
        sys.stderr.write(str(x))


if __name__ == '__main__':
    dati_connessoine = connect()

    conn = dati_connessoine[0]
    cur = dati_connessoine[1]

    drop_tables(cur)
    create_tables(cur)
    insert_tuple_Sailor(cur)
    insert_tuple_Boat(cur)
    id_sailor_stderr5(cur)
    update_tuples_185(cur)
    print_tuples_200(cur)
    create_btree_index(cur)
    id_sailor_stderr9(cur)
    update_tuples_200(cur)
    print_tuples_210(cur)

    cur.close()
    conn.close()
