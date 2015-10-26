

from query import *
import psycopg2
from faker import Faker
import random
import sys
import re
import time

fake = Faker()

user_credentials = {'host': 'localhost',
                    'database': 'openerp',
                    'user': 'postgres',
                    'password': 'focus'}
n=10

def value_provider(limit=None):
    return {"character": fake.random_letter(),
            "character varying": fake.random_letter(),
            "varchar": fake.pystr(max_chars=5),
            "char": fake.random_letter(),
            "text": fake.random_letter(),
            "bit": random.choice([True, False]),
            "varbit": random.choice([True, False]),
            "bit varying": random.choice([True, False]),
            "smallint": fake.random_digit(),
            "int": fake.random_number(),
            "bigint": fake.random_number(),
            "integer": fake.random_digit(),
            "decimal": fake.random_number(),
            "smallserial": fake.random_number(),
            "serial": fake.random_number(),
            "bigserial": fake.random_number(),
            "numeric": fake.random_digit(),
            "double precision": fake.random_number(),
            "real": fake.random_number(),
            "money": fake.random_number(),
            "bool": random.choice([True, False]),
            "boolean": random.choice([True, False]),
            "bytea": random.choice([0, 1, 10]),
            "date": fake.date(pattern="%Y-%m-%d"),
            "interval": fake.time(pattern="%H:%M:%S"),
            "time": fake.time(pattern="%H:%M:%S"),
            "timestamp": fake.date_time_this_year(),
            "timestamp without time zone": fake.date_time(),
            "timestamp with time zone": fake.date_time(),
            "time without time zone": fake.time(pattern="%H:%M:%S"),
            "time with time zone": fake.time(pattern="%H:%M:%S"),
            "cidr": fake.ipv4(),
            "inet": fake.ipv6(),
            "ARRAY": '{{"jame","came"}}',
            "oid": fake.random_number(),
            "string": fake.random_letter(),
            "null": fake.random_digit_or_empty(),
            "tsvector": fake.random_letter(),
            'USER-DEFINED': 'G',
            "year": fake.year()

            }


def check_constraint_value_provider(*args):
        return{
                "timestamp without time zone": timestamp_wo_timezone(*args),
                #"integer": random_number(*args)
                }
def date_provider(start='1900-01-01 00:00:00', end='2020-01-01 00:00:00'):
    time_format = '%Y-%m-%d %H:%M:%S'
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))
    ptime = stime + random.random() * (etime - stime)
    return time.strftime(time_format, time.localtime(ptime))


def timestamp_wo_timezone(*args):
    times_list = re.findall(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', str(args))
    return date_provider(*times_list)


# print check_constraint_value_provider(*my_time).get("timestamp without time zone")


try:
    connection = psycopg2.connect(**user_credentials)
    cursor = connection.cursor()
    #cursor.execute("set search_path to cust_capone")
    print "connected successfully"
except:
    print "unable to connect"



def constraint_finder(tables, cur):
    """finding the constraints of all the tables in db and put them in a dictionary"""
    cur.execute(domain_table_finder)
    table_with_domains = [table[0] for table in cur.fetchall()]
    for table in tables:
        cur.execute(query_for_constraint % table)
        constraint_dic = {}
        # all_constraints is a list of tuple where each tuple contains (column_name,key_name) like (student_id,primary_key)
        all_constraints = cur.fetchall()
        #print all_constraints
        for constraint in all_constraints:
            try:
                constraint_dic[constraint[0]].append(constraint[1])
            except:
                constraint_dic[constraint[0]] = [constraint[1]]
        cur.execute(query_for_check_constraint%table)
        check_constraints= cur.fetchall()
        cur.execute(query_for_unique_index%table)
        #print check_constraints
        for constraint in check_constraints:
            try:
                constraint_dic[constraint[0]].append(constraint[1])
            except:
                constraint_dic[constraint[0]]=[constraint[1]]
        if table in table_with_domains:
            cur.execute(query_for_domain_constraint % table)
            constraint_dic[cur.fetchall()[0][0]] = 'CHECK'

        cur.execute(query_for_unique_index%table)
        indexed_column = cur.fetchall()
        if len(indexed_column) != 0:
            for column in indexed_column:
                try:
                    constraint_dic[str(str(column[0]))].append('UNIQUE')
                except:
                    constraint_dic[str(column[0])] = ['UNIQUE']


        yield   constraint_dic


constraint_finder(all_tables, cursor)

def place_holder(columns):
    """creates tuple for inserting like if a table has 4 columns it returns('%s','%s','%s','%s')"""
    value_tuple = ()
    for i in range(columns):
        value_tuple += ('%s',)
    return str(tuple(value_tuple))


cursor.execute(domain_table_finder)
table_with_domains = [table[0] for table in cursor.fetchall()]


def inset_into_table(tables, cur, constraint, number, conn, domain_tables):
    for table in tables:
        cur.execute(query_from_datatypes % table)
        column_names = [value[0] for value in cur.fetchall()]
        #print table, column_names
        cur.execute(query_from_datatypes % table)
        column_datatypes = [value[1] for value in cur.fetchall()]

        constraint_dic = constraint.next()
        print table
        place_holder(len(column_names))
        value_holder_tuple = place_holder(len(column_names))
        # print "table name %s"% table


        for i in range(number):
            value_list = []
            for column in column_names:

                if column in constraint_dic:

                    if (('FOREIGN KEY' in constraint_dic[str(column)]) or ('UNIQUE' in constraint_dic[str(column)]) or ('PRIMARY KEY' in constraint_dic[str(column)])):
                        if column_datatypes[column_names.index(column)] == "boolean":
                            value_list.append(random.choice(['True','False']))
                        if column_datatypes[column_names.index(column)] == "date" or column_datatypes[column_names.index(column)] == 'timestamp without time zone':
                            val = value_provider().get(str(column_datatypes[column_names.index(column)]))
                            value_list.append(val)



                        elif column_datatypes[column_names.index(column)] != 'int' or column_datatypes[column_names.index(column)] != "bigint"   :
                            value_list.append(str(i))

                        else:
                            value_list.append(i)

                    if ('CHECK' in constraint_dic[str(column)]) and (table in domain_tables):
                        cur.execute(query_for_check_domain % table)
                        check_constraint = cur.fetchall()[0][0]
                        limits = re.findall(r'\d+', check_constraint)
                        limits = [int(i) for i in limits]
                        value = random.randrange(*limits)
                        value_list.append(value)
                    elif ('CHECK' in constraint_dic[str(column)]):
                        cur.execute(query_for_check % (table, column))
                        check_constraint = cur.fetchall()
                        #print table
                        #print  str(column_datatypes[column_names.index(column)])
                        value = check_constraint_value_provider(*check_constraint).get(
                            str(column_datatypes[column_names.index(column)]))
                        value_list.append(value)


                else:
                    #without constraint
                    val = value_provider().get(str(column_datatypes[column_names.index(column)]))
                    value_list.append(val)

            try:
                print column_datatypes
                print column_names
                print value_list
                if len(value_list)==1:
                    if type(value_list[0]) != 'str':
                        query = ("INSERT INTO %s VALUES ('%s') " % (table,str(value_list[0]) ))
                        cur.execute(query)
                    else:
                        query = ("INSERT INTO %s VALUES '%s) " % (table,str(value_list[0]) ))
                        cur.execute(query)
                    #cur.execute(query )
                else:
                    query = ("INSERT INTO %s VALUES " % (table) + value_holder_tuple)
                    cur.execute(query % tuple(value_list))
                conn.commit()
                print "insert successfull %s" % table
            except psycopg2.IntegrityError:
                print "problem found %s" % table
                print column_names
                print column_datatypes
                print value_list
                all_tables.append(table)


inset_into_table(all_tables, cursor, constraint_finder(all_tables, cursor), n, connection, table_with_domains)