import sqlite3
from pathlib import Path

from pandas import Timestamp
from sqlalchemy.ext.declarative import declarative_base
from src.pandas_oop import models
from src.pandas_oop.fields import StringColumn, IntegerColumn, FloatColumn, DateColumn, BoolColumn

Base = declarative_base()

ABS_PATH = Path(__file__).resolve().parent.parent
# DB_CONNECTION = models.Connection(':memory:')
DB_CONNECTION = models.Connection(f'sqlite:///{ABS_PATH}/db/pandas_oop.db')
PEOPLE_DATA_FILE = ABS_PATH / 'static/data/people.csv'
PEOPLE2_DATA_FILE = ABS_PATH / 'static/data/people_jobs.csv'
CARS_DATA_FILE = ABS_PATH / 'static/data/cars.csv'


@models.Data
class PeopleNoTable(models.DataFrame):
    name = StringColumn()
    age = IntegerColumn()
    money = FloatColumn()
    insertion_date = DateColumn()
    is_staff = BoolColumn(true='yes', false='no')


@models.sql(table='people', con=DB_CONNECTION)
@models.Data
class People(models.DataFrame):
    name = StringColumn()
    age = IntegerColumn()
    money = FloatColumn()
    insertion_date = DateColumn(format='%d-%m-%Y')
    is_staff = BoolColumn(true='yes', false='no')


@models.Data
class PeopleJobs(models.DataFrame):
    name = StringColumn()
    job = StringColumn()


@models.Data
class MergedPeople(models.DataFrame):
    name = StringColumn()
    age = IntegerColumn()
    money = FloatColumn()
    insertion_date = DateColumn(format='%d-%m-%Y')
    is_staff = BoolColumn(true='yes', false='no')
    job = StringColumn()


@models.sql(table='people_numeric_bool', con=DB_CONNECTION)
@models.Data
class PeopleFromDatabase(models.DataFrame):
    name = StringColumn()
    age = IntegerColumn()
    money = FloatColumn()
    insertion_date = DateColumn()
    is_staff = BoolColumn(true=1, false=0)


@models.sql(table='people_from_db', con=DB_CONNECTION)
@models.Data
class PeopleFromDatabaseWithoutBoolArgs(models.DataFrame):
    name = StringColumn()
    age = IntegerColumn()
    money = FloatColumn()
    insertion_date = DateColumn()
    is_staff = BoolColumn()


@models.sql(table='cars', con=DB_CONNECTION)
@models.Data
class UniqueCars(models.DataFrame):
    name = StringColumn(unique=True)
    model = StringColumn(unique=True)
    random_string = StringColumn()


@models.sql(table='people_from_iter', con=DB_CONNECTION)
@models.Data
class PeopleFromIterator(models.DataFrame):
    name = StringColumn()
    age = IntegerColumn()
    money = FloatColumn()
    insertion_date = DateColumn()
    is_staff = BoolColumn()


@models.Data
class PeopleDeclaredWithDifferentFields(models.DataFrame):
    name_test = StringColumn(target_name='name')
    age = IntegerColumn()
    money_test = FloatColumn(target_name='money')
    insertion_date = DateColumn()
    is_staff = BoolColumn()


def retrieve_people():
    for x in range(1000):
        yield "John", x, 50.0, Timestamp("2005-02-02"), True
