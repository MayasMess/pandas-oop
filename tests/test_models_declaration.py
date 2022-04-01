import sqlite3
from pathlib import Path

from src.pandas_oop import models


ABS_PATH = Path(__file__).resolve().parent.parent
# DB_CONNECTION = models.Connection(':memory:')
DB_CONNECTION = models.Connection(f'sqlite:///{ABS_PATH}/pandas_oop.db')
PEOPLE_DATA_FILE = ABS_PATH / 'static/data/people.csv'
CARS_DATA_FILE = ABS_PATH / 'static/data/cars.csv'


@models.Data
class PeopleNoTable(models.DataFrame):
    name = models.StringColumn()
    age = models.IntegerColumn()
    money = models.FloatColumn()
    insertion_date = models.DateColumn()
    is_staff = models.BoolColumn(true='yes', false='no')


@models.sql(table='people', con=DB_CONNECTION)
@models.Data
class People(models.DataFrame):
    name = models.StringColumn()
    age = models.IntegerColumn()
    money = models.FloatColumn()
    insertion_date = models.DateColumn(format='%d-%m-%Y')
    is_staff = models.BoolColumn(true='yes', false='no')


@models.sql(table='people', con=DB_CONNECTION)
@models.Data
class PeopleFromDatabase(models.DataFrame):
    name = models.StringColumn()
    age = models.IntegerColumn()
    money = models.FloatColumn()
    insertion_date = models.DateColumn()
    is_staff = models.BoolColumn(true=1, false=0)


@models.sql(table='people', con=DB_CONNECTION)
@models.Data
class PeopleFromDatabaseWithoutBoolArgs(models.DataFrame):
    name = models.StringColumn()
    age = models.IntegerColumn()
    money = models.FloatColumn()
    insertion_date = models.DateColumn()
    is_staff = models.BoolColumn()


@models.sql(table='cars', con=DB_CONNECTION)
@models.Data
class UniqueCars(models.DataFrame):
    name = models.StringColumn(unique=True)
    model = models.StringColumn(unique=True)
    random_string = models.StringColumn()
