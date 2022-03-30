from pathlib import Path
from unittest import TestCase
import pandas as pd
import numpy as np
import sqlite3
from pandas import Timestamp

from modules.custom_exeptions import ValidationError, MissingDecorator
from pandas_oop import models


DB_CONNECTION = sqlite3.connect(':memory:')
ABS_PATH = Path(__file__).resolve().parent.parent.parent
DATA_FILE = ABS_PATH / 'modules/data/data.csv'


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


class TestPandasOop(TestCase):

    def test_instance_is_dataframe(self):
        people = People()
        self.assertIsInstance(people, pd.DataFrame, "Not an instance of pandas dataframe")

    def test_append_list_to_one_column(self):
        people = People()
        people.name = self.name_list
        people.age = self.age_list
        people.money = self.money_list
        people.insertion_date = self.insertion_date_list
        people.is_staff = self.is_staff_list

        result = people.to_dict()

        self.assertEqual(result, self.expected_result)

    def test_from_csv(self):
        people = People(from_csv=DATA_FILE, delimiter=";")
        data = pd.read_csv(filepath_or_buffer=DATA_FILE, delimiter=";")
        result = people.to_dict()
        self.assertEqual(result, self.expected_result)
        self.assertEqual(people.insertion_date.dtype.type, np.datetime64, "Column is not a date")

    def test_from_sql_query(self):
        people = People(from_csv=DATA_FILE, delimiter=";")
        if people.is_valid():
            people.save(if_exists='replace', index=False)
        people_from_db = PeopleFromDatabase(from_sql_query='select * from people')
        self.assertEqual(people_from_db.to_dict(), people.to_dict())

    def test_from_sql_query_without_bool_args(self):
        people = People(from_csv=DATA_FILE, delimiter=";")
        if people.is_valid():
            people.save(if_exists='replace', index=False)
        people_from_db = PeopleFromDatabaseWithoutBoolArgs(from_sql_query='select * from people')
        self.assertEqual(people_from_db.to_dict(), people.to_dict())

    def test_dataframe_is_valid(self):
        people = People()
        people.name = self.name_list
        people.age = self.age_list
        people.money = self.money_list
        people.insertion_date = self.insertion_date_list
        people.is_staff = self.is_staff_list
        if people.is_valid():
            people.save(if_exists='replace', index=False)
        self.assertTrue(people.is_valid())

    def test_dataframe_validate(self):
        people = People()
        people.name = self.name_list
        people.age = self.age_list
        people.money = self.money_list
        people.insertion_date = self.string_insertion_date_list
        people.is_staff = self.is_staff_list
        people.validate()
        people.save(if_exists='replace', index=False)
        self.assertTrue(people.is_valid())

    def test_dataframe_is_not_valid(self):
        people = People()
        people.name = self.name_list
        people.age = self.age_list
        people.money = self.money_list
        people.insertion_date = self.string_insertion_date_list
        people.is_staff = self.is_staff_list
        if people.is_valid():
            people.save(if_exists='replace', index=False)
        self.assertFalse(people.is_valid())

    def test_missing_sql_decorator_error(self):
        people = PeopleNoTable(from_csv=DATA_FILE, delimiter=";")
        if people.is_valid():
            self.assertRaises(MissingDecorator, people.save, if_exists='replace', index=False)

    def test_table_attribute(self):
        people = People(from_csv=DATA_FILE, delimiter=";")
        if people.is_valid():
            people.save(if_exists='replace', index=False)
        people_from_db = PeopleFromDatabase(from_sql_query='select * from people')
        self.assertEqual(people_from_db.to_dict(), people.to_dict())

    def setUp(self):
        # Old school creation
        self.old_school_df = pd.DataFrame({'name': pd.Series(dtype='O'),
                                           'age': pd.Series(dtype='int'),
                                           'money': pd.Series(dtype='float'),
                                           'insertion_date': pd.Series(dtype='datetime64[ns]'),
                                           'is_staff': pd.Series(dtype='bool')})
        self.old_school_read_csv_df = pd.read_csv(DATA_FILE, delimiter=';', parse_dates=['insertion_date'])
        self.old_school_read_csv_df['is_staff'] = self.old_school_read_csv_df['is_staff'].map({'yes': True,
                                                                                               'no': False})

        # Test variable for new creation
        self.name_list = ["John", "Snow"]
        self.age_list = [15, 40]
        self.money_list = [13.6, 6.7]
        self.insertion_date_list = [Timestamp('2005-02-25'), Timestamp('2005-02-25')]
        self.is_staff_list = [True, False]

        self.expected_result = {
            'name': {
                0: 'John',
                1: 'Snow'
            },
            'age': {
                0: 15,
                1: 40
            },
            'money': {
                0: 13.6,
                1: 6.7
            },
            'insertion_date': {
                0: Timestamp('2005-02-25'),
                1: Timestamp('2005-02-25')
            },
            'is_staff': {
                0: True,
                1: False
            },
        }

        self.string_insertion_date_list = ['25-02-2005', '25-02-2005']
