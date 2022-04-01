from unittest import TestCase
import pandas as pd
import numpy as np
from pandas import Timestamp

from tests.test_models_declaration import People, PeopleNoTable, PEOPLE_DATA_FILE, PeopleFromDatabase, \
    PeopleFromDatabaseWithoutBoolArgs


class TestDataframeBehavior(TestCase):

    def test_instance_is_dataframe(self):
        people = People()
        self.assertIsInstance(people, pd.DataFrame, "Not an instance of pandas dataframe")

    def test_instance_is_dataframe_no_table(self):
        people = PeopleNoTable()
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
        people = People(from_csv=PEOPLE_DATA_FILE, delimiter=";")
        data = pd.read_csv(filepath_or_buffer=PEOPLE_DATA_FILE, delimiter=";")
        result = people.to_dict()
        self.assertEqual(result, self.expected_result)
        self.assertEqual(people.insertion_date.dtype.type, np.datetime64, "Column is not a date")

    def test_from_sql_query(self):
        people = People(from_csv=PEOPLE_DATA_FILE, delimiter=";")
        people.save(if_exists='replace')
        people_from_db = PeopleFromDatabase(from_sql_query='select * from people')
        self.assertEqual(people_from_db.to_dict(), people.to_dict())

    def test_from_sql_query_without_bool_args(self):
        people = People(from_csv=PEOPLE_DATA_FILE, delimiter=";")
        people.save(if_exists='replace')
        people_from_db = PeopleFromDatabaseWithoutBoolArgs(from_sql_query='select * from people')
        self.assertEqual(people_from_db.to_dict(), people.to_dict())

    def test_dataframe_is_valid(self):
        people = People()
        people.name = self.name_list
        people.age = self.age_list
        people.money = self.money_list
        people.insertion_date = self.insertion_date_list
        people.is_staff = self.is_staff_list
        self.assertTrue(people.is_valid())

    def test_dataframe_validate(self):
        people = People()
        people.name = self.name_list
        people.age = self.age_list
        people.money = self.money_list
        people.insertion_date = self.string_insertion_date_list
        people.is_staff = self.is_staff_list
        people.validate()
        people.save()
        self.assertTrue(people.is_valid())

    def test_dataframe_is_not_valid(self):
        people = People()
        people.name = self.name_list
        people.age = self.age_list
        people.money = self.money_list
        people.insertion_date = self.string_insertion_date_list
        people.is_staff = self.is_staff_list
        self.assertFalse(people.is_valid())

    def setUp(self):
        # Old school creation
        self.old_school_df = pd.DataFrame({'name': pd.Series(dtype='O'),
                                           'age': pd.Series(dtype='int'),
                                           'money': pd.Series(dtype='float'),
                                           'insertion_date': pd.Series(dtype='datetime64[ns]'),
                                           'is_staff': pd.Series(dtype='bool')})
        self.old_school_read_csv_df = pd.read_csv(PEOPLE_DATA_FILE, delimiter=';', parse_dates=['insertion_date'])
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
