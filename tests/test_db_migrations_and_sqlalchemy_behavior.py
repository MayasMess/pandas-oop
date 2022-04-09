from pathlib import Path
from unittest import TestCase

from sqlalchemy import Column, Integer, Text
from sqlalchemy.exc import IntegrityError

from src import Base
from src.pandas_oop import models
from src.pandas_oop.fields import StringColumn, IntegerColumn, FloatColumn

ABS_PATH = Path(__file__).resolve().parent.parent
DB_CONNECTION = models.Connection(f'sqlite:///{ABS_PATH}/db/migrations.db')


class Contact(models.Base):
    __tablename__ = 'T_Contacts'

    id = Column(Integer, primary_key=True)
    firstName = Column(Text)
    lastName = Column(Text)


@models.sql(table="people_migrations", con=DB_CONNECTION)
@models.Data
class PeopleMigrations(models.DataFrame):
    name = StringColumn()


@models.sql(table="people_migrations_with_pk", con=DB_CONNECTION)
@models.Data
class PeopleMigrationsWithPrimaryKey(models.DataFrame):
    name = StringColumn(unique=True)
    age = IntegerColumn()


@models.sql(table="people_migrations_with_multiple_pk", con=DB_CONNECTION)
@models.Data
class PeopleMigrationsWithMultiplePrimaryKey(models.DataFrame):
    name = StringColumn(unique=True)
    age = IntegerColumn(unique=True)
    money = FloatColumn()


class TestMigrations(TestCase):

    def test_custom_dataframe_is_detected_as_sqlalchemy_class(self):
        detected_tables = [table.fullname for table in Base.metadata.sorted_tables]
        self.assertIn('people_migrations', detected_tables)

    def test_save_without_pk_no_error(self):
        people = PeopleMigrations()
        people.name = ['John', 'Snow', 'Armin']
        people.save()

    def test_save_with_pk(self):
        people = PeopleMigrationsWithPrimaryKey()
        people.sql_engine.execute('delete from people_migrations_with_pk')
        people.name = ['John', 'Snow', 'Armin']
        people.age = [17, 28, 39]
        people.save()
        self.assertRaises(IntegrityError, people.save)
