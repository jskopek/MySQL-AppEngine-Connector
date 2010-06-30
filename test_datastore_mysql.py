# -*- coding: utf-8 -*-
#
# Copyright 2010 Tobias Rodäbel
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Unit tests for the Datastore MySQL stub."""

from google.appengine.api import apiproxy_stub
from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore
from google.appengine.api import datastore_admin
from google.appengine.api import datastore_errors
from google.appengine.api import datastore_types
from google.appengine.api import users
from google.appengine.api.labs import taskqueue
from google.appengine.datastore import datastore_index
from google.appengine.ext import db
from google.appengine.ext.db import polymodel
from google.appengine.runtime import apiproxy_errors

import datetime
import os
import time
import typhoonae.mysql.datastore_mysql_stub
import unittest


class TaskQueueServiceStubMock(apiproxy_stub.APIProxyStub):
    """Task queue service stub for testing purposes."""

    def __init__(self, service_name='taskqueue', root_path=None):
        super(TaskQueueServiceStubMock, self).__init__(service_name)

    def _Dynamic_Add(self, request, response):
        pass

    def _Dynamic_BulkAdd(self, request, response):
        response.add_taskresult()


class DatastoreMySQLTestCaseBase(unittest.TestCase):
    """Base class for testing the TyphoonAE Datastore MySQL API proxy stub."""

    def setUp(self):
        """Sets up test environment and regisers stub."""

        # Set required environment variables
        os.environ['APPLICATION_ID'] = 'test'
        os.environ['AUTH_DOMAIN'] = 'mydomain.local'
        os.environ['USER_EMAIL'] = 'tester@mydomain.local'
        os.environ['USER_IS_ADMIN'] = '1'

        # Register API proxy stub.
        apiproxy_stub_map.apiproxy = (apiproxy_stub_map.APIProxyStubMap())

        database_info = {
            "host": "127.0.0.1",
            "user": "root",
            "passwd": "",
            "db": "testdb"
        }

        datastore = typhoonae.mysql.datastore_mysql_stub.DatastoreMySQLStub(
            'test', database_info)

        try:
            apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', datastore)
        except apiproxy_errors.ApplicationError, e:
            raise RuntimeError('These tests require a running MySQL server '
                               '(%s)' % e)

        self.stub = apiproxy_stub_map.apiproxy.GetStub('datastore_v3')

        apiproxy_stub_map.apiproxy.RegisterStub(
            'taskqueue', TaskQueueServiceStubMock())

    def tearDown(self):
        """Clears all data."""

        self.stub.Clear()


class DatastoreMySQLTestCase(DatastoreMySQLTestCaseBase):
    """Testing the TyphoonAE Datastore MySQL API proxy stub."""

    def testStub(self):
        """Tests whether our stub is registered."""

        self.assertNotEqual(None, self.stub)

    def testPutGetDelete(self):
        """Puts/gets/deletes entities into/from the datastore."""

        class Author(db.Model):
            name = db.StringProperty()

        class Book(db.Model):
            title = db.StringProperty()

        a = Author(name='Mark Twain', key_name='marktwain')
        a.put()

        b = Book(parent=a, title="The Adventures Of Tom Sawyer")
        b.put()

        key = b.key()

        del a, b

        book = datastore.Get(key)
        self.assertEqual(
            "{u'title': u'The Adventures Of Tom Sawyer'}", str(book))

        author = datastore.Get(book.parent())
        self.assertEqual("{u'name': u'Mark Twain'}", str(author))

        del book

        datastore.Delete(key)

        self.assertRaises(
            datastore_errors.EntityNotFoundError, datastore.Get, key)

        del author

        mark_twain = Author.get_by_key_name('marktwain')

        self.assertEqual('Author', mark_twain.kind())
        self.assertEqual('Mark Twain', mark_twain.name)

        mark_twain.delete()

    def testGetPutMultiTypes(self):
        """Sets and Gets models with different entity groups."""

        class Author(db.Model):
            name = db.StringProperty()

        class Book(db.Model):
            title = db.StringProperty()

        a = Author(name='Mark Twain', key_name='marktwain')
        b = Book(title="The Adventures Of Tom Sawyer")
        keys = db.put([a,b])
        self.assertEqual(2, len(keys))

        items = db.get(keys)
        self.assertEqual(2, len(items))
        
        db.delete(keys)

    def testExpando(self):
        """Test the Expando superclass."""

        class Song(db.Expando):
            title = db.StringProperty()
 
        crazy = Song(
            title='Crazy like a diamond',
            author='Lucy Sky',
            publish_date='yesterday',
            rating=5.0)
 
        oboken = Song(
            title='The man from Hoboken',
            author=['Anthony', 'Lou'],
            publish_date=datetime.datetime(1977, 5, 3))

        crazy.last_minute_note=db.Text('Get a train to the station.')

        crazy.put()
        oboken.put()

        self.assertEqual(
            'The man from Hoboken',
            Song.all().filter('author =', 'Anthony').get().title)

        self.assertEqual(
            'The man from Hoboken',
            Song.all().filter('publish_date >', datetime.datetime(1970, 1, 1)).
                get().title)

    def testPolymodel(self):
        """Tests Polymodels."""

        class Contact(polymodel.PolyModel):
            phone_number = db.PhoneNumberProperty()
            address = db.PostalAddressProperty()

        class Person(Contact):
            first_name = db.StringProperty()
            last_name = db.StringProperty()
            mobile_number = db.PhoneNumberProperty()

        class Company(Contact):
            name = db.StringProperty()
            fax_number = db.PhoneNumberProperty()

        p = Person(
            phone_number='1-206-555-9234',
            address='123 First Ave., Seattle, WA, 98101',
            first_name='Alfred',
            last_name='Smith',
            mobile_number='1-206-555-0117')

        c = Company(
            phone_number='1-503-555-9123',
            address='P.O. Box 98765, Salem, OR, 97301',
            name='Data Solutions, LLC',
            fax_number='1-503-555-6622')

        p.put()
        c.put()

        self.assertEqual(
            set([e.phone_number for e in [p, c]]),
            set([e.phone_number for e in list(Contact.all())]))

        self.assertEqual(
            set([p.phone_number]),
            set([e.phone_number for e in list(Person.all())]))

    def testGetEntitiesByNameAndID(self):
        """Tries to retrieve entities by name or numeric id."""

        class Book(db.Model):
            title = db.StringProperty()

        Book(title="The Hitchhiker's Guide to the Galaxy").put()
        book = Book.get_by_id(1)
        self.assertEqual("The Hitchhiker's Guide to the Galaxy", book.title)

        Book(key_name="solong",
             title="So Long, and Thanks for All the Fish").put()
        book = Book.get_by_key_name("solong")
        self.assertEqual("So Long, and Thanks for All the Fish", book.title)

    def testTransaction(self):
        """Executes multiple operations in one transaction."""

        class Author(db.Model):
            name = db.StringProperty()

        class Book(db.Model):
            title = db.StringProperty()

        marktwain = Author(name='Mark Twain', key_name='marktwain').put()

        def tx():
            assert db.get(marktwain).name == "Mark Twain"

            b = Book(parent=marktwain, title="The Adventures Of Tom Sawyer")
            b.put()

            c = Book(
                parent=marktwain, title="The Hitchhiker's Guide to the Galaxy")
            c.put()

            c.delete()

        db.run_in_transaction(tx)

        self.assertEqual(1, Author.all().count())
        self.assertEqual(1, Book.all().count())

        marktwain = Author.get_by_key_name('marktwain')

        def query_tx():
            query = db.Query()
            query.filter('__key__ = ', marktwain.key())
            author = query.get()

        self.assertRaises(
            datastore_errors.BadRequestError,
            db.run_in_transaction, query_tx)

    def testKindlessAncestorQueries(self):
        """Perform kindless queries for entities with a given ancestor."""

        class Author(db.Model):
            name = db.StringProperty()

        class Book(db.Model):
            title = db.StringProperty()

        author = Author(name='Mark Twain', key_name='marktwain').put()

        book = Book(parent=author, title="The Adventures Of Tom Sawyer").put()

        query = db.Query()
        query.ancestor(author)
        query.filter('__key__ = ', book)

        self.assertEqual(book, query.get().key())

        book = query.get()
        book.delete()

        self.assertEqual(0, query.count())

    def testRunQuery(self):
        """Runs some simple queries."""

        class Employee(db.Model):
            first_name = db.StringProperty(required=True)
            last_name = db.StringProperty(required=True)
            manager = db.SelfReferenceProperty()

        manager = Employee(first_name='John', last_name='Dowe')
        manager.put()

        employee = Employee(
            first_name=u'John', last_name='Appleseed', manager=manager.key())
        employee.put()

        # Perform a very simple query.
        query = Employee.all()
        self.assertEqual(set(['John Dowe', 'John Appleseed']),
                         set(['%s %s' % (e.first_name, e.last_name)
                              for e in query.run()]))

        # Rename the manager.
        manager.first_name = 'Clara'
        manager.put()

        # And perform the same query as above.
        query = Employee.all()
        self.assertEqual(set(['Clara Dowe', 'John Appleseed']),
                         set(['%s %s' % (e.first_name, e.last_name)
                              for e in query.run()]))

        # Get only one entity.
        query = Employee.all()
        self.assertEqual(u'Dowe', query.get().last_name)
        self.assertEqual(u'Dowe', query.fetch(1)[0].last_name)

        # Delete our entities.
        employee.delete()
        manager.delete()

        # Our query results should now be empty.
        query = Employee.all()
        self.assertEqual([], list(query.run()))

    def testCount(self):
        """Counts query results."""

        class Balloon(db.Model):
            color = db.StringProperty()

        Balloon(color='Red').put()

        self.assertEqual(1, Balloon.all().count())

        Balloon(color='Blue').put()

        self.assertEqual(2, Balloon.all().count())

    def testQueryWithFilter(self):
        """Tries queries with filters."""

        class SomeKind(db.Model):
            value = db.StringProperty()

        foo = SomeKind(value="foo")
        foo.put()

        bar = SomeKind(value="bar")
        bar.put()

        class Artifact(db.Model):
            description = db.StringProperty(required=True)
            age = db.IntegerProperty()

        vase = Artifact(description="Mycenaean stirrup vase", age=3300)
        vase.put()

        helmet = Artifact(description="Spartan full size helmet", age=2400)
        helmet.put()

        unknown = Artifact(description="Some unknown artifact")
        unknown.put()

        query = Artifact.all().filter('age =', 2400)

        self.assertEqual(
            ['Spartan full size helmet'],
            [artifact.description for artifact in query.run()])

        query = db.GqlQuery("SELECT * FROM Artifact WHERE age = :1", 3300)

        self.assertEqual(
            ['Mycenaean stirrup vase'],
            [artifact.description for artifact in query.run()])

        query = Artifact.all().filter('age IN', [2400, 3300])

        self.assertEqual(
            set(['Spartan full size helmet', 'Mycenaean stirrup vase']),
            set([artifact.description for artifact in query.run()]))

        vase.delete()

        query = Artifact.all().filter('age IN', [2400])

        self.assertEqual(
            ['Spartan full size helmet'],
            [artifact.description for artifact in query.run()])

        helmet.age = 2300
        helmet.put()

        query = Artifact.all().filter('age =', 2300)

        self.assertEqual([2300], [artifact.age for artifact in query.run()])

        query = Artifact.all()

        self.assertEqual(
            set([2300L, None]),
            set([artifact.age for artifact in query.run()]))

    def testQueryForKeysOnly(self):
        """Queries for entity keys instead of full entities."""

        class Asset(db.Model):
            name = db.StringProperty(required=True)
            price = db.FloatProperty(required=True)

        lamp = Asset(name="Bedside Lamp", price=10.45)
        lamp.put()

        towel = Asset(name="Large Towel", price=3.50)
        towel.put()

        query = Asset.all(keys_only=True)

        app_id = os.environ['APPLICATION_ID']
 
        self.assertEqual(
            set([
                datastore_types.Key.from_path(u'Asset', 1, _app=app_id),
                datastore_types.Key.from_path(u'Asset', 2, _app=app_id)]),
            set(query.run()))

    def testQueryWithOrder(self):
        """Tests queries with sorting."""

        class Planet(db.Model):
            name = db.StringProperty()
            moon_count = db.IntegerProperty()
            distance = db.FloatProperty()

        earth = Planet(name="Earth", distance=93.0, moon_count=1)
        earth.put()

        saturn = Planet(name="Saturn", distance=886.7, moon_count=18)
        saturn.put()

        venus = Planet(name="Venus", distance=67.2, moon_count=0)
        venus.put()

        mars = Planet(name="Mars", distance=141.6, moon_count=2)
        mars.put()

        mercury = Planet(name="Mercury", distance=36.0, moon_count=0)
        mercury.put()

        query = (Planet.all()
            .filter('moon_count <', 10)
            .order('moon_count')
            .order('-name')
            .order('distance'))

        self.assertEqual(
            [u'Venus', u'Mercury', u'Earth', u'Mars'],
            [planet.name for planet in query.run()]
        )

        query = Planet.all().filter('distance >', 100.0).order('-distance')

        self.assertEqual(
            ['Saturn', 'Mars'],
            [planet.name for planet in query.run()]
        )

        query = Planet.all().filter('distance <=', 93.0).order('distance')

        self.assertEqual(
            ['Mercury', 'Venus', 'Earth'],
            [planet.name for planet in query.run()]
        )

        query = (Planet.all()
            .filter('distance >', 80.0)
            .filter('distance <', 150.0)
            .order('distance'))

        self.assertEqual(
            ['Earth', 'Mars'],
            [planet.name for planet in query.run()])

        query = Planet.all().filter('distance >=', 93.0).order('distance')
        self.assertEqual(
            [u'Earth', u'Mars', u'Saturn'],
            [planet.name for planet in query.run()])

        query = Planet.all().filter('distance ==', 93.0)
        self.assertEqual(
            [u'Earth'], [planet.name for planet in query.run()])

    def testQueriesWithMultipleFiltersAndOrders(self):
        """Tests queries with multiple filters and orders."""

        class Artist(db.Model):
            name = db.StringProperty()

        class Album(db.Model):
            title = db.StringProperty()

        class Song(db.Model):
            artist = db.ReferenceProperty(Artist)
            album = db.ReferenceProperty(Album)
            duration = db.StringProperty()
            genre = db.CategoryProperty()
            title = db.StringProperty()

        beatles = Artist(name="The Beatles")
        beatles.put()

        abbeyroad = Album(title="Abbey Road")
        abbeyroad.put()

        herecomesthesun = Song(
            artist=beatles.key(),
            album=abbeyroad.key(),
            duration="3:06",
            genre=db.Category("Pop"),
            title="Here Comes The Sun")
        herecomesthesun.put()

        query = (Song.all()
            .filter('artist =', beatles)
            .filter('album =', abbeyroad))

        self.assertEqual(u'Here Comes The Sun', query.get().title)

        cometogether = Song(
            artist=beatles.key(),
            album=abbeyroad.key(),
            duration="4:21",
            genre=db.Category("Pop"),
            title="Come Together")
        cometogether.put()

        something = Song(
            artist=beatles.key(),
            album=abbeyroad.key(),
            duration="3:03",
            genre=db.Category("Pop"),
            title="Something")
        something.put()

        because1 = Song(
            key_name='because',
            artist=beatles.key(),
            album=abbeyroad.key(),
            duration="2:46",
            genre=db.Category("Pop"),
            title="Because")
        because1.put()

        because2= Song(
            artist=beatles.key(),
            album=abbeyroad.key(),
            duration="2:46",
            genre=db.Category("Pop"),
            title="Because")
        because2.put()

        query = (Song.all()
            .filter('artist =', beatles)
            .filter('album =', abbeyroad)
            .order('title'))

        self.assertEqual(
            [u'Because', u'Because', u'Come Together', u'Here Comes The Sun',
             u'Something'],
            [song.title for song in query.run()])

        query = Song.all().filter('title !=', 'Because').order('title')

        self.assertEqual(
            [u'Come Together', u'Here Comes The Sun', u'Something'],
            [song.title for song in query.run()])

        query = Song.all().filter('title >', 'Come').order('title')

        self.assertEqual(
            [u'Come Together', u'Here Comes The Sun', u'Something'],
            [song.title for song in query.run()])

        something.delete()

        query = Song.all().filter('title >', 'Come').order('title')

        self.assertEqual(
            [u'Come Together', u'Here Comes The Sun'],
            [song.title for song in query.run()])

    def testUnicode(self):
        """Tests unicode."""

        class Employee(db.Model):
            first_name = db.StringProperty(required=True)
            last_name = db.StringProperty(required=True)

        employee = Employee(first_name=u'Björn', last_name=u'Müller')
        employee.put()

        query = Employee.all(keys_only=True).filter('first_name =', u'Björn')
        app_id = os.environ['APPLICATION_ID']
        self.assertEqual(
            datastore_types.Key.from_path(u'Employee', 1, _app=app_id),
            query.get())

    def testListProperties(self):
        """Tests list properties."""

        class Numbers(db.Model):
            values = db.ListProperty(int)

        Numbers(values=[0, 1, 2, 3]).put()
        Numbers(values=[4, 5, 6, 7]).put()

        query = Numbers.all().filter('values =', 0)
        self.assertEqual([0, 1, 2, 3], query.get().values)

        query = db.GqlQuery(
            "SELECT * FROM Numbers WHERE values > :1 AND values < :2", 4, 7)
        self.assertEqual([4, 5, 6, 7], query.get().values)

        class Issue(db.Model):
            reviewers = db.ListProperty(db.Email)

        me = db.Email('me@somewhere.net')
        you = db.Email('you@home.net')
        issue = Issue(reviewers=[me, you])
        issue.put()

        query = db.GqlQuery(
            "SELECT * FROM Issue WHERE reviewers = :1",
            db.Email('me@somewhere.net'))

        self.assertEqual(1, query.count())

        query = db.GqlQuery(
            "SELECT * FROM Issue WHERE reviewers = :1",
            'me@somewhere.net')

        self.assertEqual(1, query.count())

        query = db.GqlQuery(
            "SELECT * FROM Issue WHERE reviewers = :1",
            db.Email('foo@bar.net'))

        self.assertEqual(0, query.count())

    def testStringListProperties(self):
        """Tests string list properties."""

        class Pizza(db.Model):
            topping = db.StringListProperty()

        Pizza(topping=["tomatoe", "cheese"]).put()
        Pizza(topping=["tomatoe", "cheese", "salami"]).put()
        Pizza(topping=["tomatoe", "cheese", "prosciutto"]).put()

        query = Pizza.all(keys_only=True).filter('topping =', "salami")
        self.assertEqual(1, query.count())

        query = Pizza.all(keys_only=True).filter('topping =', "cheese")
        self.assertEqual(3, query.count())

        query = Pizza.all().filter('topping IN', ["salami", "prosciutto"])
        self.assertEqual(2, query.count())

        key = datastore_types.Key.from_path('Pizza', 1)
        query = db.GqlQuery("SELECT * FROM Pizza WHERE __key__ IN :1", [key])
        pizza = query.get()
        self.assertEqual(["tomatoe", "cheese"], pizza.topping)

        pizza.delete()

        query = db.GqlQuery("SELECT * FROM Pizza WHERE __key__ IN :1", [key])
        self.assertEqual(0, query.count())

    def testVariousPropertiyTypes(self):
        """Tests various property types."""

        class Note(db.Model):
            timestamp = db.DateTimeProperty(auto_now=True)
            description = db.StringProperty()
            author_email = db.EmailProperty()
            location = db.GeoPtProperty()
            user = db.UserProperty()

        Note(
            description="My first note.",
            author_email="me@inter.net",
            location="52.518,13.408",
            user=users.get_current_user()
        ).put()

        query = db.GqlQuery("SELECT * FROM Note ORDER BY timestamp DESC")
        self.assertEqual(1, query.count())

        query = db.GqlQuery(
            "SELECT * FROM Note WHERE timestamp <= :1", datetime.datetime.now())

        self.assertEqual(1, query.count())

        note = query.get()

        self.assertEqual("My first note.", note.description)

        self.assertEqual(db.Email("me@inter.net"), note.author_email)
        self.assertEqual("me@inter.net", note.author_email)

        self.assertEqual(
            datastore_types.GeoPt(52.518000000000001, 13.407999999999999),
            note.location)
        self.assertEqual("52.518,13.408", note.location)

        del note

        query = Note.all().filter(
            'location =',
            datastore_types.GeoPt(52.518000000000001, 13.407999999999999))
        self.assertEqual(1, query.count())

        query = Note.all().filter('location =', db.GeoPt("52.518,13.408"))
        self.assertEqual(1, query.count())

    def testDerivedProperty(self):
        """Query by derived property."""

        class LowerCaseProperty(db.Property):
            """A convenience class for generating lower-cased fields."""

            def __init__(self, property, *args, **kwargs):
                """Constructor.
 
                Args:
                    property: The property to lower-case.
                """
                super(LowerCaseProperty, self).__init__(*args, **kwargs)
                self.property = property

            def __get__(self, model_instance, model_class):
                return self.property.__get__(
                    model_instance, model_class).lower()
 
            def __set__(self, model_instance, value):
                raise db.DerivedPropertyError(
                    "Cannot assign to a DerivedProperty")

        class TestModel(db.Model):
            contents = db.StringProperty(required=True)
            lowered_contents = LowerCaseProperty(contents)

        TestModel(contents='Foo Bar').put()

        query = db.GqlQuery(
            "SELECT * FROM TestModel WHERE lowered_contents = :1", 'foo bar')

        self.assertEqual('Foo Bar', query.get().contents)

    def testQueriesWithLimit(self):
        """Retrieves a limited number of results."""

        class MyModel(db.Model):
            property = db.StringProperty()

        for i in range(100):
            MyModel(property="Random data.").put()

        self.assertEqual(50, MyModel.all().count(limit=50))

    def testAllocateIds(self):
        """ """

        class EmptyModel(db.Model):
            pass

        for i in xrange(0, 1000):
            key = EmptyModel().put()

        query = db.GqlQuery("SELECT * FROM EmptyModel")
        self.assertEqual(1000, query.count())

        start, end = db.allocate_ids(key, 2000)
        self.assertEqual(start, 2001)
        self.assertEqual(end, 4000)

    def testCursors(self):
        """Tests the cursor API."""

        class Integer(db.Model):
            value = db.IntegerProperty()

        for i in xrange(0, 2000):
            Integer(value=i).put()

        # Set up a simple query.
        query = Integer.all()

        # Fetch some results.
        a = query.fetch(500)
        self.assertEqual(0L, a[0].value)
        self.assertEqual(499L, a[-1].value)

        b = query.fetch(500, offset=500)
        self.assertEqual(500L, b[0].value)
        self.assertEqual(999L, b[-1].value)

        # Perform several queries with a cursor.
        cursor = query.cursor()
        query.with_cursor(cursor)

        c = query.fetch(200)
        self.assertEqual(1000L, c[0].value)
        self.assertEqual(1199L, c[-1].value)

        query.with_cursor(query.cursor())
        d = query.fetch(500)
        self.assertEqual(1200L, d[0].value)
        self.assertEqual(1699L, d[-1].value)

        query.with_cursor(query.cursor())
        self.assertEqual(1700L, query.get().value)

        # Use a query with filters.
        query = Integer.all().filter('value >', 500).filter('value <=', 1000) 
        e = query.fetch(100)
        query.with_cursor(query.cursor())
        e = query.fetch(50)
        self.assertEqual(601, e[0].value)
        self.assertEqual(650, e[-1].value)

    def testGetSchema(self):
        """Infers an app's schema from the entities in the datastore."""

        class Foo(db.Model):
            foobar = db.IntegerProperty(default=42)

        Foo().put()

        entity_pbs = datastore_admin.GetSchema()
        entity = datastore.Entity.FromPb(entity_pbs.pop())
        self.assertEqual('Foo', entity.key().kind())

    def testTransactionalTasks(self):
        """Tests tasks within transactions."""

        def my_transaction():
            taskqueue.add(url='/path/to/my/worker', transactional=True)

        db.run_in_transaction(my_transaction)
