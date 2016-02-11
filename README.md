[![Build Status](https://travis-ci.org/hershaw/mongoschema.svg?branch=master)](https://travis-ci.org/hershaw/mongoschema)

# mongoschema
Super lightweight python MongoDB ORM built on top of pymongo using raw python types

# NOTICE

This is still very young and immature code. I am using it in production for a few simple and low-traffic applications right now
and still find myself fixing some pretty critical bugs every once in a while.

If you're interested in giving it a go but not sure if it's worth the risk, get in touch with me and we'll figure it out.

# Support

Currently only tested with python 2.7. No idea if it works with anything else, but I would say there's a good chance.

# Installation

    pip install git+git://github.com/hershaw/mongoschema.git@master#egg=mongoschema

# Update

    pip install git+git://github.com/hershaw/mongoschema.git@master#egg=mongoschema --upgrade

# Usage

## Step 1: Define the schema

To do this, you need to import the `MongoSchema` and `MongoField` classes:

    from mongoschema import MongoSchema, MongoField as MF

Subclassing `MongoSchema` and adding a few attributes to it is how you define a collection. For example, here is a User
schema with only the required fields:

    conn = pymongo.MongoClient()
    db = conn['myapp']
    
    class User(MongoSchema):
        collection = db.user
        schema = {
            'username': MF(unicode),
        }

Two things to note right away:
- An `id` field is automatically added and maps to `_id` in the collection.
- Note that you are responsible for importing your own pymongo DB connection.

So now that you have a collection defined for a user, let's create one and access the data:

    user = User.create(username=u'Billy')
    print(user.username) # "Billy"

And that's it, the user is created and saved to the database. Now let's use a few more features:

    class User(MongoSchema):
        collection = db.user
        schema = {
            'username': MF(unicode),
            'email': MF(unicode, validate_regexp=re.compile('[^@]+@[^@]+\.[^@]+')),
            'password': MF(unicode),
            'salt': MF(unicode),
        }
        indexes = [
          'username',
          ['email', {'unique': True}],
        ]
        
        @staticmethod
        def register(username, email, password):
          hashed_password = salt_password(password, unicode(uuid.uuid4().hex))
          user = User.create(
            username=username,
            email=email,
            password=hashed_password,
            salt=salt
          )
          return user


Three new things here:
- We have added a new field email that has a validation regexp that it must pass before being saved to the collection.
- We have added an index on the field `username` and a unique index on the `email` field.
- We now have a static method that we can use as a wrapper for the default `create` function that we used directly earlier.

## Indexes

The `indexes` attribute is a list. Each element of the list will be passed on as arguments to pymongo's `ensure_index` function.
Each index can be in two distinct forms:

- A single string. This is the simplest and the pymongo `ensure_index` is called with that string.
- A list. In this form, the first element passed as the first argument to `ensure_index` while the rest are passed as kwargs.

All of the following forms are acceptable entries into the indexes array:

    'email' # simplest
    ['email', {}] # functionally equivilent to the above
    [(('email', -1),), {'sparse': True}], # descending sparse index
    [(('username', -1), ('email', -1)), {}] # compound index

## Static functions on MongoSchema classes

You will never instiantiate anything that subclasses the `MongoSchema` class. You will define functions using the
`staticmethod` and `classmethod` decorators. These classes are for defining structure, actually dealing with data is for
another type of class.

Speaking of...

# Step 2: Define the doc classes

When you defined the `User` class and used it to create a user, notice that the instance that was returned from `create` was
NOT an instance of `User`.

    print(user) # -> "<class 'base.MongoDoc'>"

The `MongoDoc` base class does provide simple functionality like `save`, `update`, and `remove` but nothing else. If you
need to add instance methods to your users, you need to subclass `MongoDoc` and set the result as the `User`'s doc_class.

    from mongoschema import MongoDoc
    
    class UserDoc(MongoDoc):
      
      def pretty_print(self):
        print('%s - %s' % self.email, self.username)
    
    
    class User(MongoDoc):
      collection = db.user
      # Schema and indexes and stuff...
      doc_class = UserDoc
      
    user = Use.create(username='sam', email='blah@blah.com')
    print(type(user)) # "<class '__main__.UserDoc'>"
    print(user.pretty_print()) # "blah@blah.com - sam"
