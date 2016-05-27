"""tests.py

This module illustrates various testing Redis Key Schemas.
in Chapter 2 of Mastering Redis.

To run from the command-line:

 $ python tests.py

"""
__author__ = "Jeremy Nelson"
__license__ = "GPLv3"

import redis
import unittest

class RedisKeySchemaTest(unittest.TestCase):
    """This class is an example of testing a Redis Key Schema in Python"""

    def setUp(self):
        """Method sets a local Redis instance and other instance variables for
        other test methods""" 
        self.test_db = redis.StrictRedis() # Assumes an empty Redis instance
        if test_db.dbsize() > 0:
            raise ValueError("Redis Test Instance must be empty")

    def tearDown(self):
        self.test_db.flushall()

    
class BookKeySchemaTest(RedisKeySchemaTest):

    def test_delimiter(self):
        """Method tests for a colon in  Redis keys in the datastore."""
        first_key = next(self.test_db.scan("book*"))
        self.assert_(first_key.startswith("book:"))    

if __name__ == '__main__':
    unittest.main() 
