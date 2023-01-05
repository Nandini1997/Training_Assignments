import unittest
from Python_Hackerrank.python_mutation.core.utils import *

class unitTestCase(unittest.TestCase):
    def testStringMutation(self):
        s = "abracadabra"
        i, c = 5, 'k'
        actual_output = mutate_string(s, i, c)
        expected_output = "abrackdabra"
        self.assertEqual(expected_output, actual_output)

    def testStringMutation1(self):
        s = "nandinisrinivas"
        i, c = 5, 'k'
        actual_output = mutate_string(s, i, c)
        expected_output = "nandikisrinivas"
        self.assertEqual(expected_output, actual_output)
