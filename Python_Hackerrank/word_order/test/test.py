import unittest
from Python_Hackerrank.word_order.core.utils import *

class unitTestCase(unittest.TestCase):
    def testWordOrder(self):
        n = 4
        arr = ["bcdef", "abcdefg", "bcde", "bcdef"]
        actual_output = word_order_find(n, arr)
        expected_output = "2 1 1 3"
        self.assertEqual(expected_output,actual_output)
