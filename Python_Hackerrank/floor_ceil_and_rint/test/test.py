from Python_Hackerrank.floor_ceil_and_rint.core.utils import *
import unittest
from pyspark.sql.functions import array
class unitTestCase(unittest.TestCase):
    def testfloor(self):
        actual_output = find_floor_ceil_rint([1, 2, 3, 4])
        expected_output = [array([ 1.,  2.,  3.,  4.]), array([ 1.,  2.,  3.,  4.]), array([ 1.,  2.,  3.,  4.])]
        self.assertEqual(actual_output, expected_output)