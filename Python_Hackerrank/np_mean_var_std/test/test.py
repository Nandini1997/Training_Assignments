import unittest
from Python_Hackerrank.np_mean_var_std.core.utils import *

class unitTestCase(unittest.TestCase):
    def testMeanVarStd(self):
        arr = [[1, 2]]
        expected_output = mean(arr)
        actual_output = [1.5]
        self.assertEqual(expected_output,actual_output)

    def testMeanVarStd2(self):
        arr = [[1, 2]]
        expected_output = std(arr)
        actual_output = 0.5
        self.assertEqual(expected_output, actual_output)
