from Python_Hackerrank.np_linear_algebra.core.utils import *
import unittest

class unitTestCae(unittest.TestCase):
    def testNpLinearAlgebra(self):
        num = 2
        input1 = [[1.1, 1.1], [1.1, 1.1]]
        actual_output = np_linear_algebra(num, input1)
        expected_output = 0.0
        self.assertEqual(actual_output, expected_output)

    def testNpLinearAlgebra1(self):
        num = 2
        input1 = [[2.1, 1.2], [3.1, 1.5]]
        actual_output = np_linear_algebra(num, input1)
        expected_output = -0.57
        self.assertEqual(actual_output, expected_output)