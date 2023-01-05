from Python_Hackerrank.finding_the_percentage.core.utils import *
import unittest

class unitTestCase(unittest.TestCase):
    def testFind(self):
        sum = finding_percentage(name="Nandini", student_mark={"Nandini": [23, 4, 55, 65], "Divya": [34, 56, 32]})
        expected_output = sum
        actual_output = 36.75
        self.assertEqual(expected_output, actual_output)

    def testFind2(self):
        sum = finding_percentage(name="Divya", student_mark={"Nandini": [23, 4, 55, 65], "Divya": [34, 56, 32]})
        expected_output = sum
        actual_output = 40.666666666666664
        self.assertEqual(expected_output,actual_output)