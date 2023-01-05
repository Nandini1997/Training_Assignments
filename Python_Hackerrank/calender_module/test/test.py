import unittest
from pyspark.sql import SparkSession
from Python_Hackerrank.calender_module.core.core import *

class unitTestClass(unittest.TestCase):

    def testfindfay2(self):
        input2 = "02 05 2023"
        actual_output = findDay(input2)
        self.assertEqual(actual_output, "SUNDAY")

    def testfindfay1(self):
        input2 = "01 05 2023"
        actual_output = findDay(input2)
        self.assertEqual(actual_output, "THURSDAY")