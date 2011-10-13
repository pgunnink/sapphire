from __future__ import division

from math import sqrt, pi
import unittest

import clusters


class SimpleClusterTest(unittest.TestCase):
    def setUp(self):
        self.cluster = clusters.SimpleCluster(size=100)

    def test_station_positions_and_angles(self):
        a = sqrt(100 ** 2 - 50 ** 2)
        expected = [(0, 2 * a / 3, 0), (0, 0, 0), (-50, -a / 3, 2 * pi / 3),
                    (50, -a / 3, -2 * pi / 3)]
        actual = [(station.position[0], station.position[1], station.angle)
                  for station in self.cluster.stations]

        for actual_value, expected_value in zip(actual, expected):
            self.assertTupleAlmostEqual(actual_value, expected_value)

    @unittest.expectedFailure
    def test_get_detector_corners(self):
        #FIXME: the thing is, this is a mess.  Think about this.  Hard.
        self.cluster.stations[1].detectors[1].get_detector_coordinates

    def assertTupleAlmostEqual(self, actual, expected):
        self.assertTrue(type(actual) == type(expected) == tuple)

        msg = "Tuples differ: %s != %s" % (str(actual), str(expected))
        for actual_value, expected_value in zip(actual, expected):
            self.assertAlmostEqual(actual_value, expected_value, msg=msg)