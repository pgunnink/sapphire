from __future__ import division

import unittest

from math import pi, sqrt

from sapphire import clusters


class SimpleClusterTest(unittest.TestCase):
    def setUp(self):
        self.cluster = clusters.SimpleCluster(size=100)

    def test_station_positions_and_angles(self):
        a = sqrt(100 ** 2 - 50 ** 2)
        expected = [(0, 2 * a / 3, 0, 0), (0, 0, 0, 0),
                    (-50, -a / 3, 0, 2 * pi / 3), (50, -a / 3, 0, -2 * pi / 3)]
        actual = [(station.x[0], station.y[0], station.z[0], station.angle[0])
                  for station in self.cluster.stations]

        for actual_value, expected_value in zip(actual, expected):
            self.assert_tuple_almost_equal(actual_value, expected_value)

    def test_get_detector_coordinates(self):
        for station in self.cluster.stations:
            for detector in station.detectors:
                detector.get_xy_coordinates()

    def assert_tuple_almost_equal(self, actual, expected):
        self.assertIsInstance(actual, tuple)
        self.assertIsInstance(expected, tuple)

        msg = "Tuples differ: %s != %s" % (str(actual), str(expected))
        for actual_value, expected_value in zip(actual, expected):
            self.assertAlmostEqual(actual_value, expected_value, msg=msg)


if __name__ == '__main__':
    unittest.main()
