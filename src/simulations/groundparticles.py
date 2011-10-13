""" Simulation classes

    Define simulations as a class, so they can be subclassed.  This way,
    only bits of simulation logic need to be overridden.

"""
import tables
import os.path
import progressbar as pb
import sys
import numpy as np
from math import pi, sin, cos, atan2, sqrt, isinf

import storage


class GroundParticlesSimulation(object):

    """Base simulation class

    This class defines a detector simulation, taking a shower simulation
    as input.  This class can be overridden to, for example, parallelize
    the simulation.
    """

    def __init__(self, cluster, data, grdpcles, output, R, N, force=False):
        """Simulation initialization

        :param cluster: BaseCluster (or derived) instance
        :param data: the HDF5 file
        :param grdpcles: name of the dataset containing the ground particles
        :param output: name of the destination group to store results
        :param R: maximum distance of shower to center of cluster
        :param N: number of simulations to perform
        :param force: if True, ignore initialization errors, due to
            missing ground particles or existing previous simulations.
            Only use this if you really know what you're doing!

        """
        self.cluster = cluster
        self.data = data
        self.R = R
        self.N = N

        try:
            self.grdpcles = data.getNode('/', grdpcles)
        except tables.NoSuchNodeError:
            if force:
                self.grdpcles = None
            else:
                raise RuntimeError("Cancelling simulation; %s not found in "
                                   "tree." % grdpcles)

        head, tail = os.path.split(output)
        try:
            self.output = self.data.createGroup(head, tail,
                                                createparents=True)
        except tables.NodeError:
            if force:
                self.output = self.data.getNode(head, tail)
            else:
                raise RuntimeError("Cancelling simulation; %s already exists?"
                                   % output)

    def generate_positions(self):
        """Generate positions and an orientation uniformly on a circle

        :return: r, phi, alpha

        """
        for i in range(self.N):
            phi, alpha = np.random.uniform(-pi, pi, 2)
            r = np.sqrt(np.random.uniform(0, self.R ** 2))
            yield r, phi, alpha

    def get_station_particles(self, station, X, Y, alpha):
        """Return all particles hitting a station

        :param station: station definition
        :param X, Y: coordinates of station center
        :param alpha: rotation angle of station

        :return: list of detectors containing list of particles
        :rtype: list of lists

        """
        particles = []

        for detector in station.detectors:
            particles.append(self.get_detector_particles(X, Y, detector,
                                                         alpha))
        return particles

    def get_detector_particles(self, X, Y, detector, alpha):
        """Return all particles hitting a single detector

        Given a HDF5 table containing information on all simulated particles
        and coordinates and orientation of a detector, search for all
        particles which have hit the detector.

        :param X, Y: X, Y coordinates of center of station
        :param alpha: rotation angle of entire station

        :return: list of particles which have hit the detector

        """
        c = detector.get_corners(X, Y, alpha)

        # determine equations describing detector edges
        b11, line1, b12 = self.get_line_boundary_eqs(c[0], c[1], c[2])
        b21, line2, b22 = self.get_line_boundary_eqs(c[1], c[2], c[3])

        # particles satisfying all equations are inside the detector
        return self.grdpcles.readWhere("(b11 < %s) & (%s < b12) & "
                                       "(b21 < %s) & (%s < b22)" % \
                                       (line1, line1, line2, line2))

    def get_line_boundary_eqs(self, p0, p1, p2):
        """Get line equations using three points

        Given three points, this function computes the equations for two
        parallel lines going through these points.  The first and second point
        are on the same line, whereas the third point is taken to be on a
        line which runs parallel to the first.  The return value is an
        equation and two boundaries which can be used to test if a point is
        between the two lines.

        :param p0, p1: (x, y) tuples on the same line
        :param p2: (x, y) tuple on the parallel line

        :return: value1, equation, value2, such that points satisfying value1
            < equation < value2 are between the parallel lines

        Example::

            >>> get_line_boundary_eqs((0, 0), (1, 1), (0, 2))
            (0.0, 'y - 1.000000 * x', 2.0)

        """
        (x0, y0), (x1, y1), (x2, y2) = p0, p1, p2

        # Compute the general equation for the lines
        if not (x0 == x1):
            # First, compute the slope
            a = (y1 - y0) / (x1 - x0)

            # Calculate the y-intercepts of both lines
            b1 = y0 - a * x0
            b2 = y2 - a * x2

            line = "y - %f * x" % a
        else:
            # line is exactly vertical
            line = "x"
            b1, b2 = x0, x2

        # And order the y-intercepts
        if b1 > b2:
            b1, b2 = b2, b1

        return b1, line, b2

    def run(self, positions=None):
        """Run a simulation

        This is the code which performs the simulation.  It creates a list
        of positions, creates all necessary tables and performs the
        simulation.

        :param positions: if given, use these coordinates instead of
            generating new ones

        """
        self._run_welcome_msg()

        if not positions:
            positions = self.generate_positions()

        self.headers = self.data.createTable(self.output, '_headers',
                                             storage.SimulationHeader)
        self.particles = self.data.createTable(self.output, '_particles',
                                               storage.ParticleEvent)

        progress = pb.ProgressBar(maxval=self.N, widgets=[pb.Percentage(),
                                                          pb.Bar(),
                                                          pb.ETA()],
                                  fd=sys.stderr)
        for event_id, (r, phi, alpha) in progress(enumerate(positions)):
            self.simulate_event(event_id, r, phi, alpha)

        self.headers.flush()
        self.particles.flush()

        self.store_observables()

        self._run_exit_msg()

    def _run_welcome_msg(self):
        """Print a welcome message at start of simulation run"""

        print 74 * '-'
        print """Running simulation

Ground particles:   %s
Output destination: %s

Maximum core distance of cluster center:   %f m
Number of cluster positions in simulation: %d
        """ % (self.grdpcles._v_pathname, self.output._v_pathname, self.R,
               self.N)

    def _run_exit_msg(self):
        print 74 * '-'
        print

    def simulate_event(self, event_id, r, phi, alpha):
        """Simulate a single event

        :param event_id: event id
        :param r, phi: polar coordinates of cluster center
        :param alpha: rotation of cluster

        """
        self.write_header(event_id, 0, r, phi, alpha)
        for station_id, station in enumerate(self.cluster.stations, 1):
            x, y, beta = station.get_coordinates(r, phi, alpha)
            # calculate station r, phi just to save it in header
            s_r = sqrt(x ** 2 + y ** 2)
            s_phi = atan2(y, x)
            self.write_header(event_id, station_id, s_r, s_phi, beta)

            plist = self.get_station_particles(station, x, y, beta)
            self.write_detector_particles(event_id, station_id, plist)

    def write_header(self, event_id, station_id, r, phi, alpha):
        """Write simulation event header information to file

        :param event_id: simulation event id
        :param station_id: station id inside cluster, 0 for cluster header
        :param r, phi: r, phi for cluster or station position, both as
            absolute coordinates
        :param alpha: cluster rotation angle or station rotation angle

        """
        row = self.headers.row
        row['id'] = event_id
        row['station_id'] = station_id
        row['r'] = r
        row['phi'] = phi
        row['alpha'] = alpha
        row.append()

    def write_detector_particles(self, event_id, station_id, plist):
        """Write particles to file

        :param event_id: simulation event id
        :param station_id: station id inside cluster
        :param plist: list of detectors, containing list of particles

        """
        row = self.particles.row
        for detector_id, detector in enumerate(plist):
            for particle in detector:
                row['id'] = event_id
                row['station_id'] = station_id
                row['detector_id'] = detector_id
                row['pid'] = particle['pid']
                row['r'] = particle['core_distance']
                row['phi'] = particle['polar_angle']
                row['time'] = particle['arrival_time']
                row['energy'] = particle['energy']
                row.append()

    def store_observables(self):
        """Analyze simulation results and store derived data

        Loop through simulation result tables and find observables like the
        number of particles which hit detectors, as well as the arrival time
        of the first particle to hit a detector.  The number of detectors
        which have particles are also recorded.  Finally the per shower
        results from all stations are combined and stored as a coincidence
        event.

        To speed things up, pointers into the two simulation result tables are
        advanced row by row.  Event and station id's are continually checked
        to now when to break.  The flow is a bit complicated, but it is fast.

        """
        obs = self.data.createTable(self.output, 'observables',
                                    storage.ObservableEvent)
        coinc = self.data.createTable(self.output, 'coincidences',
                                      storage.CoincidenceEvent)
        c_index = self.data.createVLArray(self.output, 'c_index',
                                          tables.UInt32Atom())


        print "Storing observables from %s" % self.output._v_pathname

        obs_row = obs.row
        # index into the observables table
        obs_index = 0
        coinc_row = coinc.row
        progress = pb.ProgressBar(maxval=len(self.headers),
                                  widgets=[pb.Percentage(), pb.Bar(),
                                           pb.ETA()],
                                  fd=sys.stderr).start()
        headers = iter(self.headers)
        particles = iter(self.particles)

        # start with first rows initialized
        header = headers.next()
        particle = particles.next()
        # loop over events
        while True:
            assert header['station_id'] == 0
            # freeze header row for later use
            event = header.fetch_all_fields()
            c_list = []

            # N = number of stations which trigger
            N = 0
            try:
                # loop over headers
                while True:
                    header = headers.next()
                    # if station_id == 0, we have a new event, not a station
                    if header['station_id'] == 0:
                        break
                    else:
                        t = [[], [], [], []]
                        # loop over particles
                        while True:
                            # check if particle matches current event/station
                            if particle['id'] != event['id'] or \
                                particle['station_id'] != \
                                header['station_id']:
                                break
                            else:
                                t[particle['detector_id']].append(particle['time'])
                                try:
                                    particle = particles.next()
                                except StopIteration:
                                    # Ran out of particles. Forcing invalid
                                    # id, so that higher-level loops can
                                    # finish business, but no new particles
                                    # will be processed
                                    particle['id'] = -1
                        self.write_observables(obs_row, header, t)
                        # trigger if Ndet hit >= 2
                        if sum([1 if u else 0 for u in t]) >= 2:
                            N += 1
                            # only add triggered stations to c_list, just
                            # like real data
                            c_list.append(obs_index)
                        # point index to next (empty) slot
                        obs_index += 1
            # StopIteration when we run out of headers
            except StopIteration:
                break
            finally:
                self.write_coincidence(coinc_row, event, N)
                c_index.append(c_list)
                progress.update(header.nrow + 1)
        progress.finish()

        obs.flush()
        coinc.flush()
        print

    def write_observables(self, row, station, t):
        r = station['r']
        phi = station['phi']
        row['id'] = station['id']
        row['station_id'] = station['station_id']
        row['r'] = r
        row['phi'] = phi
        row['x'] = r * cos(phi)
        row['y'] = r * sin(phi)
        row['alpha'] = station['alpha']
        row['N'] = sum([1 if u else 0 for u in t])
        row['t1'], row['t2'], row['t3'], row['t4'] = \
            [min(u) if len(u) else -999 for u in t]
        row['n1'], row['n2'], row['n3'], row['n4'] = \
            [len(u) for u in t]
        row.append()

    def write_coincidence(self, row, event, N):
        r = event['r']
        phi = event['phi']
        row['id'] = event['id']
        row['N'] = N
        row['r'] = r
        row['phi'] = phi
        row['x'] = r * cos(phi)
        row['y'] = r * sin(phi)
        row['alpha'] = event['alpha']
        row.append()