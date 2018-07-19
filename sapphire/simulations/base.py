"""Perform simulations of air showers on a cluster of stations

This base class can be subclassed to provide various kinds of
simulations. These simulations will inherit the base functionallity from
this class, including the creation of event and coincidence tables to
store the results, which will look similar to regular HiSPARC data, such
that the same reconstruction analysis can be applied to both.

Example usage::

    >>> import tables

    >>> from sapphire.simulations.base import BaseSimulation
    >>> from sapphire import ScienceParkCluster

    >>> data = tables.open_file('/tmp/test_base_simulation.h5', 'w')
    >>> cluster = ScienceParkCluster()

    >>> sim = BaseSimulation(cluster, data, '/simulations/this_run', 10)
    >>> sim.run()

"""
import random
import warnings

import numpy as np
import tables

from six import iteritems

from .. import storage
from ..analysis.process_events import ProcessEvents
from ..utils import pbar


class BaseSimulation(object):

    """Base class for simulations.

    :param cluster: :class:`~sapphire.clusters.BaseCluster` instance.
    :param data: writeable PyTables file handle.
    :param output_path: path (as string) to the PyTables group (need not
                        exist) in which the result tables will be created.
    :param n: number of simulations to perform.
    :param seed: seed for the pseudo-random number generators.
    :param progress: if True show a progressbar while simulating.

    """

    def __init__(self, cluster, data, output_path='/', n=1, seed=None,
                 progress=True, save_detailed_traces=False, verbose=False):
        self.cluster = cluster
        self.data = data
        self.output_path = output_path
        self.n = n
        self.progress = progress
        self.verbose = verbose
        self.save_detailed_traces = save_detailed_traces
        self.use_preliminary = False
        self._prepare_output_tables()

        if seed is not None:
            random.seed(seed)
            np.random.seed(seed)

    def _prepare_output_tables(self):
        """Prepare output tables in the output data file.

        The groups and tables will be created in the output_path.

        :raises tables.NodeError: If any of the groups (e.g.
            '/coincidences') already exist a exception will be raised.
        :raises tables.FileModeError: If the datafile is not writeable.

        """
        self._prepare_coincidence_tables()
        self._prepare_station_tables()
        self._store_station_index()

    def run(self, skip_large_distance=False):
        """Run the simulations."""

        for (shower_id, shower_parameters) in enumerate(self.generate_shower_parameters()):
            chosen_energy = np.log10( shower_parameters['energy'] )
            chosen_core_pos = shower_parameters['core_pos']
            chosen_radius = np.sqrt( chosen_core_pos[0]**2. + chosen_core_pos[1]**2. )
            if skip_large_distance:
                if (chosen_energy < (13 + 0.1)) and (chosen_energy > (13 - 0.1)) and chosen_radius > 60:
                    continue
                if (chosen_energy < (13.5 + 0.1)) and (chosen_energy > (13.5 - 0.1)) and chosen_radius > 60:
                    continue
                if (chosen_energy < (14 + 0.1)) and (chosen_energy > (14 - 0.1)) and chosen_radius > 80:
                    continue



            '''
                if (chosen_energy < (12.5 + 0.1)) and (chosen_energy > (12.5 - 0.1)) 
                and chosen_radius > 50:
                    continue
            if (chosen_energy < (14.5 + 0.1)) and (chosen_energy > (14.5 - 0.1)) and chosen_radius > 110:
                continue
            if (chosen_energy < (15 + 0.1)) and (chosen_energy > (15 - 0.1)) and chosen_radius > 150:
                continue
            if (chosen_energy < (15.5 + 0.1)) and (chosen_energy > (15.5 - 0.1)) and chosen_radius > 250:
                continue
            if (chosen_energy < (16 + 0.1)) and (chosen_energy > (16 - 0.1)) and chosen_radius > 350:
                continue
            if (chosen_energy < (16.5 + 0.1)) and (chosen_energy > (16.5 - 0.1)) and chosen_radius > 500:
                continue
            if (chosen_energy < (17 + 0.1)) and (chosen_energy > (17 - 0.1)) and chosen_radius > 600:
                continue
            if (chosen_energy < (17.5 + 0.1)) and (chosen_energy > (17.5 - 0.1)) and chosen_radius > 1000:
                continue
            if (chosen_energy < (18 + 0.1)) and (chosen_energy > (18 - 0.1)) and chosen_radius > 1000:
                continue
            '''
            if self.use_preliminary:
                station_events = self.pretrigger_simulate_events_for_shower(shower_parameters)
            else:
                station_events = self.simulate_events_for_shower(shower_parameters)
            # No need to store coincidences of a cluster containing only one station
            if len(self.cluster.stations) > 1:
                self.store_coincidence(shower_id, shower_parameters,
                                       station_events)

    def generate_shower_parameters(self):
        """Generate shower parameters like core position, energy, etc."""
        shower_parameters = {'core_pos': (None, None),
                             'zenith': None,
                             'azimuth': None,
                             'size': None,
                             'energy': None,
                             'ext_timestamp': None}

        for _ in pbar(range(self.n), show=self.progress):
            yield shower_parameters

    def simulate_events_for_shower(self, shower_parameters):
        """Simulate station events for a single shower"""

        station_events = []
        for station_id, station in enumerate(self.cluster.stations):
            has_triggered, station_observables = \
                self.simulate_station_response(station,
                                               shower_parameters)
				
            if has_triggered:
                event_index = \
                    self.store_station_observables(station_id,
                                                   station_observables)
                station_events.append((station_id, event_index))

        return station_events

    def pretrigger_simulate_events_for_shower(self, shower_parameters):
        """This function can be overwritten in order to include a pre-trigger (see
        GEANT implementation)"""
        return self.simulate_events_for_shower(shower_parameters)


    def simulate_station_response(self, station, shower_parameters):
        """Simulate station response to a shower."""
        detector_observables = self.simulate_all_detectors(
            station.detectors, shower_parameters)
        has_triggered = self.simulate_trigger(detector_observables)
        station_observables = \
            self.process_detector_observables(detector_observables)
        station_observables = self.simulate_gps(station_observables,
                                                shower_parameters, station)
        return has_triggered, station_observables

    def simulate_all_detectors(self, detectors, shower_parameters):
        """Simulate response of all detectors in a station.

        :param detectors: list of detectors
        :param shower_parameters: parameters of the shower

        """
        detector_observables = []
        for detector in detectors:
            observables = self.simulate_detector_response(detector,
                                                          shower_parameters)
            
            detector_observables.append(observables)
        return detector_observables

    def simulate_detector_response(self, detector, shower_parameters):
        """Simulate detector response to a shower.

        :param detector: :class:`~sapphire.clusters.Detector` instance
        :param shower_parameters: shower parameters
        :return: dictionary with keys 'n' (number of particles in
            detector) and 't' (time of arrival of first detected particle).

        """
        observables = {'n': 0, 'n_muons': 0, 'n_electrons': 0, 'n_gammas': 0,
                           't': -999, 'integrals': 0.}

        return observables

    def simulate_trigger(self, detector_observables):
        """Simulate a trigger response."""

        return True

    def simulate_gps(self, station_observables, shower_parameters, station):
        """Simulate gps timestamp."""

        gps_timestamp = {'ext_timestamp': 0, 'timestamp': 0, 'nanoseconds': 0}
        station_observables.update(gps_timestamp)

        return station_observables

    def process_detector_observables(self, detector_observables):
        """Process detector observables for a station.

        The list of detector observables is converted into a dictionary
        containing the familiar observables like pulseheights, n1, n2,
        ..., t1, t2, ..., integrals, etc.

        :param detector_observables: list of observables of the detectors
                                     making up a station.
        :return: dictionary containing the familiar station observables
                 like n1, n2, n3, etc.

        """
        station_observables = {'pulseheights': 4 * [-1.],
                               'integrals': 4 * [-1.],
                               'integrals_muon': 4 * [-1.],
                               'integrals_electron': 4 * [-1.],
                               'integrals_gamma': 4 * [-1.],
                               'pulseheights_muon': 4 * [-1.],
                               'pulseheights_electron': 4 * [-1.],
                               'pulseheights_gamma': 4 * [-1.],
                               'traces': np.empty([4,80]),
                               'photontimes': 4* [-1],
                               'coordinates': np.zeros([4,2])}

        for detector_id, observables in enumerate(detector_observables, 1):
            for key, value in iteritems(observables):
                if key in ['n', 'n_muons', 'n_electrons', 'n_gammas', 't']:
                    key = key + str(detector_id)
                    station_observables[key] = value
                elif key in ['pulseheights', 'integrals', 'integrals_muon','integrals_electron',
                             'integrals_gamma', 'pulseheights_muon', 'pulseheights_electron',
                             'pulseheights_gamma','traces', 'photontimes',
                             'coordinates' ]:
                    idx = detector_id - 1
                    station_observables[key][idx] = value
        return station_observables

    def store_station_observables(self, station_id, station_observables):
        """Store station observables.

        :param station_id: the id of the station in self.cluster
        :param station_observables: A dictionary containing the
            variables to be stored for this event.
        :return: The index (row number) of the newly added event.

        """
        events_table = self.station_groups[station_id].events
        row = events_table.row
        row['event_id'] = events_table.nrows
        for key, value in iteritems(station_observables):
            if key in events_table.colnames:
                row[key] = value
            else:
                warnings.warn('Unsupported variable')
        row.append()
        events_table.flush()
        
        return events_table.nrows - 1

    def store_coincidence(self, shower_id, shower_parameters,
                          station_events):
        """Store coincidence.

        Store the information to find events of different stations
        belonging to the same simulated shower in the coincidences
        tables.

        :param shower_id: The shower number for the coincidence id.
        :param shower_parameters: A dictionary with the parameters of
            the simulated shower.
        :param station_events: A list of tuples containing the
            station_id and event_index referring to the events that
            participated in the coincidence.

        """
        row = self.coincidences.row
        row['id'] = shower_id
        row['N'] = len(station_events)
        row['x'], row['y'] = shower_parameters['core_pos']
        row['zenith'] = shower_parameters['zenith']
        row['azimuth'] = shower_parameters['azimuth']
        row['size'] = shower_parameters['size']
        row['energy'] = shower_parameters['energy']

        timestamps = []
        for station_id, event_index in station_events:
            station = self.cluster.stations[station_id]
            row['s%d' % station.number] = True
            station_group = self.station_groups[station_id]
            event = station_group.events[event_index]
            timestamps.append((event['ext_timestamp'], event['timestamp'],
                               event['nanoseconds']))

        try:
            first_timestamp = sorted(timestamps)[0]
        except IndexError:
            first_timestamp = (0, 0, 0)

        row['ext_timestamp'], row['timestamp'], row['nanoseconds'] = \
            first_timestamp
        row.append()
        self.coincidences.flush()

        self.c_index.append(station_events)
        self.c_index.flush()

    def _prepare_coincidence_tables(self):
        """Create coincidence tables

        These are the same as the tables created by
        :class:`~sapphire.analysis.coincidences.CoincidencesESD`.
        This makes it easy to link events detected by multiple stations.

        """
        self.coincidence_group = self.data.create_group(self.output_path,
                                                        'coincidences',
                                                        createparents=True)
        try:
            self.coincidence_group._v_attrs.cluster = self.cluster
        except tables.HDF5ExtError:
            warnings.warn('Unable to store cluster object, to large for HDF.')

        description = storage.Coincidence
        s_columns = {'s%d' % station.number: tables.BoolCol(pos=p)
                     for p, station in enumerate(self.cluster.stations, 12)}
        description.columns.update(s_columns)

        self.coincidences = self.data.create_table(
            self.coincidence_group, 'coincidences', description)

        self.c_index = self.data.create_vlarray(
            self.coincidence_group, 'c_index', tables.UInt32Col(shape=2))

        self.s_index = self.data.create_vlarray(
            self.coincidence_group, 's_index', tables.VLStringAtom())

    def _prepare_station_tables(self):
        """Create the groups and events table to store the observables

        :param id: the station number, used for the group name
        :param station: a :class:`sapphire.clusters.Station` object

        """
        self.cluster_group = self.data.create_group(self.output_path,
                                                    'cluster_simulations',
                                                    createparents=True)
        self.station_groups = []
        for station in self.cluster.stations:
            station_group = self.data.create_group(self.cluster_group,
                                                   'station_%d' %
                                                   station.number)
            description = ProcessEvents.processed_events_description
            # Add to this description some simulation-only parameters
            description["n_muons1"] = tables.Float32Col(shape=(), dflt=-1.0, pos=22)
            description["n_muons2"] = tables.Float32Col(shape=(), dflt=-1.0, pos=23)
            description["n_muons3"] = tables.Float32Col(shape=(), dflt=-1.0, pos=24)
            description["n_muons4"] = tables.Float32Col(shape=(), dflt=-1.0, pos=25)
            description["n_electrons1"] = tables.Float32Col(shape=(), dflt=-1.0, pos=26)
            description["n_electrons2"] = tables.Float32Col(shape=(), dflt=-1.0, pos=27)
            description["n_electrons3"] = tables.Float32Col(shape=(), dflt=-1.0, pos=28)
            description["n_electrons4"] = tables.Float32Col(shape=(), dflt=-1.0, pos=29)
            description["n_gammas1"] = tables.Float32Col(shape=(), dflt=-1.0, pos=30)         
            description["n_gammas2"] = tables.Float32Col(shape=(), dflt=-1.0, pos=31)
            description["n_gammas3"] = tables.Float32Col(shape=(), dflt=-1.0, pos=32)
            description["n_gammas4"] = tables.Float32Col(shape=(), dflt=-1.0, pos=33)
            description["integrals_muon"] = tables.Int32Col(shape=4, dflt=-1.0, pos=34)
            description["integrals_electron"] = tables.Int32Col(shape=4, dflt=-1.0, pos=35)
            description["integrals_gamma"] = tables.Int32Col(shape=4, dflt=-1.0, pos=36)
            description["shower_energy"] = tables.Float32Col(shape=(), dflt=-1.0, pos=37)
            description["zenith"] = tables.Float32Col(shape=(), dflt=-1.0, pos=38)
            description["azimuth"] = tables.Float32Col(shape=(), dflt=-1.0, pos=39)
            description["core_distance"] = tables.Float32Col(shape=(), dflt=-1.0, pos=40)
            description["cr_particle"] = tables.Float32Col(shape=(), dflt=-1.0, pos=41)
            description["pulseheights_muon"] = tables.Int32Col(shape=4, dflt=-1.0, pos=42)
            description["pulseheights_electron"] = tables.Int32Col(shape=4, dflt=-1.0, pos=43)
            description["pulseheights_gamma"] = tables.Int32Col(shape=4, dflt=-1.0, pos=44)
            description["coordinates"] = tables.Float32Col(shape=(4,2), dflt=-1, pos=45)
            description["photontimes_idx"] = tables.Int32Col(shape=4, dflt=-1, pos=46)
            description["seeds"] = tables.Int32Col(shape=2, dflt=-1, pos=47)
            self.data.create_table(station_group, 'events', description,
                                   expectedrows=self.n)
            if self.save_detailed_traces:
                self.data.create_vlarray(station_group, 'photontimes',
                                         tables.Float32Atom(shape=()),
                                         'Arrival times of photons')

            self.station_groups.append(station_group)

    def _store_station_index(self):
        """Stores the references to the station groups for coincidences"""

        for station_group in self.station_groups:
            self.s_index.append(station_group._v_pathname.encode('utf-8'))
        self.s_index.flush()

    def __repr__(self):
        if not self.data.isopen:
            return "<finished %s>" % self.__class__.__name__
        return ('<%s, cluster: %r, data: %r, output_path: %r>' %
                (self.__class__.__name__, self.cluster, self.data.filename,
                 self.output_path))
