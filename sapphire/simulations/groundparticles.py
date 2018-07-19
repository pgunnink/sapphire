"""Perform simulations of CORSIKA air showers on a cluster of stations

This simulation uses a HDF5 file created from a CORSIKA simulation with
the ``store_corsika_data`` script. The shower is 'thrown' on the cluster
with random core positions and azimuth angles.

Example usage::

    >>> import tables
    >>> from sapphire import GroundParticlesSimulation, ScienceParkCluster
    >>> data = tables.open_file('/tmp/test_groundparticle_simulation.h5', 'w')
    >>> cluster = ScienceParkCluster()
    >>> sim = GroundParticlesSimulation('corsika.h5', 500, cluster, data,
    ...                                 '/', 10)
    >>> sim.run()

"""
from __future__ import print_function

from math import cos, log10, pi, sin, sqrt, tan
from time import time
import subprocess
import shutil
import os
from six import iteritems

import warnings
import numpy as np
import tables

from ..corsika.corsika_queries import CorsikaQuery
from ..corsika.particles import particle_id

from ..utils import c, closest_in_list, norm_angle, pbar, vector_length
from .detector import ErrorlessSimulation, HiSPARCSimulation
from .gammas import simulate_detector_mips_gammas


#import matplotlib
#matplotlib.use('agg')
#import matplotlib.pyplot as plt

TRACE_LENGTH = 80
MAX_VOLTAGE = 4096*0.57/1e3

class GroundParticlesGEANT4Simulation(ErrorlessSimulation):

    def __init__(self, corsikafile_path, max_core_distance,
                 trigger_function=None, cutoff_number_of_particles=10, *args, **kwargs):
        """Simulation initialization

        :param corsikafile_path: path to the corsika.h5 file containing
                                 the groundparticles.
        :param max_core_distance: maximum distance of shower core to
                                  center of cluster.

        """
        super(GroundParticlesGEANT4Simulation, self).__init__(*args, **kwargs)
        self.cutoff_number_of_particles = cutoff_number_of_particles
        self.corsikafile = tables.open_file(corsikafile_path, 'r')
        self.groundparticles = self.corsikafile.get_node('/groundparticles')
        self.max_core_distance = max_core_distance
        if trigger_function is not None:
            self.trigger_function = trigger_function
            self.use_preliminary = True


    def __del__(self):
        self.finish()

    def finish(self):
        """Clean-up after simulation"""

        self.corsikafile.close()

    def generate_shower_parameters(self):
        """Generate shower parameters like core position, energy, etc.

        For this groundparticles simulation, only the shower core position
        and rotation angle of the shower are generated.  Do *not*
        interpret these parameters as the position of the cluster, or the
        rotation of the cluster!  Interpret them as *shower* parameters.

        :return: dictionary with shower parameters: core_pos
                 (x, y-tuple) and azimuth.

        """
        r_max = self.max_core_distance
        now = int(time())

        event_header = self.corsikafile.get_node_attr('/', 'event_header')
        event_end = self.corsikafile.get_node_attr('/', 'event_end')
        corsika_parameters = {'zenith': event_header.zenith,
                              'size': event_end.n_electrons_levels,
                              'energy': event_header.energy,
                              'particle': event_header.particle}
        self.corsika_azimuth = event_header.azimuth

        self.corsika_zenith = corsika_parameters['zenith']
        self.corsika_energy = corsika_parameters['energy']
        self.cr_particle = particle_id(corsika_parameters['particle'])

        for i in pbar(range(self.n), show=self.progress):
            ext_timestamp = (now + i) * int(1e9)
            x, y = self.generate_core_position(r_max)
            self.core_distance = np.sqrt(x**2 + y**2)
            self.shower_azimuth = self.generate_azimuth()

            shower_parameters = {'ext_timestamp': ext_timestamp,
                                 'core_pos': (x, y),
                                 'azimuth': self.shower_azimuth}

            # Subtract CORSIKA shower azimuth from desired shower azimuth
            # make it fit in (-pi, pi] to get rotation angle of the cluster.
            alpha = self.shower_azimuth - self.corsika_azimuth
            alpha = norm_angle(alpha)
            self._prepare_cluster_for_shower(x, y, alpha)

            shower_parameters.update(corsika_parameters)
            yield shower_parameters

    def _prepare_cluster_for_shower(self, x, y, alpha):
        """Prepare the cluster object for the simulation of a shower.

        Rotate and translate the cluster so that (0, 0) coincides with the
        shower core position and that the angle between the rotated cluster
        and the CORSIKA shower is the desired azimuth.

        :param x,y: position of shower core relative to cluster origin in m.
        :param alpha: angle the cluster needs to be rotated in radians.

        """
        # rotate the core position around the original cluster center
        xp = x * cos(-alpha) - y * sin(-alpha)
        yp = x * sin(-alpha) + y * cos(-alpha)

        self.cluster.set_coordinates(-xp, -yp, 0, -alpha)

    def _simulateCathode(self, N_photon):
        """Simulate Cathode
        
        :param N_photon: number photons
        :return: number of emitted cathode electrons
        
        """
        N_electron = 0
        for i in range(0,int(N_photon)):
            if np.random.random() < .25:
                N_electron += 1
        return N_electron

    def _simulateTrace(self, N, start, t_rise=7.0, t_fall=25.0, stop=300.0, Gmean=17.0e6, R = 50.0):
        """Simulate event trace
        
        :param N: Number of emitted cathode electrons
        :param start: Photon arrivel time in ns
        :param t_rise: Risetime of the pulse
        :param t_fall: Falltime of the pulse
        :param stop: End of trace
        :param GR: Gain times resistance of the PMT
        
        :return: trace with binned simulated data according to Leo ISBN 978-3-642-57920-2 page 190
            
        """
        if N == 0:
            trace = np.zeros(80)
            return trace
        trace = []
        e = 1.6e-19 # charge electron
        sigma = Gmean / 10
        sigma = sigma / np.sqrt(N)
        G = np.random.normal(Gmean, sigma) # Gains are normally distributed with sigma 10%
        for i in np.arange(0,start,2.5):
            trace.append(0)
        for i in np.arange(start,stop,2.5):
            t = np.float(i - start)
            constant = (G * R * N * e)/((t_fall - t_rise)*1e-9) # time in s instead of ns
            trace.append( constant * ( np.exp(-t/t_rise) - np.exp(-t/t_fall) ) )

        trace = np.array(trace)

        return np.array(trace)

    def _simulate_PMT(self, photontimes):
        """Simulate an entire PMT from cathode to response of PMT type
            
        :param photontimes: an array with the arrival times of photons at the pmt
        
        :return: np array with the trace in V
        
        """
        
        # First check if the photontimes list is empty
        if len(photontimes) == 0:
            return np.array([0])
        
        # Determine how many particles arrived per 2.5 nanosecond

        n_phot, bin_edges = np.histogram(photontimes,bins=np.linspace(0,200,81))
        t_arr = (0.5*(bin_edges[1:] + bin_edges[:-1]))-1.25
        
        # Simulate the ideal response per nanosecond and combine all single ns responses
        n_elec0 = self._simulateCathode(n_phot[0])
        trace = self._simulateTrace(n_elec0, t_arr[0], stop=t_arr[-1]+2.5)
        for nphot, tarr in zip(n_phot[1:],t_arr[1:]):
            n_elec = self._simulateCathode(nphot)
            trace += self._simulateTrace(n_elec, tarr, stop=t_arr[-1]+2.5)

        trace = np.array(trace)

        return trace

    def pretrigger_simulate_events_for_shower(self, shower_parameters):
        """Simulate station events for a single shower"""
        station_events = []
        particles_station = []
        for station_id, station in enumerate(self.cluster.stations):
            detectors_particles = self.preliminary_detectors_response(
                station.detectors, shower_parameters)
            particles_station.append(detectors_particles)

        if self.trigger_function(particles_station):
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


    def preliminary_detectors_response(self, detectors, shower_parameters):
        detector_particles = []
        n_detected = []
        for detector in detectors:
            particles = self.get_particles_in_detector(detector, shower_parameters)
            detector_particles.append(particles)
        return detector_particles



    def simulate_detector_response(self, detector, shower_parameters):
        """Simulate detector response to a shower.

        Checks if leptons have passed a detector. If so, it returns the number
        of leptons in the detector and the arrival time of the first lepton
        passing the detector.

        :param detector: :class:`~sapphire.clusters.Detector` for which
                         the observables will be determined.
        :param shower_parameters: dictionary with the shower parameters.

        """

        particles = self.get_particles_in_detector(detector, shower_parameters)
        n_detected = len(particles)

        if n_detected:
            n_muons, n_electrons, n_gammas, firstarrival, pulseintegral, \
            pulseintegral_muon, pulseintegral_electron, pulseintegral_gamma, \
            pulseheights, pulseheights_muon, pulseheights_electron, \
            pulseheights_gamma, traces, photon_arrival_times, location_in_plaat = \
            self.simulate_detector_mips_for_particles(particles, detector, 
                                                      shower_parameters)
            particles['t'] += firstarrival
            nz = cos(shower_parameters['zenith'])
            tproj = detector.get_coordinates()[-1] / (c * nz)
            first_signal = particles['t'].min() + detector.offset - tproj
            # If the signal is below 30 mV n_detected exists but should not
            if n_muons + n_electrons + n_gammas == 0:
                observables = {'n': 0, 'n_muons': 0, 'n_electrons': 0, 'n_gammas': 0,
                           't': -999, 'integrals': 0., 'integrals_muon': 0.,
                           'integrals_electron': 0., 'integrals_gamma': 0.,
                           'pulseheights': 0., 'pulseheights_muon': 0.,
                           'pulseheights_electron': 0., 'pulseheights_gamma': 0.,
                           'traces': 0., 'photontimes': [0], 'coordinates': 0}
            else:
                observables = {'n': n_muons + n_electrons + n_gammas,
                               'n_muons': n_muons,
                               'n_electrons': n_electrons,
                               'n_gammas': n_gammas,
                               't': self.simulate_adc_sampling(first_signal),
                               'integrals': pulseintegral,
                               'integrals_muon': pulseintegral_muon,
                               'integrals_electron': pulseintegral_electron,
                               'integrals_gamma': pulseintegral_gamma,
                               'pulseheights': pulseheights,
                               'pulseheights_muon': pulseheights_muon,
                               'pulseheights_electron': pulseheights_electron,
                               'pulseheights_gamma': pulseheights_gamma,
                               'traces': traces,
                               'photontimes': photon_arrival_times,
                               'coordinates': location_in_plaat,
                               'seeds': self.seeds}
        else:
            observables = {'n': 0, 'n_muons': 0, 'n_electrons': 0, 'n_gammas': 0,
                           't': -999, 'integrals': 0., 'integrals_muon': 0.,
                           'integrals_electron': 0., 'integrals_gamma': 0.,
                           'pulseheights': 0., 'pulseheights_muon': 0.,
                           'pulseheights_electron': 0., 'pulseheights_gamma': 0.,
                           'traces': 0., 'photontimes': [0], 'coordinates': 0}

        return observables

    def simulate_detector_mips_for_particles(self, particles, detector,
                                             shower_parameters):
        """Simulate the detector signal for particles

        :param particles: particle rows with the p_[x, y, z]
                          components of the particle momenta.

        """
        # Determine the arrival time of the first particles measured since the
        # start of the shower (first interaction)
        times_since_first_interaction = []
        for particle in particles:
            t = particle["t"]
            times_since_first_interaction.append(t)
        t_first_interaction = min(times_since_first_interaction)
        #print("--")

        # Run the geant4 simulation for each particle
        arrived_photons_per_particle = []
        arrived_photons_per_particle_muon = []
        arrived_photons_per_particle_electron = []
        arrived_photons_per_particle_gamma = []
        arrivaltimes = []
        n_muons = 0
        n_electrons = 0
        n_gammas = 0


        particle_types = ['', 'gamma', 'e+', 'e-', '', 'mu+', 'mu-']

        # if cutoff is defined, not all particles are calculated
        if self.cutoff_number_of_particles is not None:
            idx = []
            idx_rest = []
            i = 0
            for particle in particles:
                if particle['particle_id'] in [2,3,5,6]:
                    idx.append(i)
                else:
                    idx_rest.append(i)
                i += 1
            number_of_electrons = len(idx)
            if number_of_electrons<self.cutoff_number_of_particles:
                idx.extend(idx_rest[:self.cutoff_number_of_particles-number_of_electrons])

            idx = np.random.permutation(idx) # shuffle the electrons
            idx = idx[:self.cutoff_number_of_particles]


            particles = particles[idx,]
        local_cor = [np.nan, np.nan]
        earliest_particle = np.inf
        #plt.figure()

        for i, particle in enumerate(particles):
            # Determine which particle hit the detector
            particle_id = particle["particle_id"]
            particletype = particle_types[particle_id]

            # Determine the position the particle hit the detector in the
            # corsika detector reference system (-25 < x < 25 and -50 < y < 50)
            # taking projection due to detector-height differences into
            # account.
            x = particle["x"]
            y = particle["y"]
            p = np.array([x, y])
            
            detx, dety, detz = detector.get_coordinates()
            detcorners = detector.get_corners()
            
            # Obtain corners
            c1 = np.array(detcorners[0])
            c2 = np.array(detcorners[1])
            c3 = np.array(detcorners[2])
            c4 = np.array(detcorners[3])
            
            # Rotate corners to convenient system where the axes of the detector align with x and y
            # I don't know what all the entries in the orientation list are but the last one works.
            theta = detector.orientation[-1] + (self.shower_azimuth - self.corsika_azimuth)
            THETA = np.array([[np.cos(theta),-1*np.sin(theta)], [np.sin(theta),np.cos(theta)]])
            
            c1_new = np.inner(THETA, (c1 - c1)) + c1
            c2_new = np.inner(THETA, (c2 - c1)) + c1
            c3_new = np.inner(THETA, (c3 - c1)) + c1
            c4_new = np.inner(THETA, (c4 - c1)) + c1
            
            # Increase the size of the detector to also include perspex hits and near misses 
            # (which could still be a hit because the skibox lid is a bit higher than the scintillator)
            c1_new = np.array([c1_new[0] - 0.1, c1_new[1] - 0.1])
            c2_new = np.array([c2_new[0] + 0.1, c2_new[1] - 0.1])
            c3_new = np.array([c3_new[0] + 0.1, c3_new[1] + 0.675 + 0.1])
            c4_new = np.array([c4_new[0] - 0.1, c4_new[1] + 0.675 + 0.1])
            
            # Rotate the system back
            theta = -1.0 * theta
            THETA_BACK = np.array([[np.cos(theta),-1*np.sin(theta)], [np.sin(theta),np.cos(theta)]])
            
            c1_new = np.inner(THETA_BACK, (c1_new - c1)) + c1
            c2_new = np.inner(THETA_BACK, (c2_new - c1)) + c1
            c3_new = np.inner(THETA_BACK, (c3_new - c1)) + c1
            c4_new = np.inner(THETA_BACK, (c4_new - c1)) + c1
            
            # Slightly bigger detcorners now
            detcorners = [c1_new, c2_new, c3_new, c4_new]

            zenith = shower_parameters['zenith']
            azimuth = self.corsika_azimuth

            znxnz = detz * tan(zenith) * cos(azimuth)
            znynz = detz * tan(zenith) * sin(azimuth)
            
            detcproj = [(cx - znxnz, cy - znynz) for cx, cy in detcorners]
            cproj1 = np.array([detcproj[0][0],detcproj[0][1]])
            cproj2 = np.array([detcproj[1][0],detcproj[1][1]])
            cproj3 = np.array([detcproj[2][0],detcproj[2][1]])
            cproj4 = np.array([detcproj[3][0],detcproj[3][1]])
            
            # Here I determine the distance from a point to a line
            xdistance = (np.linalg.norm(np.cross(cproj2 - cproj1, cproj1 - p)) /
                         np.linalg.norm(cproj2 - cproj1))
            ydistance = (np.linalg.norm(np.cross(cproj4 - cproj1, cproj1 - p)) /
                         np.linalg.norm(cproj4 - cproj1))
           
            # Convert to cm 
            xdetcoord = 100 * xdistance - 50 - 10
            ydetcoord = 100 * ydistance - 25 - 10
            # Determine at which angle the particle hit the detector
            px = particle["p_x"]
            py = particle["p_y"]
            pz = particle["p_z"]

            # Determine the energy of the incoming particle
            particleenergy = np.sqrt(px ** 2 + py ** 2 + pz ** 2)
            
            # Start the GEANT4 simulation using the position, direction and
            # energy of the incoming particle. This simulation creates a
            # new directory RUN_1 with a csv file containing the number of
            # photons that arrived at the PMT.
            try: # sometimes the program crashes with exit status 11
                output = subprocess.check_output(["/user/kaspervd/Documents/repositories/diamond/20170117_geant4_simulation/HiSPARC-stbc-build/./skibox", "1", particletype,
                                                  "{}".format(particleenergy),
                                                  "{}".format(xdetcoord),
                                                  "{}".format(ydetcoord),
                                                  "-99889",#"-99893.695",
                                                  "{}".format(px),
                                                  "{}".format(py),
                                                  "{}".format(pz)])
                if self.verbose:
                    print( "./skibox", "1", particletype,
                                                      "{}".format(particleenergy),
                                                      "{}".format(xdetcoord),
                                                      "{}".format(ydetcoord),
                                                      "-99889",
                                                      "{}".format(px),
                                                      "{}".format(py),
                                                      "{}".format(pz) )
    
                # Determine the number of photons that have arrived at the PMT
                # and the time it took for the first photon to arrive at the PMT.
                geantfile = np.genfromtxt("RUN_1/outpSD.csv", delimiter=",")
                try:
                    photontimes = geantfile[1:,0]
                    arrivaltime = min(photontimes)
    
                    # Not all particles arrive at the same time, so a trace gets
                    # wider if there is some time between the creation of scintil.
                    # photons. In order to achieve this add the arrival time of the
                    # particle with respect to the first arrived particle to the
                    # arrival times of the scint. photons created by this particle.
                    # If there is only one particle this latency is zero.
                    t_later_than_first = particle["t"] - t_first_interaction
                    #print("Later than first (in ns): ",t_later_than_first)
                    photontimes += t_later_than_first

                    # Succesful interaction, keep statistics
                    if particle_id == 1:
                        n_gammas += 1
                        arrived_photons_per_particle_gamma = np.append(arrived_photons_per_particle_gamma,photontimes)
                    elif particle_id in [2, 3]:
                        n_electrons += 1
                        if t_later_than_first<earliest_particle:
                            local_cor = [xdetcoord, ydetcoord]
                            earliest_particle = t_later_than_first
                        arrived_photons_per_particle_electron = np.append(arrived_photons_per_particle_electron,photontimes)
                    elif particle_id in [5, 6]:
                        n_muons += 1
                        if t_later_than_first<earliest_particle:
                            local_cor = [xdetcoord, ydetcoord]
                            earliest_particle = t_later_than_first
                        arrived_photons_per_particle_muon = np.append(arrived_photons_per_particle_muon,photontimes)




                except:
                    # No photons have arrived (a gamma that didn't undergo any
                    # iteraction).
                    photontimes = np.array([]) # empty list
                    arrivaltime = -999

                # Remove the directory created by the GEANT4 simulation
                shutil.rmtree("RUN_1")
            except: # If the program crashed with exit status 11
                photontimes = np.array([])
                arrivaltime = -999



            # If multiple particles hit the detector, they are treated
            # seperately. Make lists in order to be able to add all
            # arrived photons.

            #label_particle = '{}'.format(particletype) + ' t={0:.2f}'.format(
            #    t_later_than_first)
            #plt.plot(self._simulate_PMT(photontimes), label=label_particle)

            arrived_photons_per_particle = np.append(arrived_photons_per_particle,
                                                     photontimes)
            arrivaltimes.append(arrivaltime)
        #plt.legend()
        #plt.savefig('all_particles.png')
        #plt.close()
        # We now have a list with the arrival times of the photons at the PMT (also for individual particles)
        # The next step is to simulate the PMT
        all_particles_trace = self._simulate_PMT(arrived_photons_per_particle)
        all_particles_trace[all_particles_trace < -MAX_VOLTAGE] = -MAX_VOLTAGE # this sits here because
        # you might want to keep the muon, electron and gamma traces
        muon_trace = self._simulate_PMT(arrived_photons_per_particle_muon)
        electron_trace = self._simulate_PMT(arrived_photons_per_particle_electron)
        gamma_trace = self._simulate_PMT(arrived_photons_per_particle_gamma)
        
        # Now obtain the pulseheight for each trace (in mV)
        pulseheight = 1e3 * abs(all_particles_trace.min())
        pulseheight_muon = 1e3 * abs(muon_trace.min())
        pulseheight_electron = 1e3 * abs(electron_trace.min())
        pulseheight_gamma = 1e3 * abs(gamma_trace.min())
        

        # Now obtain the pulseintegral for each trace (in mVns)
        pulseintegral = 1e3 * abs(2.5*all_particles_trace.sum())
        pulseintegral_muon = 1e3 * abs(2.5*muon_trace.sum())
        pulseintegral_electron = 1e3 * abs(2.5*electron_trace.sum())
        pulseintegral_gamma = 1e3 * abs(2.5*gamma_trace.sum())
        
        # Also determine the first arrival time
        trigger_delay = 0
        for i, value in enumerate(all_particles_trace):
            if value*1e3 < -30.0:
                trigger_delay = i * 2.5
                break
        # If an electron was detected and a gamma without interaction, the event will be triggered but
        # the minimal arrival time will be -999 because of the non-interacting gamma. So I need to
        # correct for this. But if only a non-interacting gamma was detected I need to keep the -999
        # in the list otherwise we don't have a firstarrival time. The solution is to remove all -999
        # values if a pulse height is measured.
        arrivaltimes = np.array(arrivaltimes)
        if pulseheight > 0:
            arrivaltimes = arrivaltimes[arrivaltimes > -999]
        firstarrival = np.min(arrivaltimes) + trigger_delay

        all_particles_trace = np.array(all_particles_trace*1e3,dtype=int)

        return n_muons, n_electrons, n_gammas, firstarrival, pulseintegral, \
               pulseintegral_muon, pulseintegral_electron, pulseintegral_gamma, \
               pulseheight, pulseheight_muon, pulseheight_electron, pulseheight_gamma, \
               all_particles_trace, arrived_photons_per_particle, local_cor
    
    def simulate_trigger(self, detector_observables):
        """Simulate a trigger response.

        This implements the trigger as used on HiSPARC stations:
        - 4-detector station: at least two high or three low signals.
        - 2-detector station: at least 2 low signals.

        :param detector_observables: list of dictionaries, each containing
                                     the observables of one detector.
        :return: True if the station triggers, False otherwise.

        """
        n_detectors = len(detector_observables)
        detectors_low = sum([True for observables in detector_observables
                             if observables['pulseheights'] > 30])
        detectors_high = sum([True for observables in detector_observables
                              if observables['pulseheights'] > 70])
        treshold_low = 3
        if n_detectors == 4 and (detectors_high >= 2 or detectors_low >= treshold_low):
            return True
        elif n_detectors == 2 and detectors_low >= 2:
            return True
        else:
            return False

    def simulate_gps(self, station_observables, shower_parameters, station):
        """Simulate gps timestamp.

        :param station_observables: dictionary containing the observables
                                    of the station.
        :param shower_parameters: dictionary with the shower parameters.
        :param station: :class:`sapphire.clusters.Station` for which
                         to simulate the gps timestamp.
        :return: station_observables updated with gps timestamp and
                 trigger time.

        """
        arrival_times = [station_observables['t%d' % id]
                         for id in range(1, 5)
                         if station_observables.get('n%d' % id, -1) > 0]

        if len(arrival_times) > 1:
            trigger_time = sorted(arrival_times)[1]

            ext_timestamp = shower_parameters['ext_timestamp']
            ext_timestamp += int(trigger_time + station.gps_offset +
                                 self.simulate_gps_uncertainty())
            timestamp = int(ext_timestamp / int(1e9))
            nanoseconds = int(ext_timestamp % int(1e9))

            gps_timestamp = {'ext_timestamp': ext_timestamp,
                             'timestamp': timestamp,
                             'nanoseconds': nanoseconds,
                             't_trigger': trigger_time}
            station_observables.update(gps_timestamp)

        return station_observables

    def get_particles_in_detector(self, detector, shower_parameters):
        """Simulate the detector detection area accurately.

        First particles are filtered to see which fall inside a
        non-rotated square box around the detector (i.e. sides of 1.2m).
        For the remaining particles a more accurate query is used to see
        which actually hit the detector. The advantage of using the
        square is that column indexes can be used, which may speed up
        queries.

        :param detector: :class:`~sapphire.clusters.Detector` for which
                         to get particles.
        :param shower_parameters: dictionary with the shower parameters.

        """
        
        # Possible keys in particles
        #
        # particle_id, 1 = gamma, 2-3 is electron, 4 is neutrino, 5-6 is muon
        # r - core distance in m
        # phi - azimuth angle in rad
        # x - x position in m
        # y - y position in m
        # t - time since first interaction in ns
        # p_x - momentum in x direction in eV/c
        # p_y - momentum in y direction in eV/c
        # p_z - momentum in z direction in eV/c
        # hadron_generation
        # observation_level - observation level above sea level in cm
        
        detector_boundary = 3.0

        x, y, z = detector.get_coordinates()
        detcorners = detector.get_corners()

        # Obtain corners
        c1 = np.array(detcorners[0])
        c2 = np.array(detcorners[1])
        c3 = np.array(detcorners[2])
        c4 = np.array(detcorners[3])
        
        # Rotate corners to convenient system where the axes of the detector align with x and y
        # I don't know what all the entries in the orientation list are but the last one works.
        theta = detector.orientation[-1] + (self.shower_azimuth - self.corsika_azimuth)
        THETA = np.array([[np.cos(theta),-1*np.sin(theta)], [np.sin(theta),np.cos(theta)]])
        
        c1_new = np.inner(THETA, (c1 - c1)) + c1
        c2_new = np.inner(THETA, (c2 - c1)) + c1
        c3_new = np.inner(THETA, (c3 - c1)) + c1
        c4_new = np.inner(THETA, (c4 - c1)) + c1
        
        # Increase the size of the detector to also include perspex hits and near misses 
        # (which could still be a hit because the skibox lid is a bit higher than the scintillator)
        c1_new = np.array([c1_new[0] - 0.1, c1_new[1] - 0.1])
        c2_new = np.array([c2_new[0] + 0.1, c2_new[1] - 0.1])
        c3_new = np.array([c3_new[0] + 0.1, c3_new[1] + 0.675 + 0.1])
        c4_new = np.array([c4_new[0] - 0.1, c4_new[1] + 0.675 + 0.1])
        
        # Rotate the system back
        theta = -1.0 * theta
        THETA_BACK = np.array([[np.cos(theta),-1*np.sin(theta)], [np.sin(theta),np.cos(theta)]])
        
        c1_new = np.inner(THETA_BACK, (c1_new - c1)) + c1
        c2_new = np.inner(THETA_BACK, (c2_new - c1)) + c1
        c3_new = np.inner(THETA_BACK, (c3_new - c1)) + c1
        c4_new = np.inner(THETA_BACK, (c4_new - c1)) + c1
        
        # Slightly bigger corners now
        corners = [c1_new, c2_new, c3_new, c4_new]

        zenith = shower_parameters['zenith']
        azimuth = self.corsika_azimuth

        znxnz = z * tan(zenith) * cos(azimuth)
        znynz = z * tan(zenith) * sin(azimuth)
        xproj = x - znxnz
        yproj = y - znynz

        cproj = [(cx - znxnz, cy - znynz) for cx, cy in corners]

        b11, line1, b12 = self.get_line_boundary_eqs(*cproj[0:3])
        b21, line2, b22 = self.get_line_boundary_eqs(*cproj[1:4])
        query = ("(x >= %f) & (x <= %f) & (y >= %f) & (y <= %f) & "
                 "(b11 < %s) & (%s < b12) & (b21 < %s) & (%s < b22) & "
                 "(particle_id <= 6)" %
                 (xproj - detector_boundary, xproj + detector_boundary,
                  yproj - detector_boundary, yproj + detector_boundary,
                  line1, line1, line2, line2))

        return self.groundparticles.read_where(query)

    def get_line_boundary_eqs(self, p0, p1, p2):
        """Get line equations using three points

        Given three points, this function computes the equations for two
        parallel lines going through these points.  The first and second
        point are on the same line, whereas the third point is taken to
        be on a line which runs parallel to the first.  The return value
        is an equation and two boundaries which can be used to test if a
        point is between the two lines.

        :param p0,p1: (x, y) tuples on the same line.
        :param p2: (x, y) tuple on the parallel line.
        :return: value1, equation, value2, such that points satisfying
            value1 < equation < value2 are between the parallel lines.

        Example::

            >>> get_line_boundary_eqs((0, 0), (1, 1), (0, 2))
            (0.0, 'y - 1.000000 * x', 2.0)

        """
        (x0, y0), (x1, y1), (x2, y2) = p0, p1, p2

        # Compute the general equation for the lines
        if x0 == x1:
            # line is exactly vertical
            line = "x"
            b1, b2 = x0, x2
        else:
            # First, compute the slope
            a = (y1 - y0) / (x1 - x0)

            # Calculate the y-intercepts of both lines
            b1 = y0 - a * x0
            b2 = y2 - a * x2

            line = "y - %f * x" % a

        # And order the y-intercepts
        if b1 > b2:
            b1, b2 = b2, b1

        return b1, line, b2

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
        row['shower_energy'] = self.corsika_energy
        row['zenith'] = self.corsika_zenith
        row['azimuth'] = self.shower_azimuth
        row['cr_particle'] = self.cr_particle
        row['core_distance'] = self.core_distance
        row['seeds'] = self.seeds
        for key, value in iteritems(station_observables):
            if key in events_table.colnames:
                row[key] = value
            elif key == 'photontimes' and self.save_detailed_traces:
                reference_idx = []
                photon_array = self.station_groups[station_id].photontimes
                idx_photon = len(photon_array)
                for photontimes in value:
                    photon_array.append(photontimes)
                    reference_idx.append(idx_photon)
                    idx_photon += 1
                row['photontimes_idx'] = reference_idx
            else:
                warnings.warn('Unsupported variable: %s' % key)
        row.append()
        events_table.flush()

        return events_table.nrows - 1


class GroundParticlesSimulation(HiSPARCSimulation):

    def __init__(self, corsikafile_path, max_core_distance, *args, **kwargs):
        """Simulation initialization

        :param corsikafile_path: path to the corsika.h5 file containing
                                 the groundparticles.
        :param max_core_distance: maximum distance of shower core to
                                  center of cluster.

        """
        super(GroundParticlesSimulation, self).__init__(*args, **kwargs)

        self.corsikafile = tables.open_file(corsikafile_path, 'r')
        self.groundparticles = self.corsikafile.get_node('/groundparticles')
        self.max_core_distance = max_core_distance

    def __del__(self):
        self.finish()

    def finish(self):
        """Clean-up after simulation"""

        self.corsikafile.close()

    def generate_shower_parameters(self):
        """Generate shower parameters like core position, energy, etc.

        For this groundparticles simulation, only the shower core position
        and rotation angle of the shower are generated.  Do *not*
        interpret these parameters as the position of the cluster, or the
        rotation of the cluster!  Interpret them as *shower* parameters.

        :return: dictionary with shower parameters: core_pos
                 (x, y-tuple) and azimuth.

        """
        r_max = self.max_core_distance
        now = int(time())

        event_header = self.corsikafile.get_node_attr('/', 'event_header')
        event_end = self.corsikafile.get_node_attr('/', 'event_end')
        corsika_parameters = {'zenith': event_header.zenith,
                              'size': event_end.n_electrons_levels,
                              'energy': event_header.energy,
                              'particle': event_header.particle}
        self.corsika_azimuth = event_header.azimuth

        for i in pbar(range(self.n), show=self.progress):
            ext_timestamp = (now + i) * int(1e9)
            x, y = self.generate_core_position(r_max)
            shower_azimuth = self.generate_azimuth()

            shower_parameters = {'ext_timestamp': ext_timestamp,
                                 'core_pos': (x, y),
                                 'azimuth': shower_azimuth}

            # Subtract CORSIKA shower azimuth from desired shower azimuth
            # make it fit in (-pi, pi] to get rotation angle of the cluster.
            alpha = shower_azimuth - self.corsika_azimuth
            alpha = norm_angle(alpha)
            self._prepare_cluster_for_shower(x, y, alpha)

            shower_parameters.update(corsika_parameters)
            yield shower_parameters

    def _prepare_cluster_for_shower(self, x, y, alpha):
        """Prepare the cluster object for the simulation of a shower.

        Rotate and translate the cluster so that (0, 0) coincides with the
        shower core position and that the angle between the rotated cluster
        and the CORSIKA shower is the desired azimuth.

        :param x,y: position of shower core relative to cluster origin in m.
        :param alpha: angle the cluster needs to be rotated in radians.

        """
        # rotate the core position around the original cluster center
        xp = x * cos(-alpha) - y * sin(-alpha)
        yp = x * sin(-alpha) + y * cos(-alpha)

        self.cluster.set_coordinates(-xp, -yp, 0, -alpha)

    def simulate_detector_response(self, detector, shower_parameters):
        """Simulate detector response to a shower.

        Checks if leptons have passed a detector. If so, it returns the number
        of leptons in the detector and the arrival time of the first lepton
        passing the detector.

        :param detector: :class:`~sapphire.clusters.Detector` for which
                         the observables will be determined.
        :param shower_parameters: dictionary with the shower parameters.

        """

        particles = self.get_particles_in_detector(detector, shower_parameters)
        n_detected = len(particles)

        if n_detected:
            mips = self.simulate_detector_mips_for_particles(particles)
            particles['t'] += self.simulate_signal_transport_time(n_detected)
            nz = cos(shower_parameters['zenith'])
            tproj = detector.get_coordinates()[-1] / (c * nz)
            first_signal = particles['t'].min() + detector.offset - tproj
            observables = {'n': round(mips, 3),
                           't': self.simulate_adc_sampling(first_signal)}
        else:
            observables = {'n': 0., 't': -999}

        return observables

    def simulate_detector_mips_for_particles(self, particles):
        """Simulate the detector signal for particles

        :param particles: particle rows with the p_[x, y, z]
                          components of the particle momenta.

        """
        # determination of lepton angle of incidence
        theta = np.arccos(abs(particles['p_z']) /
                          vector_length(particles['p_x'], particles['p_y'],
                                        particles['p_z']))
        n = len(particles)
        mips = self.simulate_detector_mips(n, theta)

        return mips

    def simulate_trigger(self, detector_observables):
        """Simulate a trigger response.

        This implements the trigger as used on HiSPARC stations:
        - 4-detector station: at least two high or three low signals.
        - 2-detector station: at least 2 low signals.

        :param detector_observables: list of dictionaries, each containing
                                     the observables of one detector.
        :return: True if the station triggers, False otherwise.

        """
        n_detectors = len(detector_observables)
        detectors_low = sum([True for observables in detector_observables
                             if observables['n'] > 0.3])
        detectors_high = sum([True for observables in detector_observables
                              if observables['n'] > 0.5])

        if n_detectors == 4 and (detectors_high >= 2 or detectors_low >= 3):
            return True
        elif n_detectors == 2 and detectors_low >= 2:
            return True
        else:
            return False

    def simulate_gps(self, station_observables, shower_parameters, station):
        """Simulate gps timestamp.

        :param station_observables: dictionary containing the observables
                                    of the station.
        :param shower_parameters: dictionary with the shower parameters.
        :param station: :class:`sapphire.clusters.Station` for which
                         to simulate the gps timestamp.
        :return: station_observables updated with gps timestamp and
                 trigger time.

        """
        arrival_times = [station_observables['t%d' % id]
                         for id in range(1, 5)
                         if station_observables.get('n%d' % id, -1) > 0]

        if len(arrival_times) > 1:
            trigger_time = sorted(arrival_times)[1]

            ext_timestamp = shower_parameters['ext_timestamp']
            ext_timestamp += int(trigger_time + station.gps_offset +
                                 self.simulate_gps_uncertainty())
            timestamp = int(ext_timestamp / int(1e9))
            nanoseconds = int(ext_timestamp % int(1e9))

            gps_timestamp = {'ext_timestamp': ext_timestamp,
                             'timestamp': timestamp,
                             'nanoseconds': nanoseconds,
                             't_trigger': trigger_time}
            station_observables.update(gps_timestamp)

        return station_observables

    def get_particles_in_detector(self, detector, shower_parameters):
        """Get particles that hit a detector.

        Particle ids 2, 3, 5, 6 are electrons and muons,
        id 4 is no longer used (were neutrino's).

        The detector is approximated by a square with a surface of 0.5
        square meter which is *not* correctly rotated.  In fact, during
        the simulation, the rotation of the detector is undefined.  This
        is faster than a more thorough implementation.

        The CORSIKA simulation azimuth is used for the projection because the
        cluster is rotated such that from the perspective of the rotated
        detectors the CORSIKA showers come from the desired azimuth. In the
        simulation frame the CORSIKA shower azimuth remains unchanged.

        :param detector: :class:`~sapphire.clusters.Detector` for which
                         to get particles.
        :param shower_parameters: dictionary with the shower parameters.

        """
        detector_boundary = sqrt(0.5) / 2.

        x, y, z = detector.get_coordinates()
        zenith = shower_parameters['zenith']
        azimuth = self.corsika_azimuth

        nxnz = tan(zenith) * cos(azimuth)
        nynz = tan(zenith) * sin(azimuth)
        xproj = x - z * nxnz
        yproj = y - z * nynz

        query = ('(x >= %f) & (x <= %f) & (y >= %f) & (y <= %f)'
                 ' & (particle_id >= 2) & (particle_id <= 6)' %
                 (xproj - detector_boundary, xproj + detector_boundary,
                  yproj - detector_boundary, yproj + detector_boundary))
        return self.groundparticles.read_where(query)


class GroundParticlesGammaSimulation(GroundParticlesSimulation):
    """Simulation which includes signals from gamma particles in the shower"""

    def simulate_detector_response(self, detector, shower_parameters):
        """Simulate detector response to a shower.

        Checks if particles have passed a detector. If so, it returns the
        number of particles in the detector and the arrival time of the first
        particle passing the detector.

        :param detector: :class:`~sapphire.clusters.Detector` for which
                         the observables will be determined.
        :param shower_parameters: dictionary with the shower parameters.

        """
        leptons, gammas = self.get_particles_in_detector(detector,
                                                         shower_parameters)
        n_leptons = len(leptons)
        n_gammas = len(gammas)

        if not n_leptons + n_gammas:
            return {'n': 0, 't': -999}

        if n_leptons:
            mips_lepton = self.simulate_detector_mips_for_particles(leptons)
            leptons['t'] += self.simulate_signal_transport_time(n_leptons)
            first_lepton = leptons['t'].min()
        else:
            mips_lepton = 0

        if n_gammas:
            mips_gamma = self.simulate_detector_mips_for_gammas(gammas)
            gammas['t'] += self.simulate_signal_transport_time(n_gammas)
            first_gamma = gammas['t'].min()
        else:
            mips_gamma = 0

        if n_leptons and n_gammas:
            first_signal = min(first_lepton, first_gamma) + detector.offset
        elif n_leptons:
            first_signal = first_lepton + detector.offset
        elif n_gammas:
            first_signal = first_gamma + detector.offset

        return {'n': mips_lepton + mips_gamma,
                't': self.simulate_adc_sampling(first_signal)}

    def get_particles_in_detector(self, detector, shower_parameters):
        """Get particles that hit a detector.

        Particle ids 2, 3, 5, 6 are electrons and muons,
        id 4 is no longer used (were neutrino's).

        The detector is approximated by a square with a surface of 0.5
        square meter which is *not* correctly rotated.  In fact, during
        the simulation, the rotation of the detector is undefined.  This
        is faster than a more thorough implementation.

        *Detector height is ignored!*

        :param detector: :class:`~sapphire.clusters.Detector` for which
                         to get particles.
        :param shower_parameters: dictionary with the shower parameters.

        """
        detector_boundary = sqrt(.5) / 2.

        x, y, z = detector.get_coordinates()
        zenith = shower_parameters['zenith']
        azimuth = self.corsika_azimuth

        nxnz = tan(zenith) * cos(azimuth)
        nynz = tan(zenith) * sin(azimuth)
        xproj = x - z * nxnz
        yproj = y - z * nynz

        query_leptons = \
            ('(x >= %f) & (x <= %f) & (y >= %f) & (y <= %f)'
             ' & (particle_id >= 2) & (particle_id <= 6)' %
             (xproj - detector_boundary, xproj + detector_boundary,
              yproj - detector_boundary, yproj + detector_boundary))

        query_gammas = \
            ('(x >= %f) & (x <= %f) & (y >= %f) & (y <= %f)'
             ' & (particle_id == 1)' %
             (xproj - detector_boundary, xproj + detector_boundary,
              yproj - detector_boundary, yproj + detector_boundary))

        return (self.groundparticles.read_where(query_leptons),
                self.groundparticles.read_where(query_gammas))

    def simulate_detector_mips_for_gammas(self, particles):
        """Simulate the detector signal for gammas

        :param particles: particle rows with the p_[x, y, z]
                          components of the particle momenta.

        """
        p_gamma = np.sqrt(particles['p_x'] ** 2 + particles['p_y'] ** 2 +
                          particles['p_z'] ** 2)

        # determination of lepton angle of incidence
        theta = np.arccos(abs(particles['p_z']) /
                          p_gamma)

        mips = simulate_detector_mips_gammas(p_gamma, theta)

        return mips


class DetectorBoundarySimulation(GroundParticlesSimulation):

    """More accuratly simulate the detection area of the detectors.

    Take the orientation of the detectors into account and use the
    exact detector boundaries. This requires a slightly more complex
    query which is a bit slower.

    """

    def get_particles_in_detector(self, detector, shower_parameters):
        """Simulate the detector detection area accurately.

        First particles are filtered to see which fall inside a
        non-rotated square box around the detector (i.e. sides of 1.2m).
        For the remaining particles a more accurate query is used to see
        which actually hit the detector. The advantage of using the
        square is that column indexes can be used, which may speed up
        queries.

        :param detector: :class:`~sapphire.clusters.Detector` for which
                         to get particles.
        :param shower_parameters: dictionary with the shower parameters.

        """
        detector_boundary = 0.6

        x, y, z = detector.get_coordinates()
        corners = detector.get_corners()
        zenith = shower_parameters['zenith']
        azimuth = self.corsika_azimuth

        znxnz = z * tan(zenith) * cos(azimuth)
        znynz = z * tan(zenith) * sin(azimuth)
        xproj = x - znxnz
        yproj = y - znynz

        cproj = [(cx - znxnz, cy - znynz) for cx, cy in corners]

        b11, line1, b12 = self.get_line_boundary_eqs(*cproj[0:3])
        b21, line2, b22 = self.get_line_boundary_eqs(*cproj[1:4])
        query = ("(x >= %f) & (x <= %f) & (y >= %f) & (y <= %f) & "
                 "(b11 < %s) & (%s < b12) & (b21 < %s) & (%s < b22) & "
                 "(particle_id >= 2) & (particle_id <= 6)" %
                 (xproj - detector_boundary, xproj + detector_boundary,
                  yproj - detector_boundary, yproj + detector_boundary,
                  line1, line1, line2, line2))

        return self.groundparticles.read_where(query)

    def get_line_boundary_eqs(self, p0, p1, p2):
        """Get line equations using three points

        Given three points, this function computes the equations for two
        parallel lines going through these points.  The first and second
        point are on the same line, whereas the third point is taken to
        be on a line which runs parallel to the first.  The return value
        is an equation and two boundaries which can be used to test if a
        point is between the two lines.

        :param p0,p1: (x, y) tuples on the same line.
        :param p2: (x, y) tuple on the parallel line.
        :return: value1, equation, value2, such that points satisfying
            value1 < equation < value2 are between the parallel lines.

        Example::

            >>> get_line_boundary_eqs((0, 0), (1, 1), (0, 2))
            (0.0, 'y - 1.000000 * x', 2.0)

        """
        (x0, y0), (x1, y1), (x2, y2) = p0, p1, p2

        # Compute the general equation for the lines
        if x0 == x1:
            # line is exactly vertical
            line = "x"
            b1, b2 = x0, x2
        else:
            # First, compute the slope
            a = (y1 - y0) / (x1 - x0)

            # Calculate the y-intercepts of both lines
            b1 = y0 - a * x0
            b2 = y2 - a * x2

            line = "y - %f * x" % a

        # And order the y-intercepts
        if b1 > b2:
            b1, b2 = b2, b1

        return b1, line, b2


class ParticleCounterSimulation(GroundParticlesSimulation):

    """Do not simulate mips, just count the number of particles."""

    def simulate_detector_mips(self, n, theta):
        """A mip for a mip, count number of particles in a detector."""

        return n


class FixedCoreDistanceSimulation(GroundParticlesSimulation):

    """Shower core at a fixed core distance (from cluster origin).

    :param core_distance: distance of shower core to center of cluster.

    """

    @classmethod
    def generate_core_position(cls, r_max):
        """Generate a random core position on a circle

        :param r_max: Fixed core distance, in meters.
        :return: Random x, y position on the circle with radius r_max.

        """
        phi = np.random.uniform(-pi, pi)
        x = r_max * cos(phi)
        y = r_max * sin(phi)
        return x, y


class GroundParticlesSimulationWithoutErrors(ErrorlessSimulation,
                                             GroundParticlesSimulation):

    """This simulation does not simulate errors/uncertainties

    This results in perfect timing (first particle through detector)
    and particle counting for the detectors.

    """

    pass


class MultipleGroundParticlesSimulation(GroundParticlesSimulation):

    """Use multiple CORSIKA simulated air showers in one run.

    Simulations will be selected from the set of available showers.
    Each time an energy and zenith angle is generated a shower is selected
    from the CORSIKA overview. Each shower is reused multiple times to
    take advantage of caching, and to reduce IO stress.

    .. warning::

        This simulation loads a new shower often it is therefore more I/O
        intensive than :class:`GroundParticlesSimulation`. Do not run many
        of these simulations simultaneously!

    """

    # CORSIKA data location at Nikhef
    DATA = '/data/hisparc/corsika/data/{seeds}/corsika.h5'

    def __init__(self, corsikaoverview_path, max_core_distance, min_energy,
                 max_energy, *args, **kwargs):
        """Simulation initialization

        :param corsikaoverview_path: path to the corsika_overview.h5 file
                                     containing the available simulations.
        :param max_core_distance: maximum distance of shower core to
                                  center of cluster.
        :param min_energy,max_energy: upper and lower shower energy limits,
                                      in eV.

        """
        # Super of the super class.
        super(GroundParticlesSimulation, self).__init__(*args, **kwargs)
        
        
        self.cq = CorsikaQuery(corsikaoverview_path)
        self.max_core_distance = max_core_distance
        self.min_energy = min_energy
        self.max_energy = max_energy
        self.available_energies = {e for e in self.cq.all_energies
                                   if min_energy <= 10 ** e <= max_energy}
        self.available_zeniths = {e: self.cq.available_parameters('zenith',
                                                                  energy=e)
                                  for e in self.available_energies}

    def finish(self):
        """Clean-up after simulation"""

        self.cq.finish()

    def generate_shower_parameters(self):
        """Generate shower parameters like core position, energy, etc.

        For this groundparticles simulation, only the shower core position
        and rotation angle of the shower are generated.  Do *not*
        interpret these parameters as the position of the cluster, or the
        rotation of the cluster!  Interpret them as *shower* parameters.

        :return: dictionary with shower parameters: core_pos
                 (x, y-tuple) and azimuth.

        """
        r = self.max_core_distance
        n_reuse = 100
        now = int(time())

        for i in pbar(range(self.n), show=self.progress):
            sim = self.select_simulation()
            if sim is None:
                continue
            
            corsika_parameters = {'zenith': sim['zenith'],
                                  'size': sim['n_electron'],
                                  'energy': sim['energy'],
                                  'particle': sim['particle_id']}
            self.corsika_azimuth = sim['azimuth']

            seeds = self.cq.seeds([sim])[0]
            self.seeds = np.fromstring( seeds, dtype=np.int, sep='_' )
            with tables.open_file(self.DATA.format(seeds=seeds), 'r') as data:
                try:
                    self.groundparticles = data.get_node('/groundparticles')
                except tables.NoSuchNodeError:
                    print('No groundparticles in %s' % seeds)
                    continue

                for j in range(n_reuse):
                    ext_timestamp = (now + i + (float(j) / n_reuse)) * int(1e9)
                    x, y = self.generate_core_position(r)
                    shower_azimuth = self.generate_azimuth()

                    shower_parameters = {'ext_timestamp': ext_timestamp,
                                         'core_pos': (x, y),
                                         'azimuth': shower_azimuth}

                    # Subtract CORSIKA shower azimuth from desired shower
                    # azimuth to get rotation angle of the cluster.
                    alpha = shower_azimuth - self.corsika_azimuth
                    alpha = norm_angle(alpha)
                    self._prepare_cluster_for_shower(x, y, alpha)

                    shower_parameters.update(corsika_parameters)
                    yield shower_parameters

    def select_simulation(self):
        """Generate parameters for selecting a CORSIKA simulation

        :return: simulation row from a CORSIKA Simulations table.

        """
        energy = self.generate_energy(self.min_energy, self.max_energy)
        shower_energy = closest_in_list(log10(energy), self.available_energies)

        zenith = self.generate_zenith()
        shower_zenith = closest_in_list(np.degrees(zenith),
                                        self.available_zeniths[shower_energy])

        azimuth = 180.0*np.random.randint(0,2)
        shower_azimuth = closest_in_list(azimuth,self.available_azimuths)

        sims = self.cq.simulations(energy=shower_energy, zenith=shower_zenith,
                                   azimuth=shower_azimuth)
        if not len(sims):
            return None
        sim = np.random.choice(sims)
        return sim


        
        
class MultipleGroundParticlesGEANT4Simulation(GroundParticlesGEANT4Simulation):

    """Use multiple CORSIKA simulated air showers in one run.

    Simulations will be selected from the set of available showers.
    Each time an energy and zenith angle is generated a shower is selected
    from the CORSIKA overview. Each shower is reused multiple times to
    take advantage of caching, and to reduce IO stress.

    .. warning::

        This simulation loads a new shower often it is therefore more I/O
        intensive than :class:`GroundParticlesSimulation`. Do not run many
        of these simulations simultaneously!

    """

    # CORSIKA data location at Nikhef
    DATA = '/dcache/hisparc/kaspervd/corsika_low_energy_cuts/data/{seeds}/corsika.h5'

    def __init__(self, corsikaoverview_path, max_core_distance, min_energy,
                 max_energy, cutoff_number_of_particles=None, zenith=None,
                 trigger_function=None, *args, **kwargs):
        """Simulation initialization

        :param corsikaoverview_path: path to the corsika_overview.h5 file
                                     containing the available simulations.
        :param max_core_distance: maximum distance of shower core to
                                  center of cluster.
        :param min_energy,max_energy: upper and lower shower energy limits,
                                      in eV.

        """
        super(GroundParticlesGEANT4Simulation, self).__init__(*args, **kwargs)
        #super(MultipleGroundParticlesGEANT4Simulation, self).__init__(*args, **kwargs)
        #super().__init__(*args, **kwargs)
        
        self.zenith = zenith # this value is later checked by select_simulation
        self.cutoff_number_of_particles = cutoff_number_of_particles
        self.cq = CorsikaQuery(corsikaoverview_path)
        self.max_core_distance = max_core_distance
        self.min_energy = min_energy
        self.max_energy = max_energy

        self.available_energies = {e for e in self.cq.all_energies
                                   if min_energy <= 10 ** e <= max_energy}
        self.available_zeniths = {e: self.cq.available_parameters('zenith',
                                                                  energy=e)
                                  for e in self.available_energies}
        self.available_azimuths = {e: self.cq.available_parameters('azimuth',
                                                                   energy=e)
                                  for e in self.available_energies}

        if trigger_function is not None:
            self.trigger_function = trigger_function
            self.use_preliminary = True

    def finish(self):
        """Clean-up after simulation"""

        self.cq.finish()

    def generate_shower_parameters(self):
        """Generate shower parameters like core position, energy, etc.

        For this groundparticles simulation, only the shower core position
        and rotation angle of the shower are generated.  Do *not*
        interpret these parameters as the position of the cluster, or the
        rotation of the cluster!  Interpret them as *shower* parameters.

        :return: dictionary with shower parameters: core_pos
                 (x, y-tuple) and azimuth.

        """
        r = self.max_core_distance
        n_reuse = 1
        now = int(time())

        for i in pbar(range(self.n), show=self.progress):
            sim = self.select_simulation()
            if sim is None:
                continue
            corsika_parameters = {'zenith': sim['zenith'],
                                  'size': sim['n_electron'],
                                  'energy': sim['energy'],
                                  'particle': sim['particle_id']}
            self.corsika_azimuth = sim['azimuth']

            self.corsika_zenith = sim['zenith']
            self.corsika_energy = sim['energy']
            self.cr_particle = sim['particle_id']

            seeds = self.cq.seeds([sim])[0]
            self.seeds = np.fromstring( seeds, dtype=np.int, sep='_' )
            if self.corsika_energy < (1e14 - 1e10):
                # Because of the high dcache i/o load I create, all
                # CORSIKA simulations with an energy below log(eV) = 14 
                # were moved to a temporary directory on the stoomboot node.
                tmpdir = os.environ["TMPDIR"]
                localDATA = tmpdir+"/{seeds}/corsika.h5"
                #print("Load local")

                with tables.open_file(localDATA.format(seeds=seeds), 'r') as data:
                    try:
                        self.groundparticles = data.get_node('/groundparticles')
                    except tables.NoSuchNodeError:
                        print('No groundparticles in %s' % seeds)
                        continue

                    for j in range(n_reuse):
                        ext_timestamp = (now + i + (float(j) / n_reuse)) * int(1e9)
                        
                        x, y = self.generate_core_position(r)
                        self.core_distance = np.sqrt(x**2 + y**2)
                        self.shower_azimuth = self.generate_azimuth()

                        shower_parameters = {'ext_timestamp': ext_timestamp,
                                             'core_pos': (x, y),
                                             'azimuth': self.shower_azimuth}

                        # Subtract CORSIKA shower azimuth from desired shower
                        # azimuth to get rotation angle of the cluster.
                        alpha = self.shower_azimuth - self.corsika_azimuth
                        alpha = norm_angle(alpha)
                        self._prepare_cluster_for_shower(x, y, alpha)
    
                        shower_parameters.update(corsika_parameters)
                        yield shower_parameters

            else: # Use the regular dcache data
                #print("Load dCache")
                with tables.open_file(self.DATA.format(seeds=seeds), 'r') as data:
                    try:
                        self.groundparticles = data.get_node('/groundparticles')
                        '''
                        n = 0 
                        for row in self.groundparticles:
                            if row['particle_id'] in [2,3]:
                                n+=1
                        print('Out of %s particles %s are electrons, %.2f percent' % (len(self.groundparticles),n, n/len(self.groundparticles)))
                        '''
                    except tables.NoSuchNodeError:
                        print('No groundparticles in %s' % seeds)
                        continue

                    for j in range(n_reuse):
                        ext_timestamp = (now + i + (float(j) / n_reuse)) * int(1e9)
                        x, y = self.generate_core_position(r)
                        self.core_distance = np.sqrt(x**2 + y**2)
                        self.shower_azimuth = self.generate_azimuth()

                        shower_parameters = {'ext_timestamp': ext_timestamp,
                                             'core_pos': (x, y),
                                             'azimuth': self.shower_azimuth}

                        # Subtract CORSIKA shower azimuth from desired shower
                        # azimuth to get rotation angle of the cluster.
                        alpha = self.shower_azimuth - self.corsika_azimuth
                        alpha = norm_angle(alpha)
                        self._prepare_cluster_for_shower(x, y, alpha)
    
                        shower_parameters.update(corsika_parameters)
                        yield shower_parameters


    def select_simulation(self):
        """Generate parameters for selecting a CORSIKA simulation

        :return: simulation row from a CORSIKA Simulations table.

        """
        energy = self.generate_energy(self.min_energy, self.max_energy)
        shower_energy = closest_in_list(log10(energy), self.available_energies)
        
        
        if self.zenith is None:
            zenith = self.generate_zenith()
        else:
            zenith = self.zenith
        shower_zenith = closest_in_list(np.degrees(zenith),
                                        self.available_zeniths[shower_energy])

        sims = self.cq.simulations(energy=shower_energy, zenith=shower_zenith)
        if not len(sims):
            return None
        sim = np.random.choice(sims)
        return sim


class RandomRadiiGEANT4Simulation(MultipleGroundParticlesGEANT4Simulation):

    """In a normal MultipleGroundParticlesGEANT4Simulation simulation the core
    position is chosen to be a random position within a circle area. This class
    enables the same simulation but now the core position is chosen at random
    radii.

    """
    
    @classmethod
    def generate_core_position(cls, r_max):
        phi = np.random.uniform(-pi, pi)
        r = np.random.uniform(0, r_max)
        x = r * cos(phi)
        y = r * sin(phi)
        return x, y



class SingleGEANTSimulation(GroundParticlesGEANT4Simulation):
    """
    This class inherits from GroundParticlesGEANT4Simulation in order to use the
    _simulate_pmt function
    """

    def __init__(self, progress):
        self.progress = progress
    def __del__(self):
        # overwriting the previous __del__ function which attempts to close the
        # corsikafile (which is not open here)
        pass
    def singleGEANTsim(self, particletype, particleenergy, xdetcoord, ydetcoord, px,
                       py, pz):
        """
        Runs a single GEANT simulation and returns the trace
        :param particletype: string with particletype ('e-' etc.)
        :param particleenergy: energy in eV
        :param xdetcoord: x-location in local coordinates (0,0) is the center of the
        scintillator
        :param ydetcoord: y-location
        :param px: momentum in x direction
        :param py: momentum in y direction
        :param pz: momentum in z direction
        :return: trace in V
        """


        arrived_photons_per_particle = []
        arrived_photons_per_particle_muon = []
        arrived_photons_per_particle_electron = []
        arrived_photons_per_particle_gamma = []
        n_muons = 0
        n_electrons = 0
        n_gammas = 0
        try:  # sometimes the program crashes with exit status 11
            output = subprocess.check_output([
                                                 "/user/kaspervd/Documents/repositories/diamond/20170117_geant4_simulation/HiSPARC-stbc-build/./skibox",
                                                 "1", particletype,
                                                 "{}".format(particleenergy),
                                                 "{}".format(xdetcoord),
                                                 "{}".format(ydetcoord),
                                                 "-99889",  # "-99893.695",
                                                 "{}".format(px),
                                                 "{}".format(py),
                                                 "{}".format(pz)])
            if self.verbose:
                print("./skibox", "1", particletype,
                      "{}".format(particleenergy),
                      "{}".format(xdetcoord),
                      "{}".format(ydetcoord),
                      "-99889",
                      "{}".format(px),
                      "{}".format(py),
                      "{}".format(pz))

            # Determine the number of photons that have arrived at the PMT
            # and the time it took for the first photon to arrive at the PMT.
            geantfile = np.genfromtxt("RUN_1/outpSD.csv", delimiter=",")
            try:
                photontimes = geantfile[1:, 0]
                # Succesful interaction, keep statistics
            except:
                # No photons have arrived (a gamma that didn't undergo any
                # iteraction).
                photontimes = np.array([])  # empty list

            # Remove the directory created by the GEANT4 simulation
            shutil.rmtree("RUN_1")
        except:  # If the program crashed with exit status 11
            photontimes = np.array([])

        # If multiple particles hit the detector, they are treated
        # seperately. Make lists in order to be able to add all
        # arrived photons.
        arrived_photons_per_particle = np.append(arrived_photons_per_particle,
                                                 photontimes)

        # We now have a list with the arrival times of the photons at the PMT (also for
        #  individual particles)
        # The next step is to simulate the PMT
        all_particles_trace = self._simulate_PMT(arrived_photons_per_particle)
        all_particles_trace[all_particles_trace < -MAX_VOLTAGE] = -MAX_VOLTAGE


        return all_particles_trace