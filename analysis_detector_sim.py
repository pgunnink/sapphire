import tables
from itertools import combinations
import re
import csv

from pylab import *
from scipy.optimize import curve_fit
from tikz_plot import tikz_2dhist

USE_TEX = False


DETECTORS = [(0., 5.77, 'UD'), (0., 0., 'UD'), (-5., -2.89, 'LR'),
             (5., -2.89, 'LR')]

#TIMING_ERROR = 1.3
#TIMING_ERROR = 2 * 1.3
#TIMING_ERROR = 2.5
TIMING_ERROR = 3

# For matplotlib plots
if USE_TEX:
    rcParams['font.serif'] = 'Computer Modern'
    rcParams['font.sans-serif'] = 'Computer Modern'
    rcParams['font.family'] = 'sans-serif'
    rcParams['figure.figsize'] = [5 * x for x in (1, 2./3)]
    rcParams['figure.subplot.left'] = 0.125
    rcParams['figure.subplot.bottom'] = 0.125
    rcParams['font.size'] = 11
    rcParams['legend.fontsize'] = 'small'


class ReconstructedEvent(tables.IsDescription):
    r = tables.Float32Col()
    phi = tables.Float32Col()
    alpha = tables.Float32Col()
    t1 = tables.Float32Col()
    t2 = tables.Float32Col()
    t3 = tables.Float32Col()
    t4 = tables.Float32Col()
    n1 = tables.UInt16Col()
    n2 = tables.UInt16Col()
    n3 = tables.UInt16Col()
    n4 = tables.UInt16Col()
    sim_theta = tables.Float32Col()
    sim_phi = tables.Float32Col()
    r_theta = tables.Float32Col()
    r_phi = tables.Float32Col()
    D = tables.UInt16Col()
    size = tables.UInt8Col()
    bin = tables.Float32Col()
    bin_r = tables.BoolCol()


def reconstruct_angle(event, R=10):
    """Reconstruct angles from a single event"""

    dt1 = event['t1'] - event['t3']
    dt2 = event['t1'] - event['t4']

    return reconstruct_angle_dt(dt1, dt2, R)

def reconstruct_angle_dt(dt1, dt2, R=10):
    """Reconstruct angle given time differences"""

    c = 3.00e+8

    r1 = R
    r2 = R 

    phi1 = calc_phi(1, 3)
    phi2 = calc_phi(1, 4)

    phi = arctan2((dt2 * r1 * cos(phi1) - dt1 * r2 * cos(phi2)),
                  (dt2 * r1 * sin(phi1) - dt1 * r2 * sin(phi2)) * -1)
    theta1 = arcsin(c * dt1 * 1e-9 / (r1 * cos(phi - phi1)))
    theta2 = arcsin(c * dt2 * 1e-9 / (r2 * cos(phi - phi2)))

    e1 = sqrt(rel_theta1_errorsq(theta1, phi, phi1, phi2, R, R))
    e2 = sqrt(rel_theta2_errorsq(theta2, phi, phi1, phi2, R, R))

    theta_wgt = (1 / e1 * theta1 + 1 / e2 * theta2) / (1 / e1 + 1 / e2)

    return theta_wgt, phi

def calc_phi(s1, s2):
    """Calculate angle between detectors (phi1, phi2)"""

    x1, y1 = DETECTORS[s1 - 1][:2]
    x2, y2 = DETECTORS[s2 - 1][:2]

    return arctan2((y2 - y1), (x2 - x1))

def rel_phi_errorsq(theta, phi, phi1, phi2, r1=10, r2=10):
    # speed of light in m / ns
    c = .3

    tanphi = tan(phi)
    sinphi1 = sin(phi1)
    cosphi1 = cos(phi1)
    sinphi2 = sin(phi2)
    cosphi2 = cos(phi2)

    den = ((1 + tanphi ** 2) ** 2 * r1 ** 2 * r2 ** 2 * sin(theta) ** 2
       * (sinphi1 * cos(phi - phi2) - sinphi2 * cos(phi - phi1)) ** 2
       / c ** 2)

    A = (r1 ** 2 * sinphi1 ** 2
         + r2 ** 2 * sinphi2 ** 2
         - r1 * r2 * sinphi1 * sinphi2)
    B = (2 * r1 ** 2 * sinphi1 * cosphi1
         + 2 * r2 ** 2 * sinphi2 * cosphi2
         - r1 * r2 * sinphi2 * cosphi1
         - r1 * r2 * sinphi1 * cosphi2)
    C = (r1 ** 2 * cosphi1 ** 2
         + r2 ** 2 * cosphi2 ** 2
         - r1 * r2 * cosphi1 * cosphi2)

    return 2 * (A * tanphi ** 2 + B * tanphi + C) / den

def dphi_dt0(theta, phi, phi1, phi2, r1=10, r2=10):
    # speed of light in m / ns
    c = .3

    tanphi = tan(phi)
    sinphi1 = sin(phi1)
    cosphi1 = cos(phi1)
    sinphi2 = sin(phi2)
    cosphi2 = cos(phi2)

    den = ((1 + tanphi ** 2) * r1 * r2 * sin(theta)
           * (sinphi2 * cos(phi - phi1) - sinphi1 * cos(phi - phi2))
           / c)
    num = (r2 * cosphi2 - r1 * cosphi1
           + tanphi * (r2 * sinphi2 - r1 * sinphi1))

    return num / den

def dphi_dt1(theta, phi, phi1, phi2, r1=10, r2=10):
    # speed of light in m / ns
    c = .3

    tanphi = tan(phi)
    sinphi1 = sin(phi1)
    cosphi1 = cos(phi1)
    sinphi2 = sin(phi2)
    cosphi2 = cos(phi2)

    den = ((1 + tanphi ** 2) * r1 * r2 * sin(theta)
           * (sinphi2 * cos(phi - phi1) - sinphi1 * cos(phi - phi2))
           / c)
    num = -r2 * (sinphi2 * tanphi + cosphi2)

    return num / den

def dphi_dt2(theta, phi, phi1, phi2, r1=10, r2=10):
    # speed of light in m / ns
    c = .3

    tanphi = tan(phi)
    sinphi1 = sin(phi1)
    cosphi1 = cos(phi1)
    sinphi2 = sin(phi2)
    cosphi2 = cos(phi2)

    den = ((1 + tanphi ** 2) * r1 * r2 * sin(theta)
           * (sinphi2 * cos(phi - phi1) - sinphi1 * cos(phi - phi2))
           / c)
    num = r1 * (sinphi1 * tanphi + cosphi1)

    return num / den

def rel_theta_errorsq(theta, phi, phi1, phi2, r1=10, r2=10):
    e1 = rel_theta1_errorsq(theta, phi, phi1, phi2, r1, r2)
    e2 = rel_theta2_errorsq(theta, phi, phi1, phi2, r1, r2)

    #return minimum(e1, e2)
    return e1

def rel_theta1_errorsq(theta, phi, phi1, phi2, r1=10, r2=10):
    # speed of light in m / ns
    c = .3

    sintheta = sin(theta)
    sinphiphi1 = sin(phi - phi1)

    den = (1 - sintheta ** 2) * r1 ** 2 * cos(phi - phi1) ** 2

    A = (r1 ** 2 * sinphiphi1 ** 2
         * rel_phi_errorsq(theta, phi, phi1, phi2, r1, r2))
    B = (r1 * c * sinphiphi1
         * (dphi_dt0(theta, phi, phi1, phi2, r1, r2)
            - dphi_dt1(theta, phi, phi1, phi2, r1, r2)))
    C = 2 * c ** 2

    errsq = (A * sintheta ** 2 - 2 * B * sintheta + C) / den

    return where(isnan(errsq), inf, errsq)

def rel_theta2_errorsq(theta, phi, phi1, phi2, r1=10, r2=10):
    # speed of light in m / ns
    c = .3

    sintheta = sin(theta)
    sinphiphi2 = sin(phi - phi2)

    den = (1 - sintheta ** 2) * r2 ** 2 * cos(phi - phi2) ** 2

    A = (r2 ** 2 * sinphiphi2 ** 2
         * rel_phi_errorsq(theta, phi, phi1, phi2, r1, r2))
    B = (r2 * c * sinphiphi2
         * (dphi_dt0(theta, phi, phi1, phi2, r1, r2)
            - dphi_dt2(theta, phi, phi1, phi2, r1, r2)))
    C = 2 * c ** 2

    errsq = (A * sintheta ** 2 - 2 * B * sintheta + C) / den

    return where(isnan(errsq), inf, errsq)

def do_full_reconstruction(data, tablename):
    """Do a reconstruction of all simulated data and store results"""

    try:
        data.createGroup('/', 'reconstructions', "Angle reconstructions")
    except tables.NodeError:
        pass

    try:
        data.removeNode('/reconstructions', tablename)
    except tables.NoSuchNodeError:
        pass

    table = data.createTable('/reconstructions', tablename,
                             ReconstructedEvent, "Reconstruction data")

    # zenith 0 degrees
    kwargs = dict(data=data, tablename='angle_0', THETA=0, dest=table)
    reconstruct_angles(D=1, **kwargs)

    # zenith 5 degrees, D=1,2,3,4,5
    kwargs = dict(data=data, tablename='angle_5', THETA=deg2rad(5),
                  dest=table)
    reconstruct_angles(D=1, **kwargs)

    # zenith 22.5 degrees, D=1,2,3,4,5
    kwargs = dict(data=data, tablename='angle_23', THETA=pi / 8,
                  dest=table)
    reconstruct_angles(D=1, **kwargs)

    # zenith 35 degrees, D=1,2,3,4,5
    kwargs = dict(data=data, tablename='angle_35', THETA=deg2rad(35),
                  dest=table)
    reconstruct_angles(D=1, **kwargs)

    # SPECIALS
    # zenith 22.5, sizes=5,20, D=1,2,3,4,5
    kwargs = dict(data=data, THETA=pi / 8, dest=table)
    reconstruct_angles(tablename='angle_23_size5', D=1, **kwargs)
    reconstruct_angles(tablename='angle_23_size20', D=1, **kwargs)

    # zenith 22.5, binnings, D=1,2,3,4,5
    kwargs = dict(data=data, tablename='angle_23', THETA=pi / 8,
                  dest=table)
    kwargs['randomize_binning'] = False
    reconstruct_angles(binning=1, D=1, **kwargs)
    reconstruct_angles(binning=2.5, D=1, **kwargs)
    reconstruct_angles(binning=5, D=1, **kwargs)
    kwargs['randomize_binning'] = True
    reconstruct_angles(binning=1, D=1, **kwargs)
    reconstruct_angles(binning=2.5, D=1, **kwargs)
    reconstruct_angles(binning=5, D=1, **kwargs)

def reconstruct_angles(data, tablename, dest, THETA, D, binning=False,
                       randomize_binning=False, N=None):
    """Reconstruct angles from simulation for minimum particle density"""

    match = re.search('_size([0-9]+)', tablename)
    if match:
        R = int(match.group(1))
    else:
        R = 10

    table = data.getNode('/analysis', tablename)
    dst_row = dest.row
    for event in table[:N]:
        if min(event['n1'], event['n3'], event['n4']) >= D:
            # Do we need to bin timing data?
            if binning is not False:
                event['t1'] = floor(event['t1'] / binning) * binning 
                event['t2'] = floor(event['t2'] / binning) * binning 
                event['t3'] = floor(event['t3'] / binning) * binning 
                event['t4'] = floor(event['t4'] / binning) * binning 
                # Do we need to randomize inside a bin?
                if randomize_binning is True:
                    event['t1'] += uniform(0, binning)
                    event['t2'] += uniform(0, binning)
                    event['t3'] += uniform(0, binning)
                    event['t4'] += uniform(0, binning)

            theta, phi = reconstruct_angle(event, R)
            alpha = event['alpha']

            if not isnan(theta) and not isnan(phi):
                ang_dist = arccos(sin(theta) * sin(THETA) *
                                  cos(phi - -alpha) + cos(theta) *
                                  cos(THETA))

                dst_row['r'] = event['r']
                dst_row['phi'] = event['phi']
                dst_row['alpha'] = alpha
                dst_row['t1'] = event['t1']
                dst_row['t2'] = event['t2']
                dst_row['t3'] = event['t3']
                dst_row['t4'] = event['t4']
                dst_row['n1'] = event['n1']
                dst_row['n2'] = event['n2']
                dst_row['n3'] = event['n3']
                dst_row['n4'] = event['n4']
                dst_row['sim_theta'] = THETA
                dst_row['sim_phi'] = -alpha
                dst_row['r_theta'] = theta
                dst_row['r_phi'] = phi
                dst_row['D'] = min(event['n1'], event['n3'], event['n4'])
                dst_row['size'] = R
                if binning is False:
                    bin_size = 0
                else:
                    bin_size = binning
                dst_row['bin'] = bin_size
                dst_row['bin_r'] = randomize_binning
                dst_row.append()
    dest.flush()

def do_reconstruction_plots(data, tablename):
    """Make plots based upon earlier reconstructions"""

    table = data.getNode('/reconstructions', tablename)

    #plot_uncertainty_mip(table)
    plot_uncertainty_zenith(table)
    #plot_uncertainty_size(table)
    #plot_uncertainty_binsize(table)

def plot_uncertainty_mip(table):
    figure()
    rcParams['text.usetex'] = False
    x, y, y2 = [], [], []
    for D in range(1, 6):
        x.append(D)
        events = table.readWhere(
            '(D==%d) & (sim_theta==%.40f) & (size==10) & (bin==0)' % 
            (D, float32(pi / 8)))
        print len(events),
        errors = events['sim_theta'] - events['r_theta']
        # Make sure -pi < errors < pi
        errors = (errors + pi) % (2 * pi) - pi
        errors2 = events['sim_phi'] - events['r_phi']
        # Make sure -pi < errors2 < pi
        errors2 = (errors2 + pi) % (2 * pi) - pi
        y.append(std(errors))
        y2.append(std(errors2))
    plot(x, rad2deg(y), '^', label="Theta")
    plot(x, rad2deg(y2), 'v', label="Phi")
    xlabel("Minimum number of particles")
    ylabel("Uncertainty in angle reconstruction (deg)")
    title(r"$\theta = 22.5^\circ$")
    legend(frameon=False)
    if USE_TEX:
        rcParams['text.usetex'] = True
    savefig('plots/auto-results-MIP.pdf')
    print

def plot_uncertainty_zenith(table):
    # constants for uncertainty estimation
    phi1 = calc_phi(1, 3)
    phi2 = calc_phi(1, 4)

    figure()
    rcParams['text.usetex'] = False
    x, y, y2 = [], [], []
    for THETA in [0, deg2rad(5), pi / 8, deg2rad(35)]:
        x.append(THETA)
        events = table.readWhere(
            '(D>=2) & (sim_theta==%.40f) & (size==10) & (bin==0)' % 
            float32(THETA))
        print len(events),
        errors = events['sim_theta'] - events['r_theta']
        # Make sure -pi < errors < pi
        errors = (errors + pi) % (2 * pi) - pi
        errors2 = events['sim_phi'] - events['r_phi']
        # Make sure -pi < errors2 < pi
        errors2 = (errors2 + pi) % (2 * pi) - pi
        y.append(std(errors))
        y2.append(std(errors2))
    plot(rad2deg(x), rad2deg(y), '^', label="Theta")
    # Azimuthal angle undefined for zenith = 0
    plot(rad2deg(x[1:]), rad2deg(y2[1:]), 'v', label="Phi")
    # Uncertainty estimate
    x = linspace(0, deg2rad(80), 50)
    phis = linspace(-pi, pi, 50)
    y, y2, y3 = [], [], []
    for t in x:
        y.append(mean(rel_phi_errorsq(t, phis, phi1, phi2)))
        y2.append(mean(rel_theta_errorsq(t, phis, phi1, phi2)))
    y = TIMING_ERROR * sqrt(array(y))
    y2 = TIMING_ERROR * sqrt(array(y2))
    plot(rad2deg(x), rad2deg(y), label="Estimate Phi")
    plot(rad2deg(x), rad2deg(y2), label="Estimate Theta")
    # Labels etc.
    xlabel("Shower zenith angle (degrees)")
    ylabel("Uncertainty in angle reconstruction (deg)")
    title(r"$N_{MIP} \geq 2$")
    legend(frameon=False)
    ylim(0, 60)
    if USE_TEX:
        rcParams['text.usetex'] = True
    savefig('plots/auto-results-zenith.pdf')
    print

def plot_uncertainty_size(table):
    # constants for uncertainty estimation
    phi1 = calc_phi(1, 3)
    phi2 = calc_phi(1, 4)

    figure()
    rcParams['text.usetex'] = False
    x, y, y2 = [], [], []
    for size in [5, 10, 20]:
        x.append(size)
        events = table.readWhere(
            '(D>=2) & (sim_theta==%.40f) & (size==%d) & (bin==0)' %
            (float32(pi/ 8), size))
        print len(events),
        errors = events['sim_theta'] - events['r_theta']
        # Make sure -pi < errors < pi
        errors = (errors + pi) % (2 * pi) - pi
        errors2 = events['sim_phi'] - events['r_phi']
        # Make sure -pi < errors2 < pi
        errors2 = (errors2 + pi) % (2 * pi) - pi
        y.append(std(errors))
        y2.append(std(errors2))
    plot(x, rad2deg(y), '^', label="Theta")
    plot(x, rad2deg(y2), 'v', label="Phi")
    # Uncertainty estimate
    x = linspace(5, 20, 50)
    phis = linspace(-pi, pi, 50)
    y, y2 = [], []
    for s in x:
        y.append(mean(rel_phi_errorsq(pi / 8, phis, phi1, phi2, r1=s, r2=s)))
        y2.append(mean(rel_theta_errorsq(pi / 8, phis, phi1, phi2, r1=s, r2=s)))
    y = TIMING_ERROR * sqrt(array(y))
    y2 = TIMING_ERROR * sqrt(array(y2))
    plot(x, rad2deg(y), label="Estimate Phi")
    plot(x, rad2deg(y2), label="Estimate Theta")
    # Labels etc.
    xlabel("Station size (m)")
    ylabel("Uncertainty in angle reconstruction (deg)")
    title(r"$\theta = 22.5^\circ, N_{MIP} \geq 2$")
    legend(frameon=False)
    if USE_TEX:
        rcParams['text.usetex'] = True
    savefig('plots/auto-results-size.pdf')
    print

def plot_uncertainty_binsize(table):
    # constants for uncertainty estimation
    phi1 = calc_phi(1, 3)
    phi2 = calc_phi(1, 4)

    figure()
    rcParams['text.usetex'] = False
    x, y, y2 = [], [], []
    for bin_size in [0, 1, 2.5, 5]:
        if bin_size == 0:
            is_randomized = False
        else:
            is_randomized = True
        x.append(bin_size)
        events = table.readWhere(
            '(D>=2) & (sim_theta==%.40f) & (size==10) & (bin==%.40f) & '
            '(bin_r==%s)' %
            (float32(pi/ 8), bin_size, is_randomized))
        print len(events),
        errors = events['sim_theta'] - events['r_theta']
        # Make sure -pi < errors < pi
        errors = (errors + pi) % (2 * pi) - pi
        errors2 = events['sim_phi'] - events['r_phi']
        # Make sure -pi < errors2 < pi
        errors2 = (errors2 + pi) % (2 * pi) - pi
        y.append(std(errors))
        y2.append(std(errors2))
    plot(x, rad2deg(y), '^', label="Theta")
    plot(x, rad2deg(y2), 'v', label="Phi")
    # Uncertainty estimate
    x = linspace(0, 5, 50)
    phis = linspace(-pi, pi, 50)
    y, y2 = [], []
    phi_errorsq = mean(rel_phi_errorsq(pi / 8, phis, phi1, phi2))
    theta_errorsq = mean(rel_theta_errorsq(pi / 8, phis, phi1, phi2))
    for t in x:
        y.append(sqrt((TIMING_ERROR ** 2 + t ** 2 / 12) * phi_errorsq))
        y2.append(sqrt((TIMING_ERROR ** 2 + t ** 2 / 12) * theta_errorsq))
    y = array(y)
    y2 = array(y2)
    plot(x, rad2deg(y), label="Estimate Phi")
    plot(x, rad2deg(y2), label="Estimate Theta")
    # Labels etc.
    xlabel("Bin size (ns)")
    ylabel("Uncertainty in angle reconstruction (deg)")
    title(r"$\theta = 22.5^\circ, N_{MIP} \geq 2$")
    legend(loc='best', frameon=False)
    ylim(ymin=0)
    if USE_TEX:
        rcParams['text.usetex'] = True
    savefig('plots/auto-results-binsize.pdf')
    print

def plot_ring_arrival_times(data, tablename, theta):
    s = data.getNode('/showers', tablename)
    bins = [0, 4, 20, 40, 80, sqrt(2 * 80 ** 2)]
    phi_bins = linspace(-pi, pi, 17)

    figure()
    T, E = [], []
    for a, b in zip(bins[:-1], bins[1:]):
        t, e = [], []
        for pa, pb in zip(phi_bins[:-1], phi_bins[1:]):
            p = s.electrons.readWhere('(%f <= core_distance) & '
                                      '(core_distance < %f) &'
                                      '(%f <= polar_angle) &'
                                      '(polar_angle < %f)' % (a, b, pa,
                                                              pb))
            t.append(p['arrival_time'] - min(p['arrival_time']))
            e.append(p['energy'])
        T.append([x for sublist in t for x in sublist])
        T.append([x for sublist in e for x in sublist])
    
    for a, b, t in zip(bins[:-1], bins[1:], T):
        hist(t, bins=linspace(0, 100, 200), histtype='step',
             label='%d < r < %d' % (a, b), log=True, normed=True)

    xlabel("Arrival time (ns)")
    ylabel("Normed count")
    title(r"$\theta=%s^\circ$" % theta)
    legend()
    ylim(ymin=1e-4)


if __name__ == '__main__':
    # invalid values in arcsin will be ignored (nan handles the situation
    # quite well)
    np.seterr(invalid='ignore')

    try:
        data
    except NameError:
        data = tables.openFile('data-e15.h5', 'a')

    #do_full_reconstruction(data, 'full')
    do_reconstruction_plots(data, 'full')
    #plot_ring_arrival_times(data, 'zenith0', 0)
    #plot_ring_arrival_times(data, 'zenith5', 5)
    #plot_ring_arrival_times(data, 'zenith23', 22.5)
    #plot_ring_arrival_times(data, 'zenith35', 35)
