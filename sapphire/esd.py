""" Fetch events and other data from the event summary data (ESD).

    This module enables you to access the event summary data.

    If you are in a real hurry and know what you're doing (and took the
    time to read this far), you can call the :func:`quick_download`
    function like this::

        >>> from sapphire import quick_download
        >>> data = quick_download(501)

    For regular use, look up :func:`download_data`.

"""
import calendar
import collections
import csv
import datetime
import itertools
import os.path
import re
import time

from codecs import iterdecode

import tables

from progressbar import ETA, Bar, Percentage, ProgressBar
from six import itervalues
from six.moves.http_client import BadStatusLine
from six.moves.urllib.parse import urlencode
from six.moves.urllib.request import urlopen

from . import api, storage

BASE = 'http://data.hisparc.nl/data/'
EVENTS_URL = BASE + '{station_number:d}/events/?{query}'
WEATHER_URL = BASE + '{station_number:d}/weather/?{query}'
SINGLES_URL = BASE + '{station_number:d}/singles/?{query}'
LIGHTNING_URL = BASE + 'knmi/lightning/{lightning_type:d}/?{query}'
COINCIDENCES_URL = BASE + 'network/coincidences/?{query}'


def quick_download(station_number, date=None):
    """Quickly download some data

    :param station_number: The HiSPARC station number.
    :param date: the date for which to get data (datetime.datetime
        instance), passed unchanged to the :func:`download_data` as
        :param start:.
    :return: handle to an open PyTables file.

    Everything is handled by this function, including file creation.
    Expect no frills: you just get yesterday's data.

    Example usage::

        >>> import sapphire.esd
        >>> data = sapphire.esd.quick_download(501)
        >>> print data
        data1.h5 (File) u''
        Last modif.: 'Mon Jun  9 22:03:50 2014'
        Object Tree:
        / (RootGroup) u''
        /s501 (Group) u''
        /s501/events (Table(58898,)) ''

    """
    path = _first_available_numbered_path()
    data = tables.open_file(path, 'w')
    download_data(data, None, station_number, date)
    return data


def _first_available_numbered_path():
    """Find first available file name in sequence

    If data1.h5 is taken, return data2.h5, etc.

    """
    path = 'data%d.h5'
    return next(path % idx for idx in itertools.count(start=1)
                if not os.path.exists(path % idx))


def load_data(file, group, tsv_file, type='events'):
    """Load downloaded event summary data into PyTables file.

    If you've previously downloaded event summary data from
    http://data.hisparc.nl/ in TSV format, you can load them into a PyTables
    file using this method. The result is equal to directly downloading data
    using :func:`download_data`.

    :param file: the PyTables datafile handler.
    :param group: the PyTables destination group, which need not exist.
    :param tsv_file: path to the tsv file downloaded from the HiSPARC
                     Public Database.
    :param type: the datatype to load, either 'events', 'weather',
                 'singles' or 'lightning'.

    Example::

        >>> import tables
        >>> import sapphire.esd
        >>> data = tables.open_file('data.h5', 'w')
        >>> sapphire.esd.load_data(data, '/s501', 'events-s501-20130910.tsv')

    """
    if type == 'events':
        table = _get_or_create_events_table(file, group)
        read_and_store_class = _read_line_and_store_event_class
    elif type == 'weather':
        table = _get_or_create_weather_table(file, group)
        read_and_store_class = _read_line_and_store_weather_class
    elif type == 'singles':
        table = _get_or_create_singles_table(file, group)
        read_and_store_class = _read_line_and_store_singles_class
    elif type == 'lightning':
        table = _get_or_create_lightning_table(file, group)
        read_and_store_class = _read_line_and_store_lightning_class
    else:
        raise ValueError("Data type not recognized.")

    with open(tsv_file, 'rb') as data:
        reader = csv.reader(iterdecode(data, 'utf-8'), delimiter='\t')
        with read_and_store_class(table) as writer:
            for line in reader:
                writer.store_line(line)


def download_data(file, group, station_number, start=None, end=None,
                  type='events', progress=True):
    """Download event summary data

    :param file: the PyTables datafile handler.
    :param group: the PyTables destination group, which need not exist.
    :param station_number: The HiSPARC station number for which to get data.
    :param start: a datetime instance defining the start of the search
        interval.
    :param end: a datetime instance defining the end of the search
        interval.
    :param type: the datatype to download, either 'events', 'weather',
        or 'singles'.
    :param progress: if True show a progressbar while downloading.

    If group is None, use '/s<station_number>' as a default.

    The start and stop parameters may both be None.  In that case,
    yesterday's data is downloaded.  If only end is None, a single day's
    worth of data is downloaded, starting at the datetime specified with
    start.

    Example::

        >>> import tables
        >>> import datetime
        >>> import sapphire.esd
        >>> data = tables.open_file('data.h5', 'w')
        >>> sapphire.esd.download_data(data, '/s501', 501,
        ...     datetime.datetime(2013, 9, 1), datetime.datetime(2013, 9, 2))

    """
    # sensible default for group name
    if group is None:
        group = '/s%d' % station_number

    # sensible defaults for start and end
    if start is None:
        if end is not None:
            raise RuntimeError("Start is None, but end is not. "
                               "I can't go on like this.")
        else:
            yesterday = datetime.date.today() - datetime.timedelta(days=1)
            start = datetime.datetime.combine(yesterday, datetime.time(0, 0))
    if end is None:
        end = start + datetime.timedelta(days=1)

    # build and open url, create tables and set read function
    query = urlencode({'start': start, 'end': end})
    if type == 'events':
        url = EVENTS_URL.format(station_number=station_number, query=query)
        table = _get_or_create_events_table(file, group)
        read_and_store = _read_line_and_store_event_class
    elif type == 'weather':
        url = WEATHER_URL.format(station_number=station_number, query=query)
        table = _get_or_create_weather_table(file, group)
        read_and_store = _read_line_and_store_weather_class
    elif type == 'singles':
        url = SINGLES_URL.format(station_number=station_number, query=query)
        table = _get_or_create_singles_table(file, group)
        read_and_store = _read_line_and_store_singles_class
    elif type == 'lightning':
        url = LIGHTNING_URL.format(lightning_type=station_number, query=query)
        table = _get_or_create_lightning_table(file, group)
        read_and_store = _read_line_and_store_lightning_class
    else:
        raise ValueError("Data type not recognized.")

    try:
        data = urlopen(url)
    except BadStatusLine:
        # Unexplained transient error, retry once
        data = urlopen(url)

    # keep track of event timestamp within [start, end] interval for
    # progressbar
    t_start = calendar.timegm(start.utctimetuple())
    t_end = calendar.timegm(end.utctimetuple())
    t_delta = t_end - t_start
    if progress:
        pbar = ProgressBar(max_value=1.,
                           widgets=[Percentage(), Bar(), ETA()]).start()

    # loop over lines in tsv as they come streaming in
    prev_update = time.time()
    reader = csv.reader(iterdecode(data, 'utf-8'), delimiter='\t')
    with read_and_store(table) as writer:
        for line in reader:
            timestamp = writer.store_line(line)
            # update progressbar every 0.5 seconds
            if (progress and time.time() - prev_update > 0.5 and
                    not timestamp == 0.):
                pbar.update((1. * timestamp - t_start) / t_delta)
                prev_update = time.time()
    if progress:
        pbar.finish()

    if line[0][0] == '#':
        if len(line[0]) == 1:
            # No events recieved, and no success line
            raise Exception('Failed to download data, no data recieved.')
        else:
            # Successful download because last line is a non-empty comment
            return
    else:
        # Last line is data, report failed download and date/time of last line
        raise Exception('Failed to complete download, last received data '
                        'from: %s %s.' % tuple(line[:2]))


def download_lightning(file, group, lightning_type=4, start=None, end=None,
                       progress=True):
    """Download KNMI lightning data

    :param file: the PyTables datafile handler.
    :param group: the PyTables destination group, which need not exist.
    :param lightning_type: type of lightning, see list below for the possible
                           type codes.
    :param start: a datetime instance defining the start of the search
        interval.
    :param end: a datetime instance defining the end of the search
        interval.
    :param progress: if True show a progressbar while downloading.

    Lightning types:

        0 - Single-point
        1 - Cloud-cloud
        2 - Cloud-cloud mid
        3 - Cloud-cloud end
        4 - Cloud-ground (default)
        5 - Cloud-ground return

    """
    # sensible default for group name
    if group is None:
        group = '/l%d' % lightning_type

    if lightning_type not in range(6):
        raise ValueError("Invalid lightning type.")

    download_data(file, group, lightning_type, start=start, end=end,
                  type='lightning', progress=progress)


def load_coincidences(file, tsv_file, group=''):
    """Load downloaded event summary data into PyTables file.

    If you've previously downloaded coincidence data from
    http://data.hisparc.nl/ in TSV format, you can load them into a PyTables
    file using this method. The result is equal to directly downloading data
    using :func:`download_coincidences`.

    :param file: the PyTables datafile handler.
    :param tsv_file: path to the tsv file downloaded from the HiSPARC
                     Public Database containing coincidences.
    :param group: the PyTables destination group, which need not exist.

    Example::

        >>> import tables
        >>> import sapphire.esd
        >>> data = tables.open_file('coincidences.h5', 'w')
        >>> sapphire.esd.load_coincidences(data, 'coincidences-20151130.tsv')

    """
    station_groups = _read_or_get_station_groups(file, group)
    c_group = _get_or_create_coincidences_tables(file, group, station_groups)

    with open(tsv_file, 'rb') as data:
        # loop over lines in tsv as they come streaming in, keep temporary
        # lists until a full coincidence is in.
        reader = csv.reader(iterdecode(data, 'utf-8'), delimiter='\t')
        current_coincidence = 0
        coincidence = []
        for line in reader:
            if line[0][0] == '#':
                continue
            elif int(line[0]) == current_coincidence:
                coincidence.append(line)
            else:
                # Full coincidence has been received, store it.
                _read_lines_and_store_coincidence(file, c_group,
                                                  coincidence, station_groups)
                coincidence = [line]
                current_coincidence = int(line[0])
                file.flush()

        if len(coincidence):
            # Store last coincidence
            _read_lines_and_store_coincidence(file, c_group, coincidence,
                                              station_groups)

        if line[0][0] == '#':
            if len(line[0]) == 1:
                # No events to load, and no success line
                raise Exception('No data to load, source contains no data.')
            else:
                # Successful download because last line is a non-empty comment
                pass
        else:
            # Last line is data, report possible fail and last date/time
            raise Exception('Source file seems incomplete, last received data '
                            'from: %s %s.' % tuple(line[2:4]))

        file.flush()


def download_coincidences(file, group='', cluster=None, stations=None,
                          start=None, end=None, n=2, progress=True):
    """Download event summary data coincidences

    :param file: PyTables datafile handler.
    :param group: path of destination group, which need not exist yet.
    :param cluster: HiSPARC cluster name for which to get data.
    :param stations: a list of HiSPARC station numbers for which to get data.
    :param start: a datetime instance defining the start of the search
        interval.
    :param end: a datetime instance defining the end of the search
        interval.
    :param n: the minimum number of events in the coincidence.

    The start and end parameters may both be None.  In that case,
    yesterday's data is downloaded.  If only end is None, a single day's
    worth of data is downloaded, starting at the datetime specified with
    start.

    Optionally either a cluster or stations can be defined to limit the
    results to include only events from those stations.

    Example::

        >>> import tables
        >>> import datetime
        >>> import sapphire.esd
        >>> data = tables.open_file('data_coincidences.h5', 'w')
        >>> sapphire.esd.download_coincidences(data, cluster='Aarhus',
        ...     start=datetime.datetime(2013, 9, 1),
        ...     end=datetime.datetime(2013, 9, 2), n=3)

    """
    # sensible defaults for start and end
    if start is None:
        if end is not None:
            raise RuntimeError("Start is None, but end is not. "
                               "I can't go on like this.")
        else:
            yesterday = datetime.date.today() - datetime.timedelta(days=1)
            start = datetime.datetime.combine(yesterday, datetime.time(0, 0))
    if end is None:
        end = start + datetime.timedelta(days=1)

    if stations is not None and len(stations) < n:
        raise Exception('To few stations in query, give at least n.')

    # build and open url, create tables and set read function
    query = urlencode({'cluster': cluster, 'stations': stations,
                       'start': start, 'end': end, 'n': n})
    url = COINCIDENCES_URL.format(query=query)
    station_groups = _read_or_get_station_groups(file, group)
    c_group = _get_or_create_coincidences_tables(file, group, station_groups)

    try:
        data = urlopen(url, timeout=1800)
    except BadStatusLine:
        # Unexplained transient error, retry once
        data = urlopen(url, timeout=1800)

    # keep track of event timestamp within [start, end] interval for
    # progressbar
    t_start = calendar.timegm(start.utctimetuple())
    t_end = calendar.timegm(end.utctimetuple())
    t_delta = t_end - t_start
    if progress:
        pbar = ProgressBar(max_value=1.,
                           widgets=[Percentage(), Bar(), ETA()]).start()

    # loop over lines in tsv as they come streaming in, keep temporary
    # lists until a full coincidence is in.
    prev_update = time.time()
    reader = csv.reader(iterdecode(data, 'utf-8'), delimiter='\t')
    current_coincidence = 0
    coincidence = []
    for line in reader:
        if line[0][0] == '#':
            continue
        elif int(line[0]) == current_coincidence:
            coincidence.append(line)
        else:
            # Full coincidence has been received, store it.
            timestamp = _read_lines_and_store_coincidence(file, c_group,
                                                          coincidence,
                                                          station_groups)
            # update progressbar every 0.5 seconds
            if (progress and time.time() - prev_update > 0.5 and
                    not timestamp == 0.):
                pbar.update((1. * timestamp - t_start) / t_delta)
                prev_update = time.time()
            coincidence = [line]
            current_coincidence = int(line[0])
            file.flush()

    if len(coincidence):
        # Store last coincidence
        _read_lines_and_store_coincidence(file, c_group, coincidence,
                                          station_groups)
    if progress:
        pbar.finish()

    if line[0][0] == '#':
        if len(line[0]) == 1:
            # No events recieved, and no success line
            raise Exception('Failed to download data, no data recieved.')
        else:
            # Successful download because last line is a non-empty comment
            pass
    else:
        # Last line is data, report failed download and date/time of last line
        raise Exception('Failed to complete download, last received data '
                        'from: %s %s.' % tuple(line[2:4]))

    file.flush()


def _read_or_get_station_groups(file, group):
    """Get station numbers from existing cluster attribute or a new set

    :param file: PyTables datafile handler.
    :param group: path of destination group.
    :return: existing or newly generated station group paths with station
             numbers and ids.

    """
    try:
        s_index = file.get_node(group + '/coincidences', 's_index').read()
    except tables.NoSuchNodeError:
        return _get_station_groups(group)
    else:
        re_number = re.compile('[0-9]+$')
        groups = collections.OrderedDict()
        for sid, station_group in enumerate(s_index):
            station_group = station_group.decode()
            station = int(re_number.search(station_group).group())
            groups[station] = {'group': station_group,
                               's_index': sid}
        return groups


def _get_station_groups(group):
    """Generate groups names for all stations

    :param group: path of destination group.

    Use the same hierarchy (cluster/station) as used in the HiSPARC
    datastore.

    """
    groups = collections.OrderedDict()
    network = api.Network()
    clusters = network.clusters()
    s_index = 0
    for cluster in clusters:
        stations = network.station_numbers(cluster=cluster['number'])
        for station in stations:
            groups[station] = {'group': ('%s/hisparc/cluster_%s/station_%d' %
                                         (group, cluster['name'].lower(),
                                          station)),
                               's_index': s_index}
            s_index += 1
    return groups


def _get_or_create_coincidences_tables(file, group, station_groups):
    """Get or create event table in PyTables file

    :return: the existing or created coincidences group.

    """
    try:
        return file.get_node(group + '/', 'coincidences')
    except tables.NoSuchNodeError:
        return _create_coincidences_tables(file, group, station_groups)


def _create_coincidences_tables(file, group, station_groups):
    """Setup coincidence tables

    :return: the created coincidences group.

    """
    coin_group = group + '/coincidences'

    # Create coincidences table
    description = storage.Coincidence
    s_columns = {'s%d' % station: tables.BoolCol(pos=p)
                 for p, station in enumerate(station_groups, 12)}
    description.columns.update(s_columns)
    coincidences = file.create_table(coin_group, 'coincidences', description,
                                     createparents=True)

    # Create c_index
    file.create_vlarray(coin_group, 'c_index', tables.UInt32Col(shape=2))

    # Create and fill s_index
    s_index = file.create_vlarray(coin_group, 's_index', tables.VLStringAtom())
    for station_group in itervalues(station_groups):
        s_index.append(station_group['group'].encode('utf-8'))

    return coincidences._v_parent


def _get_or_create_events_table(file, group):
    """Get or create event table in PyTables file"""

    try:
        return file.get_node(group, 'events')
    except tables.NoSuchNodeError:
        return _create_events_table(file, group)


def _create_events_table(file, group):
    """Create event table in PyTables file

    Create an event table containing the ESD data columns which are
    available in the TSV download.

    :param file: PyTables file.
    :param group: the group to contain the events table, which need not
                  exist.

    """
    description = {'event_id': tables.UInt32Col(pos=0),
                   'timestamp': tables.Time32Col(pos=1),
                   'nanoseconds': tables.UInt32Col(pos=2),
                   'ext_timestamp': tables.UInt64Col(pos=3),
                   'pulseheights': tables.Int16Col(pos=4, shape=4),
                   'integrals': tables.Int32Col(pos=5, shape=4),
                   'n1': tables.Float32Col(pos=6),
                   'n2': tables.Float32Col(pos=7),
                   'n3': tables.Float32Col(pos=8),
                   'n4': tables.Float32Col(pos=9),
                   't1': tables.Float32Col(pos=10),
                   't2': tables.Float32Col(pos=11),
                   't3': tables.Float32Col(pos=12),
                   't4': tables.Float32Col(pos=13),
                   't_trigger': tables.Float32Col(pos=14)}

    return file.create_table(group, 'events', description, createparents=True)


def _get_or_create_weather_table(file, group):
    """Get or create event table in PyTables file"""

    try:
        return file.get_node(group, 'weather')
    except tables.NoSuchNodeError:
        return _create_weather_table(file, group)


def _create_weather_table(file, group):
    """Create weather table in PyTables file

    Create a weather table containing the ESD weather columns which are
    available in the TSV download.

    :param file: PyTables file.
    :param group: the group to contain the weather table, which need not
                  exist.

    """
    description = {'event_id': tables.UInt32Col(pos=0),
                   'timestamp': tables.Time32Col(pos=1),
                   'temp_inside': tables.Float32Col(pos=2),
                   'temp_outside': tables.Float32Col(pos=3),
                   'humidity_inside': tables.Int16Col(pos=4),
                   'humidity_outside': tables.Int16Col(pos=5),
                   'barometer': tables.Float32Col(pos=6),
                   'wind_dir': tables.Int16Col(pos=7),
                   'wind_speed': tables.Int16Col(pos=8),
                   'solar_rad': tables.Int16Col(pos=9),
                   'uv': tables.Int16Col(pos=10),
                   'evapotranspiration': tables.Float32Col(pos=11),
                   'rain_rate': tables.Float32Col(pos=12),
                   'heat_index': tables.Int16Col(pos=13),
                   'dew_point': tables.Float32Col(pos=14),
                   'wind_chill': tables.Float32Col(pos=15)}

    return file.create_table(group, 'weather', description, createparents=True)


def _get_or_create_singles_table(file, group):
    """Get or create singles table in PyTables file"""

    try:
        return file.get_node(group, 'singles')
    except tables.NoSuchNodeError:
        return _create_singles_table(file, group)


def _create_singles_table(file, group):
    """Create singles table in PyTables file

    Create a singles table containing the ESD singles columns which are
    available in the TSV download.

    :param file: PyTables file.
    :param group: the group to contain the singles table, which need not
                  exist.

    """
    description = {'event_id': tables.UInt32Col(pos=0),
                   'timestamp': tables.Time32Col(pos=1),
                   'mas_ch1_low': tables.Int32Col(pos=2),
                   'mas_ch1_high': tables.Int32Col(pos=3),
                   'mas_ch2_low': tables.Int32Col(pos=4),
                   'mas_ch2_high': tables.Int32Col(pos=5),
                   'slv_ch1_low': tables.Int32Col(pos=6),
                   'slv_ch1_high': tables.Int32Col(pos=7),
                   'slv_ch2_low': tables.Int32Col(pos=8),
                   'slv_ch2_high': tables.Int32Col(pos=9)}
    return file.create_table(group, 'singles', description, createparents=True)


def _get_or_create_lightning_table(file, group):
    """Get or create lightning table in PyTables file"""

    try:
        return file.get_node(group, 'lightning')
    except tables.NoSuchNodeError:
        return _create_lightning_table(file, group)


def _create_lightning_table(file, group):
    """Create lightning table in PyTables file

    Create an lightning table containing the ESD data columns which are
    available in the TSV download.

    :param file: PyTables file.
    :param group: the group to contain the lightning table, which need not
                  exist.

    """
    description = {'event_id': tables.UInt32Col(pos=0),
                   'timestamp': tables.Time32Col(pos=1),
                   'nanoseconds': tables.UInt32Col(pos=2),
                   'ext_timestamp': tables.UInt64Col(pos=3),
                   'latitude': tables.Float32Col(pos=4),
                   'longitude': tables.Float32Col(pos=5),
                   'current': tables.Float32Col(pos=6)}

    return file.create_table(group, 'lightning', description,
                             createparents=True)


def _read_lines_and_store_coincidence(file, c_group, coincidence,
                                      station_groups):
    """Read TSV lines and store coincidence

    Read lines from the TSV download and store the coincidence and events.
    Return the coincidence timestamp to keep track of the progress.

    :param file: PyTables file for storage.
    :param c_group: the coincidences group.
    :param coincidence: text lines from the TSV file for one coincidence.
    :param station_groups: dictionary to find the path to a station_group.
    :return: coincidence timestamp.

    """
    c_idx = []
    coincidences = file.get_node(c_group, 'coincidences')
    row = coincidences.row
    row['id'] = len(coincidences)
    row['N'] = len(coincidence)
    row['timestamp'] = int(coincidence[0][4])
    row['nanoseconds'] = int(coincidence[0][5])
    row['ext_timestamp'] = (int(coincidence[0][4]) * int(1e9) +
                            int(coincidence[0][5]))

    for event in coincidence:
        station_number = int(event[1])
        try:
            row['s%d' % station_number] = True
            group_path = station_groups[station_number]['group']
        except KeyError:
            # Can not add new column, so user should make a new data file.
            raise Exception('Unexpected station number: %d, no column and/or '
                            'station group path available.' % station_number)
        event_group = _get_or_create_events_table(file, group_path)
        with _read_line_and_store_event_class(event_group) as writer:
            s_idx = station_groups[station_number]['s_index']
            e_idx = len(event_group)
            c_idx.append((s_idx, e_idx))
            writer.store_line(event[2:])

    row.append()
    c_index = file.get_node(c_group, 'c_index')
    c_index.append(c_idx)

    return int(coincidence[0][4])


class _read_line_and_store_event_class(object):

    """Store lines of event data from the ESD

    Use this contextmanager to store events from a TSV file into a PyTables
    table.

    :param table: a PyTables Table object in which to store the data.

    """

    def __init__(self, table):
        self.table = table
        self.event_counter = len(self.table)

    def __enter__(self):
        return self

    def store_line(self, line):
        """Store a single line

        :param line: the line to store as a tuple of strings (one element
                     per column).
        :return: timestamp of the stored event, or 0 if the given line was a
                 comment line starting with a '#'.

        """
        # ignore comment lines
        if line[0][0] == '#':
            return 0.

        # break up TSV line
        (date, time_str, timestamp, nanoseconds, ph1, ph2, ph3, ph4, int1,
         int2, int3, int4, n1, n2, n3, n4, t1, t2, t3, t4, t_trigger, zenith,
         azimuth) = line[:23]

        row = self.table.row

        # convert string values to correct data types or calculate values
        row['event_id'] = self.event_counter
        row['timestamp'] = int(timestamp)
        row['nanoseconds'] = int(nanoseconds)
        row['ext_timestamp'] = int(timestamp) * int(1e9) + int(nanoseconds)
        row['pulseheights'] = [int(ph1), int(ph2), int(ph3), int(ph4)]
        row['integrals'] = [int(int1), int(int2), int(int3), int(int4)]
        row['n1'] = float(n1)
        row['n2'] = float(n2)
        row['n3'] = float(n3)
        row['n4'] = float(n4)
        row['t1'] = float(t1)
        row['t2'] = float(t2)
        row['t3'] = float(t3)
        row['t4'] = float(t4)
        row['t_trigger'] = float(t_trigger)

        # store event
        row.append()

        self.event_counter += 1
        # force flush every 1e6 rows to free buffers
        if not self.event_counter % 1000000:
            self.table.flush()

        return int(timestamp)

    def __exit__(self, type, value, traceback):
        self.table.flush()


class _read_line_and_store_weather_class(_read_line_and_store_event_class):

    """Store lines of weather data from the ESD"""

    def store_line(self, line):
        # ignore comment lines
        if line[0][0] == '#':
            return 0.

        # break up TSV line
        (date, time, timestamp, temperature_inside, temperature_outside,
         humidity_inside, humidity_outside, atmospheric_pressure,
         wind_direction, wind_speed, solar_radiation, uv_index,
         evapotranspiration, rain_rate, heat_index, dew_point,
         wind_chill) = line

        row = self.table.row

        # convert string values to correct data types
        row['event_id'] = self.event_counter
        row['timestamp'] = int(timestamp)
        row['temp_inside'] = float(temperature_inside)
        row['temp_outside'] = float(temperature_outside)
        row['humidity_inside'] = int(humidity_inside)
        row['humidity_outside'] = int(humidity_outside)
        row['barometer'] = float(atmospheric_pressure)
        row['wind_dir'] = int(wind_direction)
        row['wind_speed'] = int(wind_speed)
        row['solar_rad'] = int(solar_radiation)
        row['uv'] = int(uv_index)
        row['evapotranspiration'] = float(evapotranspiration)
        row['rain_rate'] = float(rain_rate)
        row['heat_index'] = int(heat_index)
        row['dew_point'] = float(dew_point)
        row['wind_chill'] = float(wind_chill)

        # store event
        row.append()

        self.event_counter += 1
        # force flush every 1e6 rows to free buffers
        if not self.event_counter % 1000000:
            self.table.flush()

        return int(timestamp)


class _read_line_and_store_singles_class(_read_line_and_store_event_class):

    """Store lines of singles data from the ESD"""

    def store_line(self, line):
        # ignore comment lines
        if line[0][0] == '#':
            return 0.

        # break up TSV line
        (date, time, timestamp,
         mas_ch1_low, mas_ch1_high, mas_ch2_low, mas_ch2_high,
         slv_ch1_low, slv_ch1_high, slv_ch2_low, slv_ch2_high) = line

        row = self.table.row

        # convert string values to correct data types
        row['event_id'] = self.event_counter
        row['timestamp'] = int(timestamp)
        row['mas_ch1_low'] = int(mas_ch1_low)
        row['mas_ch1_high'] = int(mas_ch1_high)
        row['mas_ch2_low'] = int(mas_ch2_low)
        row['mas_ch2_high'] = int(mas_ch2_high)
        row['slv_ch1_low'] = int(slv_ch1_low)
        row['slv_ch1_high'] = int(slv_ch1_high)
        row['slv_ch2_low'] = int(slv_ch2_low)
        row['slv_ch2_high'] = int(slv_ch2_high)

        # store event
        row.append()

        self.event_counter += 1
        # force flush every 1e6 rows to free buffers
        if not self.event_counter % 1000000:
            self.table.flush()

        return int(timestamp)


class _read_line_and_store_lightning_class(_read_line_and_store_event_class):

    """Store lines of lightning data from the ESD"""

    def store_line(self, line):
        # ignore comment lines
        if line[0][0] == '#':
            return 0.

        # break up TSV line
        (date, time_str, timestamp, nanoseconds, latitude, longitude,
         current) = line[:7]

        row = self.table.row

        # convert string values to correct data types or calculate values
        row['event_id'] = self.event_counter
        row['timestamp'] = int(timestamp)
        row['nanoseconds'] = int(nanoseconds)
        row['ext_timestamp'] = int(timestamp) * int(1e9) + int(nanoseconds)
        row['latitude'] = float(latitude)
        row['longitude'] = float(longitude)
        row['current'] = float(current)

        # store event
        row.append()

        self.event_counter += 1
        # force flush every 1e6 rows to free buffers
        if not self.event_counter % 1000000:
            self.table.flush()

        return int(timestamp)
