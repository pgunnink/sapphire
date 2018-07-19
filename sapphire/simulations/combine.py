import tables
from ..utils import pbar


DEFAULT_TABLES = ['events']
def combine_simulations(simulations, station_path, output_file,
                        copy_tables=DEFAULT_TABLES, verbose=False, progress=False):
    """
    Combines the result of multiple results from a simulation
    :param simulations: list of paths to simulation files
    :param station_path: the station path in the h5 file to copy (for now only 1
    station is supported)
    :param output_file: the output file
    :param copy_tables: the tables in the file to copy (relative to the station_path)
    :param verbose: print debugging information
    :param progress: show progress
    :return:
    """
    if progress:
        print('Creating tables')
    tables.copy_file(simulations[0], output_file, overwrite=True)
    with tables.open_file(output_file, 'a') as output:
        if progress:
            iterator = pbar(simulations[1:])
        else:
            iterator = simulations[1:]
        for sim in iterator:
            with tables.open_file(sim, 'r') as data:
                if station_path not in data:
                    if verbose:
                        print('%s not populated' % sim)
                    continue
                for to_copy in copy_tables:
                    path = tables.path.join_path(station_path, to_copy)
                    copied_table = data.get_node(path)
                    to_table = output.get_node(path)
                    row = to_table.row
                    length_table = len(to_table)
                    for event in copied_table:
                        for key in copied_table.colnames:
                            if key=='event_id':
                                row[key] = length_table+event['event_id']
                            else:
                                row[key] = event[key]
                        row.append()
                    to_table.flush()