import os
import h5py
import numpy as np


class Read:
    """
    An iterator on the MillionSongSubset data folder. It calls parse_file on all the elements of the data folder
    """
    def __init__(self, directory, key=None, max_size=np.inf, print_progress=False):
        """
        The iterator can return two types of data for one element el:
        - a dictionary representation of el, containing all of its components
        - a tuple (id, value). This is the case only if key is specified
        :param directory: the path to the MillionSongSubset data folder
        :param key: the key to us for returning the (id, value) tuples
        :param max_size: when max_size files have been read, the iterator stops (for testing)
        :param print_progress: if True, the number of processed files is printed on the terminal
        """
        self.directory = directory
        self.max_size = max_size
        self.key = key
        self.print_progress = print_progress

    def __iter__(self):
        i = 0
        for root, dirs, files in os.walk(self.directory):
            for f in files:
                data = parse_file(
                    os.path.join(root, f),
                    f.split('.')[0]
                )
                if self.key is not None:
                    yield (data['id'], data[self.key])
                else:
                    yield data
                i += 1
                if i == self.max_size:
                    break

                if self.print_progress and i % 100 == 0:
                    print('Progress: ' + str(i))

            if i == self.max_size:
                break


def parse_file(h5py_path, id):
    """

    :param h5py_path: path to the h5py file
    :param id: the id of the corresponding h5py object (the name of the file without the ending .h5)
    :return: a dictionary representation of the element
    """
    h5py_object = h5py.File(h5py_path)
    data = _parse_file(h5py_object)
    out = {'.'.join(x[0]): x[1] for x in data}
    out['id'] = id
    return out


def _parse_file(h5py_object, root_name=[]):
    """
    A helper function to parse_file
    :param h5py_object:
    :param root_name:
    :return: a dictionary representation of the element
    """
    if isinstance(h5py_object, h5py.Dataset):
        if len(h5py_object.shape) == 1:
            return [(root_name, np.array(h5py_object))]
        else:
            return [(root_name, np.matrix(h5py_object))]

    out = []
    for key in h5py_object.iterkeys():
        for tuple in _parse_file(h5py_object[key], root_name + [key]):
            out.append(tuple)

    return out
