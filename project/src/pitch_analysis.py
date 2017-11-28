import numpy as np
from pyspark import RDD
from operator import add
from itertools import groupby


def get_most_popular_pitches_from_matrix(pitch_matrix, threshold=0.75, top=3, use_chord_structure=True):
    """
    Returns a list in order of most popular tuples of pitches used
    :param top: number of top pitches to return
    :param threshold: point after which a numeric value is considered as a
    :type pitch_matrix: np.matrix
    """
    number_of_segments = pitch_matrix.shape[0]
    main_pitches = pitch_matrix > threshold
    pitch_multiset = dict()
    for i in range(number_of_segments):
        pitch = tuple([x for x in np.ravel(main_pitches[i, :])])
        if use_chord_structure:
            pitch = chord_structure(pitch)

        if pitch in pitch_multiset:
            pitch_multiset[pitch] = pitch_multiset[pitch] + 1
        else:
            pitch_multiset[pitch] = 1

    out = [(pitch_multiset[key], key) for key in pitch_multiset.keys()]
    out.sort(key=lambda x: x[0], reverse=True)
    return [x[1] for x in out[:top]]


def chord_structure(pitch):
    return tuple([x for x in np.roll(pitch, -_find_longest_true_sequence_position(pitch))])


def _find_longest_true_sequence_position(pitch):
    double_list = list(pitch)
    double_list = double_list + double_list
    max_length = 0
    max_length_pos = 0
    pos = 0
    for k, g in groupby(double_list):
        length = len([i for i in g])
        pos += length
        if k and length > max_length:
            max_length = length
            max_length_pos = pos - length
    return max_length_pos


def get_most_popular_pitches_from_rdd(analysis_segments_pitch, top=10, threshold=0.75, top_per_song=3,
                                      use_chord_structure=True):
    """

    :param analysis_segments_pitch:
    :param top: the output number of pitches to return
    :param threshold: threshold to consider a pitch as valid
    :param top_per_song: for each song, the maximum number of pitches to return
    :return: a list [(pitch, pitch_occurences), ...]
    :type analysis_segments_pitch: RDD
    """
    return analysis_segments_pitch \
        .flatMap(lambda x: [(p, 1) for p in
                            get_most_popular_pitches_from_matrix(x[1], threshold, top_per_song, use_chord_structure)
                            ]) \
        .reduceByKey(add).top(top, key=lambda x: x[1])


def get_most_popular_pitches_sequence_from_matrix(pitch_matrix, sequence_length, threshold=0.75, top=3,
                                                  normalize=True):
    """
    :param pitch_matrix:
    :return:
    :type
    pitch_matrix: np.matrix
    """
    number_of_segment_sequence = pitch_matrix.shape[0] - sequence_length + 1
    main_pitches = pitch_matrix > threshold
    pitch_multiset = dict()
    for i in range(number_of_segment_sequence):
        pitch = tuple([x for x in np.ravel(main_pitches[i:i + sequence_length, :])])
        if normalize:
            pitch = normalize_sequence(pitch)

        if pitch in pitch_multiset:
            pitch_multiset[pitch] = pitch_multiset[pitch] + 1
        else:
            pitch_multiset[pitch] = 1

    out = [(pitch_multiset[key], key) for key in pitch_multiset.keys()]
    out.sort(key=lambda x: x[0], reverse=True)
    return [x[1] for x in out[:top]]


def get_most_popular_pitches_sequence_from_rdd(analysis_segments_pitch, sequence_length, top=10, threshold=0.75,
                                               top_per_song=3, normalize=True):
    return analysis_segments_pitch \
        .flatMap(lambda x: [(p, 1) for p in
                            get_most_popular_pitches_sequence_from_matrix(x[1], sequence_length, threshold,
                                                                      top_per_song, normalize)
                            ]) \
        .reduceByKey(add).top(top, key=lambda x: x[1])


def pretty_print_sequence(sequence):
    n = len(sequence) / 12
    for i in range(n):
        print('(' + ' ,'.join(['1' if x else '0' for x in sequence[i * 12:(i + 1) * 12]]) + ')')


def pretty_print_sequence_list(sequence_list):
    for i in range(len(sequence_list)):
        print('Sequence ' + str(i) + ' with ' + str(sequence_list[i][1]) + ' occurrences')
        pretty_print_sequence(sequence_list[i][0])
        print()


def normalize_sequence(pitch_sequence):
    n = len(pitch_sequence) / 12
    offset = _find_longest_true_sequence_position(pitch_sequence[:12])
    out = []
    for i in range(n):
        out = out + [x for x in np.roll(pitch_sequence[i * 12:(i + 1) * 12], -offset)]
    return tuple(out)
