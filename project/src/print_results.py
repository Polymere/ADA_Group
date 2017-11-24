import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from pyspark import SparkContext

from analysing_data import numeric_array_or_matrix_histogram


def print_histogram(histogram, number_of_labels=10):
    n = len(histogram[1])
    sns.set(font_scale=0.75)
    barplot = sns.barplot(range(n), histogram[1])
    barplot.set_xticks(np.arange(0, n + 1, float(n + 1) / number_of_labels))
    barplot.set_xticklabels(format_labels([
        np.interp(float(n + 1) * float(i) / float(number_of_labels), range(n + 1), histogram[0])
        for i in range(number_of_labels)
    ]))
    plt.show()


def format_labels(labels):
    return [format(i, '.2e') for i in labels]


if __name__ == "__main__":
    sc = SparkContext(appName="Histogram")
    rdd = sc.pickleFile(r"C:\Users\marc_\Documents\Prog\ADA_Group\project\data\analysis-bars_start")
    _min, _max, histogram, number_of_filtered_elements = numeric_array_or_matrix_histogram(rdd)
    print_histogram(histogram)
    sc.stop()
