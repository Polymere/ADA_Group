# Predicting the next big song hit, what is at the heart of a song?

# Abstract

Using the Million song dataset and its complements, we want to find the relation between a song structure/audio features/lyrics and its "hotness".

What features of a song makes it so catchy? Is it possible to predict if a song will be popular or not? Our main investigation is finding a modal
to be able to predict if a song is a popular or not. Such a modal could then be used to predict the next hit song.


# Research questions

- What features can we use to characterise a song?
- How do these features correlate with the hotness?
- How to process the features?
- Are there universal features that make a song popular ?

# Dataset

We use the [the Million song](https://labrosa.ee.columbia.edu/millionsong/) dataset for the core metadata, and the [musiXmatch](https://labrosa.ee.columbia.edu/millionsong/musixmatch) dataset for the lyrics.

# File structure of the project

- the main report is at the root at the project, it's the report.pdf file.
- the 'src' directory contain all of our main python scripts
- the 'cluster_out' directory contain the pickle files we downloaded from the cluster and that we use in our notebooks
- the 'notebooks' directory contains experimental notebooks we used during the project
- the 'papers' directory contains papers we used in our research
- the 'images' directory contains an image we needed for our wordcloud

# Contribution to the project

Robin Weiskopf: lyrics analysis, lyrics vectorisation, wordcloud, writing the report
Paul Prevel: tag analysis, tag vectorisation, billboard analysis, writing the report
Marc Adams: histogram plots, correlation analysis, linear regression, extracting rdds on the cluster, writing the report