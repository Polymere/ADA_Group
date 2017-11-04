# Generating the new summer hit !

# Abstract

Using the Million song dataset and its complements, we want to find the relation between a song structure/audio features/lyrics and its "hotness".

We want to be able to predict the new summer hit features and find the recipe of success.
The information extracted could be used to get an overview of the different trends and evolutions of the music production, as well as the characteristics of the music genres.


# Research questions

- How do the song style change with respect to the release month ?
- Can we extract a relation between the lyrics (vocabulary, emotions) and the audio features (beat, danceability) ?
- Can we predict the next hit based on the previous popular songs ?
- Are there universal features that make a song popular ?
- Are there features that characterise a genre or an artist ?

# Dataset

We will use [the Million song](https://labrosa.ee.columbia.edu/millionsong/) dataset for the core metadata, the [musiXmatch](https://labrosa.ee.columbia.edu/millionsong/musixmatch) dataset for the lyrics, the [tagtraum genre annotations](http://www.tagtraum.com/msd_genre_datasets.html) and [Top MAGD](http://www.ifs.tuwien.ac.at/mir/msd/)  for the music genres.

As for the dataset size, we will start working with the given subsamble (10 000 songs) to get an overview of the needed data cleaning operations, before using the cluster for the full dataset.


# A list of internal milestones up until project milestone 2
- Implement the data cleaning operations on the subsample
- Merge all the needed data from the different datasets
- Try different analysis approaches 
- Have a debugged/cleaned pipeline in prevision of milestone 3 

# Questions for TAa
NaN
