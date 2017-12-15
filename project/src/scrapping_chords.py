import requests
from bs4 import BeautifulSoup
import numpy as np
import urllib
import re


def get_chords(url):
    page = requests.get(url)
    parsed_page = BeautifulSoup(page.text, 'html.parser')
    span_list = parsed_page \
        .find('pre', class_="js-tab-content js-init-edit-form js-copy-content js-tab-controls-item") \
        .find_all('span')
    chords = [x.text for x in span_list]
    return chords


SPACE_RE = re.compile(r'^\s+$')


def get_chords_and_lyrics(url):
    page = requests.get(url)
    parsed_page = BeautifulSoup(page.text, 'html.parser')
    out = []
    parsed_list = [i for i in
                   parsed_page.find('pre',
                                    class_="js-tab-content js-init-edit-form js-copy-content js-tab-controls-item"
                                    ).children
                   ]
    i = 0
    while i < len(parsed_list):
        lyrics = []
        while i < len(parsed_list) and parsed_list[i].name is None:
            if SPACE_RE.match(str(parsed_list[i])) is None:
                lyrics.append(str(parsed_list[i]))
            i += 1
        if lyrics:
            out.append(lyrics)

        chords = []
        while i < len(parsed_list) and (parsed_list[i].name == 'span' or SPACE_RE.match(str(parsed_list[i]))):
            if SPACE_RE.match(str(parsed_list[i])) is None:
                chords.append(str(parsed_list[i].text))
            i += 1
        if chords:
            out.append(chords)

        # error handling in case it's a html tag we didn't expect
        if i < len(parsed_list) and parsed_list[i].name != 'span' and parsed_list[i].name is not None:
            i += 1

    return out


CHORDS_URL_MATCHER = re.compile(r'.*_chords_\d+')


def search_on_ultimate_guitare(artist, title):
    """

    :param artist: artist name
    :param title: the title of the song
    :return: an url to a tab, or None if it wasn't found
    """
    search_page = requests.get(
        'https://www.ultimate-guitar.com/search.php?search_type=title&order=&value={0}'.format(
            urllib.parse.quote(artist + ' ' + title)
        )
    )
    parsed_search_page = BeautifulSoup(search_page.text, 'html.parser')
    results = parsed_search_page.find_all('a', class_='song result-link js-search-spelling-link')
    for res in results:
        url = res.attrs['href']
        # we check if the tab is in the right format
        if CHORDS_URL_MATCHER.match(url) is not None:
            return url
    return None


def extract_chords(sample_data):
    out = []
    i = 0
    for artist, title, hotness, hotness_class in sample_data:
        print(str(i))
        i += 1
        artist = artist.decode('utf-8')
        title = title.decode('utf-8')
        search_result = search_on_ultimate_guitare(artist, title)
        if search_result is None:
            print('{0} - {1} was not found'.format(artist, title))
        else:
            chords = get_chords(search_result)
            if not chords:
                print('unable to extract chords for: ' + search_result)
            else:
                out.append((artist, title, hotness, hotness_class, chords))

    return out


if __name__ == '__main__':
    get_chords_and_lyrics('https://tabs.ultimate-guitar.com/tab/la_roux/quicksand_chords_845910')
