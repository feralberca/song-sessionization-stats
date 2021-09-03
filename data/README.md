## Dataset download process


* [Dataset reference](http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html)

## Schema


The schema for each the TSV is as follows:

```
"userid": id identifying the user
"timestamp": Timestamp in YYYYY-MM-DDTHH:mm:ssZ format
"musicbrainz-artist-id": Artist identifier
"artist-name": Artist name
"musicbrainz-track-id": Song identifier
"track-name": Song name

```

## Downloads

You may use the included `dataset-download.sh` script to download. Otherwise the data is also available [on this site](http://mtg.upf.edu/static/datasets/last.fm/lastfm-dataset-1K.tar.gz).

The script was tested on an Ubuntu disto. Other distros may have different utilities or command parameters.
