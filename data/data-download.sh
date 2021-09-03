#!/bin/sh
set -e

DIR=$( dirname "$0" )
DIR=$( cd "$DIR"; pwd )

FILE_DATASET="http://mtg.upf.edu/static/datasets/last.fm/lastfm-dataset-1K.tar.gz"

rm -rf "$DIR/lastfm-dataset-1K"

echo "Downloading data to $DIR"

curl -L -o "$DIR/song-dataset.gz" "$FILE_DATASET"
echo "Download complete"

echo "Unzipping file"
gunzip -f "$DIR/song-dataset.gz"

echo "Done!"
