#!/bin/sh
set -e

DIR=$( dirname "$0" )
DIR=$( cd "$DIR"; pwd )

FILE_DATASET="http://mtg.upf.edu/static/datasets/last.fm/lastfm-dataset-1K.tar.gz"

rm -rf "$DIR/lastfm-dataset-1K"

echo "Downloading data to $DIR"

FILE_OUT="$DIR/song-dataset.tar.gz"
#curl -L -o "$FILEOUT" "$FILE_DATASET"
echo "Download complete"

echo "Unzipping file $FILE_OUT"
tar -xvzf "$FILE_OUT"

echo "Done!"
