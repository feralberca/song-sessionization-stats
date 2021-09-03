# Song Stats Test



## Prerequisites

The instructions below assume you have a Unix-like environment.

You will need to have the following setup in your local environment or wherever you choose to run this test:

* JDK 8+
* sbt 1.3.x+

## Setup


* Clone this repo
* Run [this script](data/dataset-download.sh) to download the dataset 
* Set the memory options, 4GB should be Ok
```
$ export SBT_OPTS="-Xmx4G"
```
* Run the UserSongSessionStats app with 
```
$ sbt "runMain eng.test.solution.UserSongSessionStats ../data/lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv ../data/ouput"
```
I think it will work at once but if you encounter any issue, feel free to contact me.

## Tests


The are a few tests created for validating basic behavior. All tests can be run locally by simple invoking sbt test target:
```
$ sbt test
``` 

A happy ending test suite execution should finish like this:
```
[info] UserSongSessionStatsSpec:
[info] Spark
[info] - should not be stopped
[info] Dataset loading and cleansing - Filtering invalid and duplicated rows
[info] - should return just valid rows
[info] Test - Calculate top x songs with empty dataset
[info] - should return an empty DF
[info] Test - Calculate top x songs most played with single sessions
[info] - should return artist, track and count
[info] Test - Calculate top x songs most played in top y sessions
[info] - should return artist, track and count
[info] Run completed in 27 seconds, 883 milliseconds.
[info] Total number of tests run: 5
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 5, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
[success] Total time: 36 s, completed Sep 2, 2021 8:19:07 PM

```

## Submission Details

### Technical decisions

* The challenge could have been developed using multiple options. Before starting to code I've checked the options I had: 
** Could either implement it by following a simple ETL process and then query the records with Presto or some other similar tool 
** Could use Spark and code the logic and the tests using a functional approach.
* I've choosen Spark because it's simple to write a distributed computation using either the DSL or SQL. Also it's important to consider that I found easier to write tests in an imperative lang than SQL and it's more maintanable. 
* Spark makes the code scalable by distrubiting the processing among nodes. For this implementation and local execution we are just levaraging local CPU cores but in other runtime the computation could scale to hundreds of nodes and thousands of threads.
* Scala was choosen because I like to use typed langs for pipeline development. Pipelines can grow to a high number of lines of code and maintaning that with a dynamic lang could be challenging.

## Assumptions
* All fields are considered nullable, further filtering is applied for removing those invalid.
* If a value is present between tabs that value is considered valid, just empty values are considered NULL for the purpose of filtering. No trim or empty checks are applied, for cleansing demostration simple NULL filtering was included.
* Rows with invalid data format will be discarded. It's assumed the same format for all the timestamp values.
* UTF-8 encoding is assumed for the data
* Data in the CSV input file do not span multiple lines


## Implementation details:
* Deduplication is run on the required columns.
* On duration equality logest sessions will be prioritized using userId as sort column.
* Top songs results are sorted by descending count and ascending artist-name and track-name

## Improvements
* There was not enough time to run an exaustive analysis on the performance of the job but with time and a few code changes the code could be fine tunned.
* It was not mentioned but I've decided to include some filtering based on used columns. Simple NULL check was added but ideally it should include other cleansing and normalization routines such as: blank checks, trims and so on.
* More tests for sure. Probably using some sort of data simulation for generating the test scenario.

## Results
The file [song-stats.tsv](data/song-stats.tsv) contains the results of the requested computation. Please bear in mind the assumptions and the implementation details given that those decisions affects the results.


## Final thoughts
I really enjoyed the challange. It looks like a real use case that can be tackle on different ways. I hope you enjoy it as well reviewing it, any feedback is welcome! 
