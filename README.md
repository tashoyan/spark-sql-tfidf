# Using Apache Spark SQL API to calculate TF-IDF

This project demonstrates how to calculate term frequency - inverse document frequency
([TF-IDF](https://en.wikipedia.org/wiki/Tf%E2%80%93idf))
with help of Spark SQL API.

Additionally, this project implements a trivial search engine based on TF-IDF ranking.
The search engine accepts keywords from the user on the standard input
and looks for songs in the `songs_db` directory.

The search engine calculates weights for all words of all songs lyrics.
The weight of a word in a document is its TF-IDF value within this document.
When fulfilling a user request, the engine retrieves documents with highest matching ranks.
It calculates matching rank for a document according to the formula:
```text
doc matching rank = TF-IDF(keyword1) + ... + TF-IDF(keywordN)
```

## Building the code

Run sbt and execute `assembly` task to build the uber jar of the application.

## Running the search engine

Run the shell script:
```bash
./songs_searcher.sh
```
The search engine first will read all songs from the `songs_db` directory
and build the data set of weighted words.
Then it will ask for search keywords.
Enter your keywords separated by spaces and press Enter.
The engine will display some songs in the order of relevance to the search terms:
```text
Enter keywords separated by spaces (CTRL-C for exit):
love forever
Found:
 1. love_me_tender.txt  /work/workspace/spark-sql-tfidf/songs_db/love_me_tender.txt
 2. red_river_valley.txt        /work/workspace/spark-sql-tfidf/songs_db/red_river_valley.txt
```

## Usage terms

This code is for educational and demonstration purposes only.
It is not intended for commercial usage.

## License

Apache 2.0
