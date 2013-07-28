neo4j-experiments-as-tests
==========================

learning about neo4j using tests

Currently this contains cypher queries with tests validating correct results
using data about Philosophers from dbpedia.  I intend this project to include 
whatever I like to code for neo4j.  

run

> ./gradlew

in the root directory.

The default task runs all tests

There is also a task to convert spock tests to adocs to create githup gists
for use at http://gist.neo4j.org/ [GitHub Gist]
this does require that the Spocks test follow a format that is not yet documented

usage 
> ./gradlew createGraphGists

## This project is my first use of these technologies

* github
* gradle
* neo4j
* spock

any feedback on my usage is appriciated.  
