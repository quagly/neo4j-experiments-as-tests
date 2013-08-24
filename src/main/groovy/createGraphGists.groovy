#!/usr/bin/env groovy

/**
 * TODO
 * where to store config info
 * gradle or in config file
 * or both
 * much of this configuration can be dynamic rather than hard coded
 * need to design what goes where
 **/ 

// local variables
File inputDir = new File("/Users/mike/Documents/code/neo4j/neo4j-experiments-as-tests/src/test/groovy/")
def outputDirName = "/Users/mike/Documents/code/neo4j/neo4j-experiments-as-tests/out/graphGists/"
def resourcesDirName = "/Users/mike/Documents/code/neo4j/neo4j-experiments-as-tests/src/main/resources/"
File configFile = new File ( resourcesDirName + "graphGistConfig.groovy")
def config = new ConfigSlurper().parse(configFile.toURL())

// binding variables
setupRegex  = /^\W*setup:\W*\"(.*)\"\W*$/   // regex to get setup description 
cypherRegex = /^\W*(staticCypher|cypher)/   // regex to match variable containing cypher

// process all spock tests unless excluded
inputDir.eachFile { inputFile->
  if ( inputFile.isFile() ) {
    def fileNameWithoutExtention = inputFile.name.replaceFirst(~/\.[^\.]+$/, '')
    if ( ! config.excludeFiles.contains(fileNameWithoutExtention) ) {
      File outputFile = new File( outputDirName + fileNameWithoutExtention + ".adoc" )
      if ( outputFile.exists() ) { assert outputFile.delete() }
      spockToGraphGist( inputFile, outputFile )
    }
  } 
}

def spockToGraphGist( File inputFile, File outputFile ) {
 Boolean isCypher = false
 String setupDescr = "Initialize Graph"  // use this for initial desciption, feature tests descr will overwrite this

  // header of outputFile
  outputFile << """
= graphGist generated from spock test ${inputFile.name}

graphGist asciiDoc file for use at http://gist.neo4j.org/ [GitHub Gist]

Generated on ${new Date()}

//console

"""

  inputFile.eachLine() { line ->
    // get setup description
    setupMatcher = ( line =~ setupRegex )
    if (setupMatcher.matches()){
      setupDescr = setupMatcher[0][1]
    }

    // find lines starting with """ 
    // this is the line after the query
    if ( isCypher && line =~ /^\W*"""/ ) {
      isCypher = false 
      outputFile << "----\n"
      outputFile << "//table\n"
    }

    // output Cypher
    if ( isCypher ) { outputFile << line + "\n" }

    // find lines starting with cypher or staticCypher, leading whitespace allowed
    // this is the line before the query
    if ( line =~ cypherRegex ) {
      isCypher = true
      outputFile << """
${setupDescr}
[source,cypher]
----
"""
    }
  }
}
