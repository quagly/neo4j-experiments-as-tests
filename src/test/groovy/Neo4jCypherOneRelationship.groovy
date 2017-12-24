import spock.lang.*
import org.neo4j.graphdb.*
import org.neo4j.cypher.javacompat.*
import org.neo4j.test.*

class Neo4jCypherOneRelationship extends spock.lang.Specification {
  // class
  @Shared GraphDatabaseService graphDb

  // instance
  String cypher
  Result er
  QueryStatistics qs

  def setupSpec() {
    // for parallel execution of tests, each test class needs its own test database
    // or locking issues will occur
    // see https://neo4j.com/docs/java-reference/current/

    def ostempdir = System.getProperty('java.io.tmpdir')
    // Get the executed script as a (java) File
    File scriptFile = new File(getClass().protectionDomain.codeSource.location.path)
    // This holds the file name like "myscript.groovy"
    def scriptName = scriptFile.getName()
    def suffix = scriptName.take(scriptName.lastIndexOf('.'))
    File tempDir = new File(ostempdir, suffix)

    // better to create a testDirectory utility that supplies a test directory
    // graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase( testDirectory.directory() )
    graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase( tempDir )
  }

  def cleanupSpec() {
    graphDb.shutdown()
    // no need to remove temporary directory, it appears that the shutdown does that
  }

  def "create Plato"() {

  setup: "query to create plato with philosopher label"
    cypher = """
      CREATE (n:Philosopher { name : 'Plato' , url : 'http://dbpedia.org/resource/Plato' })
    """

  when: "execute query and capture stats"
    er = graphDb.execute(cypher)
    qs = er.queryStatistics

  then: "validate expected stats"
    qs.containsUpdates()
    qs.nodesCreated == 1
    qs.propertiesSet == 2
    qs.labelsAdded == 1

  }

  def "create Aristotle"() {

  setup: "query to create Aristotle with philosopher label"
    cypher = """
      CREATE (n:Philosopher { name : 'Aristotle' , url : 'http://dbpedia.org/resource/Aristotle' })
    """

  when: "execute query and capture stats"
    er = graphDb.execute(cypher)
    qs = er.queryStatistics

  then: "validate expected stats"
    qs.containsUpdates()
    qs.nodesCreated == 1
    qs.propertiesSet == 2
    qs.labelsAdded == 1

  }

  def "create relationship Plato influenced Aristotle"() {

  setup: "query to get Aristotle and Plato and create influenced relationship"
    // performance is better to have query return just name rather than nodes
    cypher = """
      MATCH (p:Philosopher), (a:Philosopher)
      WHERE p.name = 'Plato' AND a.name = 'Aristotle'
      CREATE (p)-[r:INFLUENCED]->(a)
      RETURN r
    """
    Transaction tx
    Relationship rel
    def reltype
    def startName
    def endName

  when: "execute query and capture stats"
    tx = graphDb.beginTx()
    try {
      er = graphDb.execute(cypher)
      qs = er.queryStatistics
      rel = er.columnAs("r").next()
      reltype = rel.type.name()
      startName = rel.startNode.getProperty('name')
      endName = rel.endNode.getProperty('name')
      tx.success()
    } finally {
      tx.close()
    }

  then: "validate expected stats"
    qs.containsUpdates()
    qs.relationshipsCreated == 1
    reltype.equals('INFLUENCED')
    startName.equals('Plato')
    endName.equals('Aristotle')
  }

  def "delete relationship Plato influenced Aristotle"() {

  setup: "query to delete INFLUENCED relationship between Aristotle and Plato"
    cypher = """
      MATCH (p:Philosopher)-[r:INFLUENCED]->(a:Philosopher)
      WHERE p.name = 'Plato' AND a.name = 'Aristotle'
      DELETE r
    """

  when: "execute query and capture stats"
    er = graphDb.execute(cypher)
    qs = er.queryStatistics

  then: "validate expected stats"
    qs.containsUpdates()
    qs.relationshipsDeleted == 1
  }

  def "delete nodes Plato and Aristotle"() {

  setup: "query to delete all philosopher Nodes"
    cypher = """
      MATCH (p:Philosopher)
      DELETE p
    """

  when: "execute query and capture stats"
    er = graphDb.execute(cypher)
    qs = er.queryStatistics

  then: "validate expected stats"
    qs.containsUpdates()
    qs.nodesDeleted == 2
  }

  def "create Plato influenced Aristotle"() {

  setup: "query to create Plato influenced Aristotle in one statement"
    // note that this is actually a create path example
    cypher = """
      CREATE path = (p:Philosopher {name:'Plato', url : 'http://dbpedia.org/resource/Plato' })
        -[:INFLUENCES]->
        ( a:Philosopher { name : 'Aristotle' , url : 'http://dbpedia.org/resource/Aristotle' })
    """

  when: "execute query and capture stats"
    er = graphDb.execute(cypher)
    qs = er.queryStatistics

  then: "validate expected stats"
    qs.containsUpdates()
    qs.nodesCreated == 2
    qs.relationshipsCreated == 1
    qs.propertiesSet == 4
    qs.labelsAdded == 2
  }

}
