import spock.lang.*
import org.neo4j.graphdb.*
import org.neo4j.cypher.javacompat.* 
import org.neo4j.test.*

class Neo4jCypherOneNode extends spock.lang.Specification {

  // class
  @Shared GraphDatabaseService graphDb 
  @Shared ExecutionEngine engine 

  // instance
  String cypher   
  def result = []
  ExecutionResult er
  QueryStatistics qs

  def setupSpec() {
    graphDb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase()
    engine = new ExecutionEngine( graphDb )
  }
 
  def "create one node"() {

  setup: "query to create one node with one property"
    cypher = """
      CREATE (n {name : 'Immanuel'})
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics

  then: "validate expected stats"
    qs.containsUpdates()
    qs.nodesCreated == 1 
    qs.propertiesSet == 1 

  }

  def "add property to node"() {

    setup: "query to add property to node"
    cypher = """
      START n=node(*)
      WHERE n.name! = 'Immanuel'
      SET n.lastName = 'Kant'
      RETURN n as kant 
    """

    when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    result = er.columnAs("kant").toList()

    then: "one property updated and lastName is Kant"
    qs.containsUpdates()
    qs.propertiesSet == 1
    result.size == 1
    // query returns node rather than property so get property to compare
    result.first().getProperty("lastName").equals('Kant')
  }

  def "remove name property and add firstName property"() {

    setup: "query to add and remove property from node"
    cypher = """
      START n=node(*)
      WHERE n.name! = 'Immanuel'
      SET n.firstName = 'Immanuel'
      REMOVE n.name
      RETURN n as kant 
    """

    when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    result = er.columnAs("kant").toList()

    then: "name property removed and firstName added"
    qs.containsUpdates()
    qs.propertiesSet == 2
    result.size == 1
    ! result.first().hasProperty("name")
    result.first().getProperty("firstName").equals('Immanuel')
  }

  def "use AND HAS and RegEx"() {

    setup: "query to filter with AND HAS and RegEx"
    cypher = """
      START n=node(*)
      WHERE
        HAS(n.firstName)           // test existence
        AND n.firstName! = 'Immanuel'  // test not Null and equal
        AND n.lastName =~ '^K.*'   // RegEx
        AND n.firstName =~ '(?i)immANuEl'      // case insensitive RegEx
        AND n.lastName IN [ 'Kant', 'Kan', 'NoIKant', 'YesIKan' ]  // in list
      RETURN n as kant 
    """

    when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    result = er.columnAs("kant").toList()
  
    then: "returns Mike West node"
    !qs.containsUpdates()
    qs.propertiesSet == 0 
    result.size == 1
    result.first().getProperty("firstName").equals('Immanuel')
    result.first().getProperty("lastName").equals('Kant')
  }

  def "delete node"() {

    setup: "query to delete node"
    cypher = """
      START n=node(*)
      WHERE n.firstName! = 'Immanuel'
      DELETE n
    """

    when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
  
    then: "node deleted"
    qs.containsUpdates()
    qs.deletedNodes == 1
  }

  def cleanupSpec() {
    graphDb.shutdown()
  }
}  
