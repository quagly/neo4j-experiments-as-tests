import spock.lang.*
import org.neo4j.graphdb.*
import org.neo4j.cypher.javacompat.* 
import org.neo4j.test.*

class NeoCypherOneLabel extends spock.lang.Specification {
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
 
  def "create Plato"() {

  setup: "query to create plato with philosopher label"
    cypher = "CREATE (n:Philosopher { name : 'Plato' , url : 'http://dbpedia.org/resource/Plato' })"

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics

  then: "validate expected stats"
    qs.containsUpdates()
    qs.nodesCreated == 1 
    qs.propertiesSet == 2
    qs.labelsAdded == 1

  }

  def "create Aristotle"() {

  setup: "query to create Aristotle with philosopher label"
    cypher = "CREATE (n:Philosopher { name : 'Aristotle' , url : 'http://dbpedia.org/resource/Aristotle' })"

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics

  then: "validate expected stats"
    qs.containsUpdates()
    qs.nodesCreated == 1 
    qs.propertiesSet == 2
    qs.labelsAdded == 1

  }

  def "get All Philosphers"() {

  setup: "query to return all nodes with label philosopher"
    cypher = """
      MATCH p:Philosopher
      RETURN p as philosopher
  """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    // get all name properties from list of nodes
    // simpler and better performance it just return the properties needed from cypher
    result = er.columnAs("philosopher").toList().collect { it.getProperty("name") }.sort()
        
  then: "validate expected stats"
    ! qs.containsUpdates()
    result.size == 2
    result.equals([ 'Aristotle', 'Plato' ])

  }

}
