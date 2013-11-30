import spock.lang.*
import org.neo4j.graphdb.*
import org.neo4j.cypher.javacompat.* 
import org.neo4j.test.*

/**
 * Demonstrate simple examples of working with paths
 * using three nodes and three relationships
 **/
class Neo4jCypherCollection extends spock.lang.Specification {

  @Shared GraphDatabaseService graphDb
  @Shared ExecutionEngine engine

  //instance
  String cypher   // cypher query string
  ExecutionResult er
  QueryStatistics qs
  def result = []


  def setupSpec() {
    graphDb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase()
    engine = new ExecutionEngine( graphDb )
  }

  def "initialize nodes and relations"() {

  setup: "query to create all nodes and relationships in one statement" 
    cypher = """ 
      CREATE (socrates:Philosopher {name:'Socrates', url : 'http://dbpedia.org/resource/Socrates' })
        , (plato:Philosopher {name:'Plato', url : 'http://dbpedia.org/resource/Plato' })
        , ( aristotle:Philosopher { name : 'Aristotle' , url : 'http://dbpedia.org/resource/Aristotle' })
        , socrates-[:INFLUENCES]->plato
        , socrates-[:INFLUENCES]->aristotle
        , plato-[:INFLUENCES]->aristotle
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics

  then: "validate expected stats"
    qs.containsUpdates()
    qs.nodesCreated == 3 
    qs.relationshipsCreated == 3
    qs.propertiesSet == 6
    qs.labelsAdded == 3
  }

  def "validate nodes"() {

  setup: "query to return all nodes with label philosopher"
    cypher = """
       MATCH (p:Philosopher)
       RETURN p.name as PhilosopherNames
       ORDER BY p.name
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    result = er.columnAs("PhilosopherNames").toList().sort()

  then: "validate expected stats"
    ! qs.containsUpdates()
    result.size() == 3
    result.equals(['Aristotle', 'Plato', 'Socrates'])

  }

  def "collect nodes"() {

  setup: "query to collect all nodes between Socrates and Aristotle inclusive"
  // shows how to collect all NODES in the path and extract thier names
    cypher = """
       MATCH p=(a)-->(b)-->(c) 
       WHERE a.name = 'Socrates' and c.name = 'Aristotle'
       RETURN EXTRACT( n IN NODES(p) | n.name )  as PhilosopherNames
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    result = er.columnAs("PhilosopherNames").toList() //.sort()

  then: "validate expected stats"
    ! qs.containsUpdates()
    // result is a single List of three nodes so size is one array
    // actually it is scala.collection.convert.Wrappers$SeqWrapper
    // instead of List which is why toList() is called below
    result.size() == 1 
    result.first().toList().sort().equals(['Aristotle', 'Plato', 'Socrates'])

  }


}

