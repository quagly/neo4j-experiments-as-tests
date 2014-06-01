import spock.lang.*
import org.neo4j.graphdb.*
import org.neo4j.cypher.javacompat.* 
import org.neo4j.test.*

/**
 * Demonstrate simple example of reading nodes and releation from files
 * using three nodes and three relationships
 **/
class Neo4jCypherReadFile extends spock.lang.Specification {

  @Shared GraphDatabaseService graphDb
  @Shared ExecutionEngine engine
  @Shared ConfigObject config

  //instance
  String cypher   // cypher query string
  ExecutionResult er
  QueryStatistics qs
  def result = []


  def setupSpec() {
    graphDb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase()
    engine = new ExecutionEngine( graphDb )
    config = new ConfigSlurper().parse(this.getClass().getClassLoader().getResource("project.groovy"))
    println config.projectDir
  }

  def "initialize philosopher nodes from file"() {

  setup: "query to create all philosophers from a file" 
    cypher = """ 
      LOAD CSV WITH HEADERS FROM "file:////${config.projectDir}//src/test/resources/philosophers.csv" AS csvLine
      CREATE (p:Philosopher { id: toInt(csvLine.id), name: csvLine.name, url: csvLine.url })
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics

  then: "validate expected stats"
    qs.containsUpdates()
    qs.nodesCreated == 3 
    qs.propertiesSet == 9
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

  def "initialize philosopher influence releationships from file"() {

  setup: "query to create all influence relationships from file" 
    cypher = """ 
      LOAD CSV WITH HEADERS FROM "file:////${config.projectDir}//src/test/resources/philosopher_influence.csv" AS csvLine
      MATCH (influencer:Philosopher { id: toInt(csvLine.influencerId)}),(influencee:Philosopher { id: toInt(csvLine.influenceeId)})
      CREATE (influencer)-[:INFLUENCES]->(influencee)
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics

  then: "validate expected stats"
    qs.containsUpdates()
    qs.relationshipsCreated == 3

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

  def "validate relationships"() {

  setup: "query to return philosopher pairs of who directly influences who"
    // find philosophers with an outgoing INFLUENCES relationship 
    cypher = """
       MATCH (a:Philosopher)-[:INFLUENCES]->(b:Philosopher)
       RETURN a.name as InfluencerName, b.name as InfluenceeName
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    result = er.iterator().toList().sort()
    result.size() == 3

  then: "validate expected stats"
    ! qs.containsUpdates()
    result.equals([
      [  InfluenceeName:'Aristotle'
       , InfluencerName:'Socrates'
      ],[
         InfluenceeName:'Plato'
       , InfluencerName:'Socrates'
      ],[
         InfluenceeName:'Aristotle'
       , InfluencerName:'Plato'
      ]
    ])

  }

  def "all influencers of Aristotle"() {

  setup: "query to return all philosophers who influenced Aristole at any depth"
    // note use of DISTINCT
    // 'Socrates Influences Aristotle' occurs twice, once for direct and once for indirect influence
    // DISTINCT will remove duplicates
    // show use of new MATCH clause filter.  New in 2.0.0-RC
    // no need for a WHERE clause here anymore
    cypher = """
       MATCH (a:Philosopher)-[:INFLUENCES*]->(b:Philosopher{name: 'Aristotle'})
       // WHERE  b.name = 'Aristotle'
       RETURN DISTINCT a.name as InfluencerName, b.name as InfluenceeName
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    result = er.iterator().toList().sort()
    //println result

    then: "validate expected stats"
      ! qs.containsUpdates()
      result.size() == 2
      result.equals([
        [  InfluenceeName:'Aristotle'
         , InfluencerName:'Socrates'
        ],[InfluenceeName:'Aristotle'
         , InfluencerName:'Plato'
        ]
      ])
   }

}
