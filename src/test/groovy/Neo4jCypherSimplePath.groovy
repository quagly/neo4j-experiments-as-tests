import spock.lang.*
import org.neo4j.graphdb.*
import org.neo4j.cypher.javacompat.*
import org.neo4j.test.*

/**
 * Demonstrate simple examples of working with paths
 * using three nodes and three relationships
 **/
class Neo4jCypherSimplePath extends spock.lang.Specification {

  @Shared GraphDatabaseService graphDb

  //instance
  String cypher   // cypher query string
  Result er
  QueryStatistics qs
  def result = []


  def setupSpec() {
    // graphDb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase()
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
  }

  def "initialize nodes and relations"() {

  setup: "query to create all nodes and relationships in one statement"
    cypher = """
      CREATE (socrates:Philosopher {name:'Socrates', url : 'http://dbpedia.org/resource/Socrates' })
        , (plato:Philosopher {name:'Plato', url : 'http://dbpedia.org/resource/Plato' })
        , ( aristotle:Philosopher { name : 'Aristotle' , url : 'http://dbpedia.org/resource/Aristotle' })
        , (socrates)-[:INFLUENCES]->(plato)
        , (socrates)-[:INFLUENCES]->(aristotle)
        , (plato)-[:INFLUENCES]->(aristotle)
    """

  when: "execute query and capture stats"
    er = graphDb.execute(cypher)
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
    er = graphDb.execute(cypher)
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
    er = graphDb.execute(cypher)
    qs = er.queryStatistics
    result = er.iterator().toList().sort()
    //println result

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

  def "get relationship once removed"() {

  setup: "query to return philosopher pairs with influence at one remove"
    // find philosophers who influenced others indirectly through those they directly influenced
    cypher = """
       MATCH (a:Philosopher)-[:INFLUENCES*2]->(b:Philosopher)
       RETURN a.name as InfluencerName, b.name as InfluenceeName
    """

  when: "execute query and capture stats"
    er = graphDb.execute(cypher)
    qs = er.queryStatistics
    result = er.iterator().toList().sort()
    //println result

  then: "validate expected stats"
    ! qs.containsUpdates()
    result.size() == 1
    result.equals([
      [  InfluenceeName:'Aristotle'
       , InfluencerName:'Socrates'
      ]
    ])
  }

  def "get relationship once removed with intermediate"() {

  setup: "query to return philosopher pairs with influence at one remove with intermediate"
    // find philosophers who influenced others indirectly through those they directly influenced
    // include the philosophers mediating this relationship
    cypher = """
       MATCH (a:Philosopher)-[:INFLUENCES]->(b:Philosopher)-[:INFLUENCES]->(c:Philosopher)
       RETURN a.name as InfluencerName, b.name as MediatorName, c.name as InfluenceeName
    """

  when: "execute query and capture stats"
    er = graphDb.execute(cypher)
    qs = er.queryStatistics
    result = er.iterator().toList().sort()
    //println result

    then: "validate expected stats"
      ! qs.containsUpdates()
      result.size() == 1
      result.equals([
        [  InfluenceeName:'Aristotle'
         , MediatorName:'Plato'
         , InfluencerName:'Socrates'
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
    er = graphDb.execute(cypher)
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

  def "all influencers of Aristotle pattern"() {

  setup: "query to return pattern of who influenced Aristole at any depth"
    cypher = """
       MATCH path = (a:Philosopher)-[:INFLUENCES*]->(b:Philosopher)
       WHERE  b.name = 'Aristotle'
       RETURN path
    """
    Transaction tx

  when: "execute query and capture stats"
    // I don't understand why getting this result requires a transaction context
    // maybe try again in a future version ( this is 2.0.0M6 )
    // tx = graphDb.beginTx()
    // try {
    er = graphDb.execute(cypher)
    qs = er.queryStatistics
    result = er.iterator().toList().sort()
    // tx.success()
    // } finally {
    //  tx.finish()
    // }

  then: "validate expected stats"
      // because paths return internal ids
      // just checking that we got three of them rather than the values
      // could also return length(path) and verify = [1,2]
      // could also return NODES(path) or for more control EXTRACT(path)
      // specifying what to extract.  See cypher doc on collection functions
      ! qs.containsUpdates()
      result.size() == 3

  }

  def "shortest path influencers of Aristotle"() {

  setup: "query to return shortest path and length of path"
  // there are two paths from Socrates to Aristotle, a 1 hop, and a 2 hop.
  // get the shortest one - a direct relationship
    cypher = """
       MATCH path = shortestPath((a:Philosopher)-[:INFLUENCES*]->(b:Philosopher))
       WHERE  a.name = 'Socrates' AND b.name = 'Aristotle'
       RETURN a.name as aName, b.name as bName , length(path) as length
    """

  when: "execute query and capture stats"
    er = graphDb.execute(cypher)
    qs = er.queryStatistics
    result = er.iterator().toList().sort()
    //println result

  then: "validate expected stats"
      // because patterns return internal ids
      // just checking length and size
      ! qs.containsUpdates()
      result.size() == 1
      result.equals([
        [  aName:'Socrates'
         , length:1
         , bName:'Aristotle']
      ])

  }


}
