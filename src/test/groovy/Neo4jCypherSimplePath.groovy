import spock.lang.*
import org.neo4j.graphdb.*
import org.neo4j.cypher.javacompat.* 
import org.neo4j.test.*

/**
 * Demonstrate simple examples of working with paths
 * using three nodes and three relationships
 **/
class NeoCypherSimplePath extends spock.lang.Specification {
  /*
   * this class needs to be modified to conform to standards
   * also add with statements to graph node names rather than return path
   */

  @Shared graphDb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase()
  @Shared ExecutionEngine engine = new ExecutionEngine( graphDb )
  String cypher   // cypher query string
  def nodeProperties = [:]
  ExecutionResult er
  QueryStatistics qs
  Relationship rel
  def names = []

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
       MATCH p:Philosopher
       RETURN p.name as PhilosopherNames
       ORDER BY p.name
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    names = er.columnAs("PhilosopherNames").toList()

  then: "validate expected stats"
    ! qs.containsUpdates()
    names.equals(['Aristotle', 'Plato', 'Socrates'])

  }

  def "validate relationships"() {

  setup: "query to return philosopher pairs of who directly influences who"
    // find philosophers with an outgoing INFLUENCES relationship 
    cypher = """
       MATCH a:Philosopher-[:INFLUENCES]->b:Philosopher
       RETURN a.name as InfluencerName, b.name as InfluenceeName
    """
    def relations = []

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    er.each { row ->
      relations << row['InfluencerName'] + " Influences " + row['InfluenceeName']  
    }

  then: "validate expected stats"
    ! qs.containsUpdates()
    relations.sort().equals([
      'Plato Influences Aristotle'
      , 'Socrates Influences Aristotle'
      , 'Socrates Influences Plato'
    ])
  }

  def "get relationship once removed"() {

  setup: "query to return philosopher pairs with influence at one remove"
    // find philosophers who influenced others indirectly through those they directly influenced
    cypher = """
       MATCH a:Philosopher-[:INFLUENCES*2]->b:Philosopher
       RETURN a.name as InfluencerName, b.name as InfluenceeName
    """
    def relations = []

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    er.each { row ->
      relations << row['InfluencerName'] + " Influences " + row['InfluenceeName']  
    }

  then: "validate expected stats"
    ! qs.containsUpdates()
    relations.sort().equals([
       'Socrates Influences Aristotle'
    ])
  }

  def "get relationship once removed with intermediate"() {

  setup: "query to return philosopher pairs with influence at one remove with intermediate"
    // find philosophers who influenced others indirectly through those they directly influenced
    // include the philosophers mediating this relationship
    cypher = """
       MATCH a:Philosopher-[:INFLUENCES]->b:Philosopher-[:INFLUENCES]->c:Philosopher
       RETURN a.name as InfluencerName, b.name as MediatorName, c.name as InfluenceeName
    """
    def relations = []

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    er.each { row ->
      relations << row['InfluencerName'] + " Influences " + row['MediatorName'] + " Influences " + row['InfluenceeName']  
    }

    then: "validate expected stats"
      ! qs.containsUpdates()
      relations.sort().equals([
         'Socrates Influences Plato Influences Aristotle'
      ])

  }


  def "all influencers of Aristotle"() {

  setup: "query to return all philosophers who influenced Aristole at any depth"
    // note use of DISTINCT
    // 'Socrates Influences Aristotle' occurs twice, once for direct and once for indirect influence
    // DISTINCT will remove duplicates
    cypher = """
       MATCH a:Philosopher-[:INFLUENCES*]->b:Philosopher
       WHERE  b.name = 'Aristotle'
       RETURN DISTINCT a.name as InfluencerName, b.name as InfluenceeName
    """
    def relations = []

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    er.each { row ->
      relations << row['InfluencerName'] + " Influences " + row['InfluenceeName']  
    }
    // sort the relations list here to make then: test results more readable
    relations.sort(true)

    then: "validate expected stats"
      ! qs.containsUpdates()
      relations.equals([
        'Plato Influences Aristotle'
        , 'Socrates Influences Aristotle'
      ])

   }

  def "all influencers of Aristotle pattern"() {

  setup: "query to return pattern of who influenced Aristole at any depth"
    cypher = """
       MATCH pattern = a:Philosopher-[:INFLUENCES*]->b:Philosopher
       WHERE  b.name = 'Aristotle'
       RETURN pattern 
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics

  then: "validate expected stats"
      // because patterns return internal ids 
      // just checking that we got three of them rather than the values
      // could also return length(pattern) and verify = [1,2]
      ! qs.containsUpdates()
      er.iterator().size() == 3

  }

  def "shortest path influencers of Aristotle"() {

  setup: "query to return shortest path and length of path"
  // there are two paths from Socrates to Aristotle, a 1 hop, and a 2 hop.  
  // get the shortest one - a direct relationship
    cypher = """
       MATCH pattern = shortestPath(a:Philosopher-[:INFLUENCES*]->b:Philosopher)
       WHERE  a.name = 'Socrates' AND b.name = 'Aristotle'
       RETURN a.name, b.name , length(pattern) as length
    """
    def length
    def size = 0

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    er.each { row ->
      size++
      length = row['length'] 
    }

  then: "validate expected stats"
      // because patterns return internal ids 
      // just checking length and size 
      ! qs.containsUpdates()
      length == 1
      size == 1

  }

}
