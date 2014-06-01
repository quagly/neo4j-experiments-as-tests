#!/bin/bash
# helper script to run a single test class

# gradle -Dtest.single=Neo4jCypherOneNode test
# gradle -Dtest.single=Neo4jCypherExceptions test
# gradle -Dtest.single=Neo4jCypherCollection test
# gradle -Dtest.single=Neo4jCypherOneRelationship test
# gradle -Dtest.single=Neo4jCypherSameSchoolInfluence test
# gradle -Dtest.single=Neo4jCypherSameSchoolTypeInfluence test
# gradle -Dtest.single=Neo4jCypherSameSchoolTypeClassInfluence test
# gradle -Dtest.single=Neo4jCypherSimplePath test
# gradle -Dtest.single=Neo4jCypherErasAndSchools test
# gradle -Dtest.single=Neo4jCypherOneLabel test
# gradle -Dtest.single=Neo4jCypherMerge test
gradle -Dtest.single=Neo4jCypherReadFile test
