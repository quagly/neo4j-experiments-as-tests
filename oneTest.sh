#!/bin/bash
# helper script to run a single test class

gradle -Dtest.single=Neo4jCypherOneNode test
# gradle -Dtest.single=NeoCypherOneRelationship test
# gradle -Dtest.single=NeoCypherErasAndSchools test
# gradle -Dtest.single=NeoCypherSameSchoolInfluence test
# gradle -Dtest.single=NeoCypherSameSchoolTypeInfluence test
# gradle -Dtest.single=NeoCypherSameSchoolTypeClassInfluence test
