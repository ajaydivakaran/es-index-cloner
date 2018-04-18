es-index-cloner
===============

Clone one elastic search index to another

- Clones documents along with mappings.
- Configure shard and replica count for the newly created index

How to run on host machine:
----------------

1. Clone repository
2. run "pip install -r requirements.txt" to install dependencies
3. run "python index_cloner -h" for help

How to run using docker:
----------------

1. run "docker run -t --rm --net=host geslot/es-index-cloner:1.0 -h" for help
