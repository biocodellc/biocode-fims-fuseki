# biocode-fims-fuseki

This repo provides the necessary code for uploading and querying [Biocode-FIMS](https://github.com/biocodellc/biocode-fims-commons/) Bcid datasets to an [Apache Fuseki](http://jena.apache.org/documentation/serving_data/) triple-store database for storing datasets as RDF triples. 

The gradlew sripts supplied with this repository can build a self-contained jar file for triplifying data, which is part of the data processing process.   This is run like:

```
./gradlew shadowJar
```

The output of this task will create a dist/ppo-fims-triples.jar file which can be executed with help, like:

```
java -jar ppo-fims-triples.jar -h
```
