package biocode.fims.fuseki;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Find expressed logical axioms between subjects and objects by querying restrictions that are expressed
 * in a pre-inferred ontology.  This class was tested using a Triples serialization but should work with other
 * formats.  The purpose of this class is to aid in the creation of instance data based on any ontology.
 * The formation of this class is built around the PPO but *should* work with other ontologies.
 * The SPARQL was built around statements from: https://www.w3.org/TR/owl-ref/#ValueRestriction
 * <p/>
 * This class will also provide a convenience methods for returning a single relation given any subject/object pair.
 */
public class findInstanceRelations {

    private String owlFileURI;
    private String lang;

    public findInstanceRelations(String owlFileURI, String lang, Boolean runCardinality) {
        this.owlFileURI = owlFileURI;
        this.lang = lang;
        simplifiedRelationsUsingSparql(runCardinality);
    }

    /**
     * Build inferred relations using Jena
     *
     * @return
     */
    private ResultSet simplifiedRelationsUsingSparql(Boolean runCardinality) {
        // Don't need inferencing... this has been pre-inferred.
        OntModel base = ModelFactory.createOntologyModel(OntModelSpec.OWL_LITE_MEM_TRANS_INF);
        base.read(owlFileURI, lang);

        // Create a new query
        // see https://mailman.stanford.edu/pipermail/protege-owl/2012-February/018179.html
        String queryString =
                "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                        "PREFIX owl:<http://www.w3.org/2002/07/owl#>\n" +
                        "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>\n" +
                        "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>\n" +
                        "SELECT ?subject ?property  ?object ?property2 ?object2\n" +
                        "WHERE { \n" +
                        "\t<ark:/21547/AXq25891034> ?property ?object .\n" +
                        //"\tFILTER (?property != rdf:type) . \n" +
                        //"\tFILTER (?property != rdfs:isDefinedBy) . \n" +
                        //"\t?object ?property2 ?object2 ." +
                        //"\tFILTER (!isLiteral(?object)) . \n" +

                        "}";

        Query query = QueryFactory.create(queryString);

        // Execute the query and obtain results
        QueryExecution qe = QueryExecutionFactory.create(query, base);
        ResultSet results = qe.execSelect();

        System.out.println(queryString);
        ResultSetFormatter.out(System.out, results, query);


        // Important - free up resources used running the query
        qe.close();
        return results;
    }


    public static void main(String[] args) {
        findInstanceRelations findRelations = new findInstanceRelations(
                "file:////Users/jdeck/IdeaProjects/ppo_fims/data/NPN_raw_data_leaf_example_trunc_output.ttl",
                //"http://data.biscicol.org/ds/data?graph=urn%3Auuid%3A46faf10d-1fcb-4072-9e9a-f1d3fea9790f",
                "Turtle",
                false);
    }


}