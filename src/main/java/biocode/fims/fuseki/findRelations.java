package biocode.fims.fuseki;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Find expressed logical axioms between subjects and objects by querying restrictions that are expressed
 * in a pre-inferred ontology.  This class was tested using a Triples serialization but should work with other
 * formats.  The purpose of this class is to aid in the creation of instance data based on any ontology.
 * The formation of this class is built around the PPO but *should* work with other ontologies.
 * The SPARQL was built around statements from: https://www.w3.org/TR/owl-ref/#ValueRestriction
 * <p/>
 * This class will also provide a convenience methods for returning a single relation given any subject/object pair.
 */
public class findRelations {

    private String owlFileURI;
    private String lang;
    private ArrayList relations = new ArrayList<relation>();

    public findRelations(String owlFileURI, String lang, Boolean runCardinality) {
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
        OntModel base = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM);
        base.read(owlFileURI, lang);

        // Create a new query
        // see https://mailman.stanford.edu/pipermail/protege-owl/2012-February/018179.html
        String queryString =
                "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                        "PREFIX owl:<http://www.w3.org/2002/07/owl#>\n" +
                        "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>\n" +
                        "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>\n" +
                        "SELECT ?subject ?s_label ?property ?object ?o_label ?valueRestriction \n" +
                        //"SELECT  ?s_label  ?p_label ?o_label ?valueRestriction \n" +
                        //"SELECT ?pp \n" +
                        "WHERE { \n" +
                        "\t?subject rdf:type owl:Class . \n" +
                        "\t?subject rdfs:label ?s_label . \n" +
                        "\t?object rdfs:label ?o_label .\n" +
                        "\t#Look for expressed relations in either subClass or equivalentClass assertions\n" +
                        "\t?subject rdfs:subClassOf|owl:equivalentClass ?restriction\n" +
                        "\t#Discover relations as valueRestrictions or cardinalityRestrictions, these are expressed somewhat\n" +
                        "\t#differently so require their own SPARQL, then we union the results\n" +
                        "\t{\n" +
                        "\t\t{\n" +
                        "\t\t\t?restriction owl:onProperty ?property .\n" +
                        "\t\t\t?restriction ?valueRestriction ?object .\n" +
                        //"\t\t\t?property rdfs:label ?p_label .\n" +

                      //  "\t\t\tFILTER (?valueRestriction = owl:allValuesFrom || ?valueRestriction = owl:someValuesFrom || ?valueRestriction = owl:hasValue)\n" +
                        "\t\t}\n";

        if (runCardinality) {
            queryString += "\t\tUNION\n" +
                    "\t\t{\n" +
                    "\t\t\t?restriction owl:onProperty ?property .\n" +
                    "\t\t\t?restriction owl:onClass ?object .\n" +
                    "\t\t\t?restriction ?valueRestriction ?cardinalityRestriction . \n" +
                    "\t\t\t#only using Qualified cardinality, i believe this the only type that makes sense in this case\n" +
                    "\t\t\tFILTER (?valueRestriction = owl:minQualifiedCardinality || ?valueRestriction = owl:qualifiedCardinality || ?valueRestriction = owl:maxQualifiedCardinality)\n" +
                    "\t\t}\n";
        }

        queryString += "\t}\n" +
                "}";
        Query query = QueryFactory.create(queryString);

        // Execute the query and obtain results
        QueryExecution qe = QueryExecutionFactory.create(query, base);
        ResultSet results = qe.execSelect();

        System.out.println(queryString);
        ResultSetFormatter.out(System.out, results, query);

        while (results.hasNext()) {
            QuerySolution qs = results.next();
            //System.out.println(qs.get("subject"));
            relation r = new relation(
                    qs.get("subject"),
                    qs.get("property"),
                    qs.get("object"),
                    qs.get("valueRestriction"),
                    qs.get("cardinalityRestriction")
            );
            relations.add(r);
        }


        // Important - free up resources used running the query
        qe.close();
        return results;
    }


    /**
     * print list of relations
     */
    public void print() {
        Iterator it = relations.iterator();
        // print header
        new relation().printRelationHeader();
        // iterate array and print each relation
        while (it.hasNext()) {
            relation r = (relation) it.next();
            r.printRelation();
        }
    }

    public static void main(String[] args) {



        //Ultimately we want to run this on the following, or rather, the inferred version of this, expressed in turtle;
        // https://raw.githubusercontent.com/PlantPhenoOntology/PPO/master/ontology/ppo.owl

        // From the simple inferred ontology that Ramona made
        /*
        findRelations fr = new findRelations(
                       "https://raw.githubusercontent.com/PlantPhenoOntology/ppo_fims/master/ontology/ppo_simple_inferred.owl",
                        "Turtle");
         */

        /*
        findRelations findRelations = new findRelations(
                "https://raw.githubusercontent.com/PlantPhenoOntology/PPO/master/ontology/ppo.owl",
                "RDF/XML",
                false
        );
        */
        findRelations findRelations = new findRelations(
                              "file:////Users/jdeck/IdeaProjects/ppo_fims/ontology/ppo_simple2_inferred.ttl",
                               "Turtle",
                false);



        // Print a list of all relations we found
        findRelations.print();
        // Print just the relation that joins two classes
        relation relation = findRelations.getRelation("http://purl.obolibrary.org/obo/PO_0009010", "http://purl.obolibrary.org/obo/PO_0009009");
        if (relation != null)
            System.out.println(relation.property.toString());
        else
            System.out.println("relation not found");
    }

    private relation getRelation(String subject, String object) {
        Iterator it = relations.iterator();

        // iterate array and print each relation
        while (it.hasNext()) {
            relation r = (relation) it.next();
            if (r.subject.toString().equalsIgnoreCase(subject) && r.object.toString().equalsIgnoreCase(object)) return r;
        }
        return null;
    }

    /**
     * relation class is useful for storing JUST The components that we are interested in.
     */
    public static class relation {
        protected RDFNode subject;
        protected RDFNode property;
        protected RDFNode object;
        protected RDFNode valueRestriction;
        protected RDFNode cardinalityRestriction;

        public relation(RDFNode subject, RDFNode property, RDFNode object, RDFNode valueRestriction, RDFNode cardinalityRestriction) {
            this.subject = subject;
            this.property = property;
            this.object = object;
            this.valueRestriction = valueRestriction;
            this.cardinalityRestriction = cardinalityRestriction;
        }

        /**
         * Generic constructor
         */
        public relation() {
        }

        public void printRelation() {
            StringBuilder sb = new StringBuilder();
            if (subject != null) sb.append(subject.toString());
            else sb.append("");
            if (property != null) sb.append("\t" + property.toString());
            else sb.append("");
            if (object != null) sb.append("\t" + object.toString());
            else sb.append("");
            if (valueRestriction != null) sb.append("\t" + valueRestriction.toString());
            else sb.append("");
            if (cardinalityRestriction != null) sb.append("\t" + cardinalityRestriction.toString());
            else sb.append("");

            System.out.println(sb.toString());
        }

        public void printRelationHeader() {
            System.out.println("subject\trelation\tobject\tvalueRestriction\tcardinalityRestriction");
        }
    }

}
