package biocode.fims.fuseki.query;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.util.FileManager;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class with sparql queries to fetch data from a fuseki server
 */
public class Query {

    private final String queryTarget;

    public Query(String queryTarget) {
        this.queryTarget = queryTarget;
    }

    public Map<String, Map<String, Integer>> countIdentifiersAndSequences(List<String> graphs) {
        Map<String, Map<String, Integer>> graphCounts = new HashMap<>();
        StringBuilder sparql = new StringBuilder();
        StringBuilder namedGraphSb = new StringBuilder();

        for (String graph : graphs) {
            if (graph != null) {
                namedGraphSb.append("\tFROM NAMED <");
                namedGraphSb.append(graph);
                namedGraphSb.append(">\n");
            }
        }

        sparql.append("SELECT * \n");
        sparql.append(namedGraphSb);
        sparql.append("WHERE {\n");
        sparql.append("\tGraph ?g {\n");
        sparql.append("\t\tSELECT (COUNT(DISTINCT ?identifier) as ?count) (COUNT(?sequence) as ?sequenceCount)\n");
        sparql.append("\t\tWHERE {\n");
        sparql.append("\t\t\t?identifier a <http://www.w3.org/2000/01/rdf-schema#Resource> .\n");
        sparql.append("\t\t\tOPTIONAL { ?identifier <urn:sequence> ?sequence }\n");
        sparql.append("\t\t}\n");
        sparql.append("\t}\n");
        sparql.append("}");

        // query fuseki graph
        QueryExecution qexec = QueryExecutionFactory.sparqlService(queryTarget, sparql.toString());
        com.hp.hpl.jena.query.ResultSet results = qexec.execSelect();

        // loop through results adding the counts to the graphCounts map
        while (results.hasNext()) {
            QuerySolution soln = results.next();
            Map<String, Integer> counts = new HashMap<>();

            counts.put("identifiers", soln.getLiteral("?count").getInt());
            counts.put("sequences", soln.getLiteral("?sequenceCount").getInt());

            graphCounts.put(String.valueOf(soln.get("?g").asNode().getIndexingValue()), counts);
        }
        return graphCounts;
    }

    /**
     * Main method here for testing purposes only
     * @param args
     */
    public static void main(String[] args) {
        StringBuilder sb = new StringBuilder();
        sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "SELECT ?subject ?sampleID ?stageNode ?p ?stageLiteral\n" +
                "WHERE {\n" +
                "?subject rdf:type <http://rs.tdwg.org/dwc/terms/LivingSpecimen> .\n" +
                "?subject <http://rs.tdwg.org/dwc/terms/materialSampleID> ?sampleID .\n" +
                "?subject <urn:exhibits_some> ?stageNode .\n" +
                "}");


        String filename = "/Users/jdeck/IdeaProjects/biscicol-fims/tripleOutput/test.16.n3";

        // Create an empty model
        Model model = ModelFactory.createDefaultModel();

        // Use the FileManager to find the input file
        InputStream in = FileManager.get().open(filename);

        if (in == null)
            throw new IllegalArgumentException("File: " + filename + " not found");

        // Read the RDF/XML file
        model.read(in, null);

        com.hp.hpl.jena.query.Query query = QueryFactory.create(sb.toString());

        QueryExecution qexec = QueryExecutionFactory.create(query, model);
        com.hp.hpl.jena.query.ResultSet results = qexec.execSelect();

        // loop through results adding the counts to the graphCounts map
        while (results.hasNext()) {
            QuerySolution soln = results.next();
            System.out.println(soln.toString());
        }

    }
}
