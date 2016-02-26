package biocode.fims.fuseki.fasta;

import biocode.fims.bcid.Resolver;
import biocode.fims.fimsExceptions.ServerErrorException;
import biocode.fims.fuseki.Uploader;
import biocode.fims.fasta.FastaManager;
import biocode.fims.run.ProcessController;
import biocode.fims.settings.PathManager;
import biocode.fims.settings.SettingsManager;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.sparql.modify.UpdateProcessRemote;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateRequest;

import java.io.*;
import java.util.*;

/**
 * FusekiFastaManager handles uploading of FASTA files and attaching the sequences to a dataset
 */
public class FusekiFastaManager extends FastaManager {
    private String fusekiService;

    /**
     * Constructo for woring with fastaFiles
     * @param fusekiService the fuseki dataset url w/o trailing "data", "query", etc
     * @param processController
     * @param fastaFilename
     */
    public FusekiFastaManager(String fusekiService, ProcessController processController, String fastaFilename) {
        super(processController, fastaFilename);
        this.fusekiService = fusekiService;
    }

    @Override
    public void upload(String graphId, String outputFolder, String filenamePrefix) {
        if (fastaData.isEmpty()) {
            throw new ServerErrorException("No fasta data was found.");
        }

        String bcidRoot;
        if (Boolean.valueOf(SettingsManager.getInstance().retrieveValue("deepRoots"))) {
            // get the bcidRoot so we can parse the identifier from the fuseki db
            Resolver r = new Resolver(processController.getExpeditionCode(), processController.getProjectId(), "Resource");
            bcidRoot = r.getIdentifier() + ":";
            r.close();
        } else {
            // if deepRoots = false, the identifier is urn:x-biscicol:Resource:{identifier}
            bcidRoot = "urn:x-biscicol:Resource:";
        }

        // save fasta data as a triple file
        File tripleFile = PathManager.createUniqueFile(filenamePrefix, outputFolder);

        try ( PrintWriter out = new PrintWriter(tripleFile) ){

            for (Map.Entry<String, String> entry : fastaData.entrySet()) {
                out.write("<");
                out.write(bcidRoot + entry.getKey());
                out.write("> <urn:sequence> \"");
                out.write(entry.getValue());
                out.write("\" .\n");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Uploader u = new Uploader(fusekiService + "/data", tripleFile, graphId);
        u.execute();

        // delete the tripleFile now that it has been uploaded
        tripleFile.delete();
    }

    /**
     * copy over the fasta sequences <urn:sequence> from the previousGraph to the newGraph. Only copy the sequences where
     * the ark: exists in the new graph
     *
     * @param previousGraph
     * @param newGraph
     */
    @Override
    public void copySequences(String previousGraph, String newGraph) {
        String insert = "INSERT { GRAPH <" + newGraph + "> { ?s <urn:sequence> ?o }} WHERE " +
                "{ GRAPH <" + newGraph + "> { ?s a <http://www.w3.org/2000/01/rdf-schema#Resource> } . " +
                "GRAPH <" + previousGraph + "> { ?s <urn:sequence> ?o }}";

        UpdateRequest update = UpdateFactory.create(insert);

        UpdateProcessRemote riStore = (UpdateProcessRemote)
                UpdateExecutionFactory.createRemote(update, fusekiService + "/update");

        riStore.execute();
    }

    /**
     * fetches the latest dataset from the fuseki triple-store and then parses the returned Resources.
     *
     * @return An array of identifiers that exist in the dataset
     */
    @Override
    public ArrayList<String> fetchIds() {
        ArrayList<String> datasetIds = new ArrayList<>();
        String graph = fetchGraph();

        if (graph != null) {
            // query fuseki graph
            String sparql = "SELECT ?identifier " +
                    "FROM <" + graph + "> " +
                    "WHERE { " +
                    "?identifier a <http://www.w3.org/2000/01/rdf-schema#Resource> " +
                    "}";
            QueryExecution qexec = QueryExecutionFactory.sparqlService(fusekiService + "/query", sparql);
            com.hp.hpl.jena.query.ResultSet results = qexec.execSelect();

            // loop through results adding identifiers to datasetIds array
            while (results.hasNext()) {
                QuerySolution soln = results.next();
                String identifier = soln.getResource("identifier").getURI();

                int subStringStart;
                if (Boolean.valueOf(SettingsManager.getInstance().retrieveValue("deepRoots"))) {
                    // get the bcidRoot so we can parse the identifier from the fuseki db
                    Resolver r = new Resolver(processController.getExpeditionCode(), processController.getProjectId(), "Resource");
                    String bcidRoot = r.getIdentifier();
                    r.close();

                    subStringStart = bcidRoot.length();
                } else {
                    // if deepRoots = false, the identifier is urn:x-biscicol:Resource:{identifier}
                    subStringStart = "urn:x-biscicol:Resource:".length();
                }

                datasetIds.add(identifier.substring(subStringStart));
            }

        }
        return datasetIds;
    }
}
