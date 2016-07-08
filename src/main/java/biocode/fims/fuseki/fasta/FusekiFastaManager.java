package biocode.fims.fuseki.fasta;

import biocode.fims.digester.Entity;
import biocode.fims.digester.Mapping;
import biocode.fims.fasta.FastaSequence;
import biocode.fims.fasta.FastaUtils;
import biocode.fims.service.ExpeditionService;
import biocode.fims.entities.Bcid;
import biocode.fims.fimsExceptions.ServerErrorException;
import biocode.fims.fuseki.Uploader;
import biocode.fims.fasta.FastaManager;
import biocode.fims.run.ProcessController;
import biocode.fims.settings.PathManager;
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
    private ExpeditionService expeditionService;

    /**
     * Constructo for woring with fastaFiles
     * @param fusekiService the fuseki dataset url w/o trailing "data", "query", etc
     * @param processController
     * @param fastaFilename
     */
    public FusekiFastaManager(String fusekiService, ProcessController processController, String fastaFilename,
                              ExpeditionService expeditionService) {
        super(processController, fastaFilename);
        this.fusekiService = fusekiService;
        this.expeditionService = expeditionService;
    }

    @Override
    public void upload(String graphId, String outputFolder, String filenamePrefix) {
        if (fastaSequences.isEmpty()) {
            throw new ServerErrorException("No fasta data was found.");
        }

        // save fasta data as a triple file
        File tripleFile = PathManager.createUniqueFile(filenamePrefix, outputFolder);

        try ( PrintWriter out = new PrintWriter(tripleFile) ){

            for (FastaSequence sequence: fastaSequences) {
                out.write("<");
                out.write(getEntityRootIdentifier() + sequence.getLocalIdentifier());
                out.write("> <" + FastaSequence.SEQUENCE_URI + "> \"");
                out.write(sequence.getSequence());
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
        Entity rootEntity = FastaUtils.getEntityRoot(processController.getMapping(), FastaSequence.SEQUENCE_URI);
        // if rootEntity is null, then there is no SEQUENCE_URI for this project
        if (rootEntity != null) {
            String insert = "INSERT { GRAPH <" + newGraph + "> { ?s <" + FastaSequence.SEQUENCE_URI + "> ?o }} WHERE " +
                    "{ GRAPH <" + newGraph + "> { ?s a <" + rootEntity.getConceptURI() + "> } . " +
                    "GRAPH <" + previousGraph + "> { ?s <" + FastaSequence.SEQUENCE_URI + "> ?o }}";

            UpdateRequest update = UpdateFactory.create(insert);

            UpdateProcessRemote riStore = (UpdateProcessRemote)
                    UpdateExecutionFactory.createRemote(update, fusekiService + "/update");

            riStore.execute();
        }
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
        Entity rootEntity = FastaUtils.getEntityRoot(processController.getMapping(), FastaSequence.SEQUENCE_URI);

        if (rootEntity == null) {
            throw new ServerErrorException("Server Error", "No entity was found containing a urn:sequence attribute");
        }

        if (graph != null) {
            // query fuseki graph
            String sparql = "SELECT ?identifier " +
                    "FROM <" + graph + "> " +
                    "WHERE { " +
                    "?identifier a <" + rootEntity.getConceptURI() + "> " +
                    "}";
            QueryExecution qexec = QueryExecutionFactory.sparqlService(fusekiService + "/query", sparql);
            com.hp.hpl.jena.query.ResultSet results = qexec.execSelect();

            // loop through results adding identifiers to datasetIds array
            while (results.hasNext()) {
                QuerySolution soln = results.next();
                String identifier = soln.getResource("identifier").getURI();

                int subStringStart = getEntityRootIdentifier().length();

                datasetIds.add(identifier.substring(subStringStart));
            }

        }
        return datasetIds;
    }

    private String getEntityRootIdentifier() {
        Entity rootEntity = FastaUtils.getEntityRoot(processController.getMapping(), FastaSequence.SEQUENCE_URI);

        if (rootEntity == null) {
            throw new ServerErrorException("Server Error", "No entity was found containing a urn:sequence attribute");
        }

        // get the bcidRoot so we can parse the identifier from the fuseki db
        Bcid bcid = expeditionService.getEntityBcid(
                processController.getExpeditionCode(),
                processController.getProjectId(),
                rootEntity.getConceptAlias()
        );

        return String.valueOf(bcid.getIdentifier());
    }
}
