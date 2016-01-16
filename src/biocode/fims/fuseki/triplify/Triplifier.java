package biocode.fims.fuseki.triplify;

import biocode.fims.digester.Mapping;
import biocode.fims.fimsExceptions.FimsRuntimeException;
import biocode.fims.run.ProcessController;
import biocode.fims.settings.Connection;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.util.FileUtils;
import de.fuberlin.wiwiss.d2rq.jena.ModelD2RQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import biocode.fims.reader.plugins.TabularDataReader;
import biocode.fims.fuseki.deepRoots.*;
import biocode.fims.settings.*;

import java.io.*;

/**
 * Triplify source file, using code adapted from the BiSciCol Triplifier
 * http://code.google.com/p/triplifier
 */
public class Triplifier {

    public Connection connection;

    private String outputFolder;
    private Model model;
    private String tripleOutputFile;
    private String updateOutputFile;
    private String filenamePrefix;
    private DeepRoots dRoots = null;
    private ProcessController processController;

    private static Logger logger = LoggerFactory.getLogger(Triplifier.class);

    /**
     * triplify dataset on the tabularDataReader, writing output to the specified outputFolder and filenamePrefix
     *
     * @param filenamePrefix
     * @param outputFolder
     */
    public Triplifier(String filenamePrefix, String outputFolder, ProcessController processController) {
        this.outputFolder = outputFolder;
        this.filenamePrefix = filenamePrefix;
        this.processController = processController;
    }

    public String getOutputFolder() {
        return outputFolder;
    }

    public String getFilenamePrefix() {
        return filenamePrefix;
    }

    public Model getModel() {
        return model;
    }

    public String getTripleOutputFile() {
        return tripleOutputFile;
    }

    public String getUpdateOutputFile() {
        return updateOutputFile;
    }


    /**
     * Return triples
     *
     * @return
     */
    public void getTriples() {
        //String filenamePrefix = inputFile.getName();
        System.gc();
        String status = "\tWriting Temporary Output ...";
        processController.appendStatus(status + "<br>");
        // Inform cmd line users
        FimsPrinter.out.println(status);

        // Write the model
        model = new ModelD2RQ(FileUtils.toURL(getMapping(true)),
                FileUtils.langN3, "urn:x-biscicol:");
        model.setNsPrefix("ark", "http://ezid.cdlib.org/id/ark");
        // Write the model as simply a Turtle file
        File tripleFile = PathManager.createUniqueFile(filenamePrefix + ".n3", outputFolder);
        try {
            FileOutputStream fos = new FileOutputStream(tripleFile);
            model.write(fos, FileUtils.langNTriple, null);
            fos.close();
        } catch (FileNotFoundException e) {
            throw new FimsRuntimeException(500, e);
        } catch (IOException e) {
            logger.warn("IOException thrown trying to close FileOutputStream object.", e);
        }
        tripleOutputFile = outputFolder + File.separator + tripleFile.getName();

        if (tripleFile.length() < 1)
            throw new FimsRuntimeException("No triples to write!", 500);
    }

    /**
     * Construct the mapping file for D2RQ to read
     *
     * @param verifyFile
     * @return
     */
    private String getMapping(Boolean verifyFile) {
        if (verifyFile)
            connection.verifyFile();

        File mapFile = PathManager.createUniqueFile(filenamePrefix + ".mapping.n3", outputFolder);
        try {
            PrintWriter pw = new PrintWriter(mapFile);
            TabularDataReader tdr = processController.getValidation().getTabularDataReader();
            Mapping mapping = processController.getValidation().getMapping();
            new D2RQPrinter(mapping, pw, connection, dRoots, processController).printD2RQ(tdr.getColNames());
            pw.close();
        } catch (FileNotFoundException e) {
            throw new FimsRuntimeException(500, e);
        }
        return outputFolder + File.separator + mapFile.getName();
    }

    /**
     * Run the triplifier using this class
     */
    public boolean run() {

        String status = "Converting Data Format ...";
        processController.appendStatus(status + "<br>");
        FimsPrinter.out.println(status);

        // Create a connection to a SQL Lite Instance
        this.connection = new Connection(processController.getValidation().getSqliteFile());
        getTriples();
        return true;
    }

    /**
     * create a DeepRoots object based on results returned from the biocode-fims DeepRoots service
     */
    public void runDeepRoots() {
        dRoots = new DeepRootsReader().createRootData(processController.getProjectId(),
                processController.getExpeditionCode());
    }
}
