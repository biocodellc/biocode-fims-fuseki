package biocode.fims.fuseki.triplify;

import biocode.fims.digester.Mapping;
import biocode.fims.digester.Validation;
import biocode.fims.fimsExceptions.FimsRuntimeException;
import biocode.fims.reader.ReaderManager;
import biocode.fims.run.ProcessController;
import biocode.fims.settings.Connection;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.util.FileUtils;
import de.fuberlin.wiwiss.d2rq.jena.ModelD2RQ;
import org.apache.commons.cli.*;
import org.apache.commons.digester3.Digester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import biocode.fims.reader.plugins.TabularDataReader;
import biocode.fims.fuseki.deepRoots.*;
import biocode.fims.settings.*;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

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
//        FimsPrinter.out.println(status);

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
     *
     * @return
     */
    private String getMapping(Boolean verifyFile) {
        if (verifyFile)
            connection.verifyFile();

        File mapFile = PathManager.createUniqueFile(filenamePrefix + ".mapping.n3", outputFolder);
        try {
            PrintWriter pw = new PrintWriter(mapFile);
            TabularDataReader tdr = processController.getValidation().getTabularDataReader();
            Mapping mapping = processController.getMapping();
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
    public boolean run(File sqlLiteFile, Boolean deepRoots) {
//        if (Boolean.valueOf(SettingsManager.getInstance().retrieveValue("deepRoots"))) {
        if (deepRoots) {
            try {
                runDeepRoots();
            } catch (Exception e) {
                System.out.println("Generating stack trace from biocode-fims-fuseki;Triplifier.run function");
                e.printStackTrace();
            }
        }

        String status = "Converting Data Format ...";
        processController.appendStatus(status + "<br>");
//        FimsPrinter.out.println(status);

        // Create a connection to a biocode-fims-commons.jar SQL Lite Instance
        //System.out.println("READING " + sqlLiteFile);
        this.connection = new Connection(sqlLiteFile);
        getTriples();
        return true;
    }

    /**
     * create a DeepRoots object based on results returned from the biocode-fims DeepRoots service
     */
    public void runDeepRoots() throws Exception {
        dRoots = new DeepRootsReader().createRootData(processController.getProjectId(),
                processController.getExpeditionCode());
    }

    /**
     * @param args
     */
    public static void main(java.lang.String[] args) throws Exception {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("/applicationContext.xml");

        // Some classes to help us
        CommandLineParser clp = new GnuParser();
        HelpFormatter helpf = new HelpFormatter();
        CommandLine cl;

        // The input file
        String inputFile = "";
        String outputDirectory = "";
        String configFile = "";
        boolean runDeepRoots = false;
        boolean stdout = true;

        // Define our commandline options
        Options options = new Options();
        options.addOption("h", "help", false, "print this help message and exit");
        options.addOption("o", "outputDirectory", true, "Output Directory");
        options.addOption("i", "inputFile", true, "Input Spreadsheet");
        options.addOption("configFile", true, "Use a local config file instead of getting from server");
//        options.addOption("deepRoots", true, "run deepRoots while triplifying");
        options.addOption("w", "writeFile", true, "Don't use stdout, instead print to file and return location");

        // Create the commands parser and parse the command line arguments.
        try {
            cl = clp.parse(options, args);
        } catch (UnrecognizedOptionException e) {
            FimsPrinter.out.println("Error: " + e.getMessage());
            return;
        } catch (ParseException e) {
            FimsPrinter.out.println("Error: " + e.getMessage());
            return;
        }

        // Help
        if (cl.hasOption("h")) {
            helpf.printHelp("fims ", options, true);
            return;
        }

        // No options returns help message
        if (cl.getOptions().length < 1) {
            helpf.printHelp("fims ", options, true);
            return;
        }

        // Sanitize project specification
        if (cl.hasOption("o")) {
            outputDirectory = cl.getOptionValue("o");
        }
//
//        if (cl.hasOption("deepRoots")) {
//            try {
//                runDeepRoots = Boolean.valueOf(cl.getOptionValue("deepRoots"));
//            } catch (Exception e) {
//                FimsPrinter.out.println("Bad option for deepRoots");
//                helpf.printHelp("fims ", options, true);
//                return;
//            }
//        }

        if (cl.hasOption("w")) {
            stdout = true;
        }

        if (cl.hasOption("i")) {
            inputFile = cl.getOptionValue("i");
        }

        if (cl.hasOption("configFile")) {
            configFile = cl.getOptionValue("configFile");
        }

        if (configFile.isEmpty() || outputDirectory.isEmpty() || inputFile.isEmpty()) {
            FimsPrinter.out.println("All options are required");
            return;
        }
        File config = new File(configFile);

        ProcessController processController = new ProcessController();

        Mapping mapping = new Mapping();
        mapping.addMappingRules(new Digester(), config);

        Validation validation = new Validation();
        validation.addValidationRules(new Digester(), config, mapping);

        processController.setMapping(mapping);
        processController.setValidation(validation);

        ReaderManager rm = new ReaderManager();
        rm.loadReaders();
        TabularDataReader tdr = rm.openFile(inputFile, mapping.getDefaultSheetName(), outputDirectory);

        validation.run(tdr, "test", outputDirectory, mapping);

        // add messages to process controller and print
        validation.getMessages(processController);

        if (!processController.getHasErrors()) {
            Triplifier t = new Triplifier("test", outputDirectory, processController);
            t.run(validation.getSqliteFile(), runDeepRoots);

            if (stdout) {
                try (BufferedReader br = new BufferedReader(new FileReader(t.getTripleOutputFile()))) {
                    String line = null;
                    while ((line = br.readLine()) != null) {
                        System.out.println(line);
                    }
                }
            } else {
                System.out.println("new Triple file created: " + t.getTripleOutputFile());
            }
        } else {
            System.err.println(processController.getMessages().toString());
            System.err.println("Unable to create triples until errors are fixed");
        }
    }
}
