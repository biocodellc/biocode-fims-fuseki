package biocode.fims.fuseki.triplify;

import biocode.fims.digester.Mapping;
import biocode.fims.digester.Validation;
import biocode.fims.fileManagers.dataset.Dataset;
import biocode.fims.fimsExceptions.FimsRuntimeException;
import biocode.fims.reader.DatasetTabularDataConverter;
import biocode.fims.reader.ReaderManager;
import biocode.fims.run.ProcessController;
import biocode.fims.settings.Connection;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFReader;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.util.FileUtils;
import de.fuberlin.wiwiss.d2rq.jena.ModelD2RQ;
import org.apache.commons.cli.*;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import biocode.fims.reader.plugins.TabularDataReader;
import biocode.fims.settings.*;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Triplify source file, using code adapted from the BiSciCol Triplifier
 * http://code.google.com/p/triplifier
 */
public class Triplifier {

    private String outputFolder;
    private Model model;
    private String tripleOutputFile;
    private String filenamePrefix;
    private ProcessController processController;
    private String outputLanguage = FileUtils.langNTriple;
    public String defaultLocalURIPrefix = "http://biscicol.org/test/";

    // Some common prefixes, to be added to the top of the input file of each expressed graph
    // TODO: set this information in the configuration file, including an IMPORT statement
    String prefixes =
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
                    "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n" +
                    "@prefix ark: <http://biscicol.org/id/ark:> .\n" +
                    "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n" +
                    "@prefix dwc: <http://rs.tdwg.org/dwc/terms/> . \n"  +
                    "@prefix dc: <http://purl.org/dc/elements/1.1/> .\n" +
                    "@prefix obo: <http://purl.obolibrary.org/obo/> .";

    private static Logger logger = LoggerFactory.getLogger(Triplifier.class);

    /**
     * triplify dataset on the tabularDataReader, writing output to the specified outputFolder and filenamePrefix
     *
     * @param filenamePrefix
     * @param outputFolder
     */
    public Triplifier(String filenamePrefix, String outputFolder,
                      ProcessController processController) {
        this.outputFolder = outputFolder;
        this.filenamePrefix = filenamePrefix;
        this.processController = processController;

    }

    public String getOutputLanguage() {
        return outputLanguage;
    }

    /**
     * Set the output language using the FileUtils.* lang constants
     *
     * @param outputLanguage
     */
    public void setOutputLanguage(String outputLanguage) {
        this.outputLanguage = outputLanguage;
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

    /**
     * Return triples
     *
     * @return
     */
    private void getTriples(String mappingFilepath) {

        String status = "\tWriting Temporary Output ...";
        processController.appendStatus(status + "<br>");

        // Write the model

        String mappingFilePathStringURL = FileUtils.toURL(mappingFilepath);
        // NOTE: ser ializ
        String serializationFormat = FileUtils.langN3;
        model = new ModelD2RQ(
                mappingFilePathStringURL,
                serializationFormat,
                defaultLocalURIPrefix);

        // Write the model as simply a Turtle file
        File tripleFile = PathManager.createUniqueFile(filenamePrefix + ".n3", outputFolder);
        try {
            FileOutputStream fos = new FileOutputStream(tripleFile);
            // NOTE: MUST use langNTriple here so cleaning expressions work
            model.write(fos, FileUtils.langNTriple, null);

            fos.close();
        } catch (FileNotFoundException e) {
            throw new FimsRuntimeException(500, e);
        } catch (IOException e) {
            logger.warn("IOException thrown trying to close FileOutputStream object.", e);
        }
        tripleOutputFile = tripleFile.getAbsolutePath();

        // cleanup property expressions
        try {
            cleanPropertyExpressions(tripleFile);
        } catch (IOException e) {
            throw new FimsRuntimeException("Error trying to clean property expressions", 500);
        }

        // add prefixes and imports
        try {
            addPrefixesAndImports(tripleFile);
        } catch (IOException e) {
            throw new FimsRuntimeException("Error adding prefixes and imports", 500);
        }

        try {
            cleanUpModel(tripleFile);
        } catch (FileNotFoundException e) {
            throw new FimsRuntimeException("Error in running clean up routines", 500);
        }

        if (tripleFile.length() < 1)
            throw new FimsRuntimeException("No triples to write!", 500);
    }

    /**
     * Construct the mapping file for D2RQ to read
     *
     * @return
     */
    private String getMapping(Connection connection, List<String> colNames) {
        connection.verifyFile();

        File mapFile = PathManager.createUniqueFile(filenamePrefix + ".mapping.n3", outputFolder);
        Validation validation = processController.getValidation();
        Mapping mapping = processController.getMapping();
        D2RQPrinter.printD2RQ(colNames, mapping, validation, mapFile, connection, defaultLocalURIPrefix);
        return mapFile.getAbsolutePath();
    }

    /**
     * Run the triplifier using this class
     */
    public void run(File sqlLiteFile, List<String> colNames) {
        String status = "\nConverting Data Format ...";
        processController.appendStatus(status + "<br>");

        Connection connection = new Connection(sqlLiteFile);
        String mappingFilepath = getMapping(connection, colNames);
        getTriples(mappingFilepath);
    }

    /**
     * D2RQ assumes all properties to be  rdf:type rdf:Property, even when they
     * can be more formally declared as owl:ObjectProperty.  The work-around is to re-write the
     * object of the rdf:type to be owl:ObjectProperty for these cases.
     * Since we know all relationships expressed in the FIMS configuration file to be expressions
     * involving object properties, we can simply loop these expressions and use a regular
     * expression parser to search and replace the appropriate property definition.
     *
     * @param inputFile Input file must be in N3 format
     *
     * @return true or false on whether this worked or not
     *
     * @throws IOException (File not found, encoding exceptions and IO errors)
     */
    public boolean cleanPropertyExpressions(File inputFile) throws IOException {

        File tempFile = new File("myTempFile.txt");

        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));

        // Look for lines to Remove from file and build Array
        ArrayList<String> linesToRemove = new ArrayList<String>();
        for (String line; (line = reader.readLine()) != null; ) {

            if (line.contains("http://www.w3.org/2002/07/owl#ObjectProperty")) {
                // replace the ObjectProperty expression with rdf:Property, so we can remove it.
                line = line.replace("http://www.w3.org/2002/07/owl#ObjectProperty", "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property");
                linesToRemove.add(line);
            }

        }

        BufferedReader reader2 = new BufferedReader(new FileReader(inputFile));
        String currentLine;

        while ((currentLine = reader2.readLine()) != null) {
            // trim newline when comparing with lineToRemove
            String trimmedLine = currentLine.trim();

            // look for line in output file
            if (!linesToRemove.contains(trimmedLine)) {
                writer.write(currentLine + System.getProperty("line.separator"));
            }

        }
        writer.close();
        reader.close();
        reader2.close();
        return tempFile.renameTo(inputFile);
    }

    /**
     * A general convenience method for adding prefixes and header to output file.
     * @param inputFile
     * @return
     * @throws IOException
     */
    public boolean addPrefixesAndImports(File inputFile) throws IOException {

            File tempFile = new File("myTempFile.txt");

            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));

            writer.write(prefixes);


            String currentLine;

            while ((currentLine = reader.readLine()) != null) {
                    writer.write(currentLine + System.getProperty("line.separator"));
            }
            writer.close();
            reader.close();
            return tempFile.renameTo(inputFile);
        }


    /**
     * A convenience method for cleaning up the model by reading in the output file and then
     * writing it back out.  It is read as N3 and written to Turtle
     * @param inputFile
     * @return
     * @throws FileNotFoundException
     */
    private boolean cleanUpModel(File inputFile) throws FileNotFoundException {
        File tempFile = new File("myTempFile.txt");

        // create an empty model
        Model model = ModelFactory.createDefaultModel();

        InputStream in = FileManager.get().open(inputFile.getAbsolutePath());
        if (in == null) {
            throw new IllegalArgumentException("File: " + inputFile.getAbsolutePath() + " not found");
        }

        // read the input langN3 file
        model.read(in, "", FileUtils.langN3);

        // write it to standard out
        FileOutputStream fos = new FileOutputStream(tempFile);

        // convert to turtle
        model.write(fos, FileUtils.langTurtle, null);

        return tempFile.renameTo(inputFile);
    }
    /**
     * @param args
     */
    public static void main(java.lang.String[] args) throws Exception {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("/applicationContextFuseki.xml");

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
        options.addOption("c", "configFile", true, "Use a local config file instead of getting from server");
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

        ProcessController processController = new ProcessController(0, null);

        Mapping mapping = new Mapping();
        mapping.addMappingRules(config);

        Validation validation = new Validation();
        validation.addValidationRules(config, mapping);

        processController.setMapping(mapping);
        processController.setValidation(validation);

        ReaderManager rm = new ReaderManager();
        rm.loadReaders();
        TabularDataReader tdr = rm.openFile(inputFile, mapping.getDefaultSheetName(), outputDirectory);

        Dataset dataset = new DatasetTabularDataConverter(tdr).convert(
                mapping.getAllAttributes(mapping.getDefaultSheetName()),
                mapping.getDefaultSheetName()
        );

        boolean isValid = validation.run(tdr, "test", outputDirectory, mapping, dataset);

        // add messages to process controller and print
        processController.addMessages(validation.getMessages());

        if (isValid) {
            Triplifier t = new Triplifier("test", outputDirectory, processController);
            JSONObject sample = (JSONObject) dataset.getSamples().get(0);
            t.run(validation.getSqliteFile(), new ArrayList<String>(sample.keySet()));

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
