package biocode.fims.fuseki.triplify;

import biocode.fims.digester.Mapping;
import biocode.fims.digester.Validation;
import biocode.fims.fimsExceptions.FimsRuntimeException;
import biocode.fims.reader.JsonTabularDataConverter;
import biocode.fims.reader.ReaderManager;
import biocode.fims.run.ProcessController;
import biocode.fims.settings.Connection;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.util.FileUtils;
import de.fuberlin.wiwiss.d2rq.jena.ModelD2RQ;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import biocode.fims.reader.plugins.TabularDataReader;
import biocode.fims.settings.*;

import java.io.*;
import java.util.ArrayList;
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
    private static String outputLanguage = FileUtils.langTurtle;
    private static String outputFormatExtension = "ttl";

    public static String defaultLocalURIPrefix = "test:";
    private boolean overWriteOutputFile = false;
    static String outputFormat = "TURTLE";


    // Some common prefixes, to be added to the top of the input file of each expressed graph
    String prefixes =
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
                    "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n" +
                    "@prefix ark: <http://biscicol.org/id/ark:> .\n" +
                    "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n" +
                    "@prefix dwc: <http://rs.tdwg.org/dwc/terms/> . \n" +
                    "@prefix dc: <http://purl.org/dc/elements/1.1/> .\n" +
                    "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n" +
                    "@prefix obo: <http://purl.obolibrary.org/obo/> .\n";

    String imports = "";

    private static Logger logger = LoggerFactory.getLogger(Triplifier.class);

    /**
     * triplify dataset on the tabularDataReader, writing output to the specified outputFolder and filenamePrefix
     *
     * @param filenamePrefix
     * @param outputFolder
     */
    public Triplifier(String filenamePrefix,
                      String outputFolder,
                      ProcessController processController,
                      boolean overWriteOutputFile,
                      String defaultLocalURIPrefix,
                      String outputLanguage
    ) {
        this.outputFolder = outputFolder;
        this.filenamePrefix = filenamePrefix;
        this.processController = processController;
        this.overWriteOutputFile = overWriteOutputFile;
        this.defaultLocalURIPrefix = defaultLocalURIPrefix;

        if (outputLanguage == null) {
            outputLanguage = this.outputLanguage;
        }
        // Set language and extensions
        if (outputLanguage.equals("N3")) {
            this.outputLanguage = outputLanguage;
            outputFormatExtension = "n3";
        } else if (outputLanguage.equals("N-TRIPLE")) {
            this.outputLanguage = outputLanguage;
            outputFormatExtension = "nt";
        } else if (outputLanguage.equals("RDF/XML")) {
            this.outputLanguage = outputLanguage;
            outputFormatExtension = "xml";
        } else if (outputLanguage.equals("TURTLE")) {
            this.outputLanguage = outputLanguage;
            outputFormatExtension = "ttl";
        } else {
            outputFormatExtension = "ttl";
        }

    }

    public Triplifier(String filenamePrefix,
                      String outputFolder,
                      ProcessController processController) {
        this(filenamePrefix, outputFolder, processController, false, "http://biscicol.org/test/", null);
    }

    public String getPrefixes() {
        return prefixes;
    }

    public void setPrefixes(String prefixes) {
        this.prefixes = prefixes;
    }

    public String getImports() {
        return imports;
    }

    public void setImports(String imports) {
        this.imports = imports;
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
        File tripleFile = null;
        if (!overWriteOutputFile)
            tripleFile = PathManager.createUniqueFile(filenamePrefix + ".n3", outputFolder);
        else
            tripleFile = new File(outputFolder + File.separator + filenamePrefix + ".n3");
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

        if (new File(tripleOutputFile).length() < 1)
            throw new FimsRuntimeException("No triples to write!", 500);
    }

    /**
     * Construct the mapping file for D2RQ to read
     *
     * @return
     */
    private String getMapping(Connection connection, List<String> colNames) {
        connection.verifyFile();
        File mapFile = null;
        if (!overWriteOutputFile)
            mapFile = PathManager.createUniqueFile(filenamePrefix + ".mapping.n3", outputFolder);
        else
            mapFile = new File(outputFolder + filenamePrefix + ".mapping.n3");

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
     *
     * @param inputFile
     *
     * @return
     *
     * @throws IOException
     */
    public boolean addPrefixesAndImports(File inputFile) throws IOException {

        File tempFile = new File("myTempFile.txt");

        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));

        writer.write(prefixes);

        writer.write(imports);

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
     *
     * @param inputFile
     *
     * @return
     *
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

        // convert to the default outputLanguage
        model.write(fos, outputLanguage, null);

        // Write the output file using the proper prefix
        String filePath = inputFile.getAbsolutePath().
                substring(0, inputFile.getAbsolutePath().lastIndexOf(File.separator));

        // Strip the trailing extension from this filename (no longer csv, xls, or txt)
        //String outputFilename = filenamePrefix;
        //if (filenamePrefix.indexOf(".") > 0)
        //    outputFilename = filenamePrefix.substring(0, filenamePrefix.lastIndexOf("."));

        File outputFile = new File(filePath + File.separator + filenamePrefix + "." + outputFormatExtension);
        tripleOutputFile = outputFile.getAbsolutePath();

        // Delete the inputFile if we don't need it anymore
        if (inputFile != outputFile) {
            inputFile.delete();
        }

        // Rename the tempFile to outputFile name we just specified
        return tempFile.renameTo(outputFile);
    }

    /**
     * @param args
     */
    public static void main(java.lang.String[] args) throws Exception {
        // Some classes to help us
        CommandLineParser clp = new GnuParser();
        HelpFormatter helpf = new HelpFormatter();
        CommandLine cl;

        // The input file
        String inputFile = "";
        String filename = "";

        String outputDirectory = "";
        String configFile = "";
        String imports = "";
        boolean runDeepRoots = false;
        boolean stdout = true;
        ArrayList<String> outputFormats = new ArrayList<String>();
        outputFormats.add("N3");
        outputFormats.add("N-TRIPLE");
        outputFormats.add("RDF/XML");
        outputFormats.add("TURTLE");

        // Define our commandline options
        Options options = new Options();
        options.addOption("h", "help", false, "print this help message and exit");
        options.addOption("o", "outputDirectory", true, "Output Directory");
        options.addOption("i", "inputFile", true, "Input Spreadsheet");
        options.addOption("c", "configFile", true, "Use a local config file instead of getting from server");
//        options.addOption("deepRoots", true, "run deepRoots while triplifying");
        options.addOption("w", "writeFile", false, "Don't use stdout, instead print to file and return location. Uses the filename from the inputFile and the outputDirectory.");
        options.addOption("I", "imports", true, "Specify a file to import an ontology into file.");
        options.addOption("F", "format", true, "output format of the triplification process: N3, N-TRIPLE, TURTLE, RDF/XML --TURTLE is default.");
        options.addOption("prefix", true, "Set the default local URI prefix.");

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
            if (!(new File(outputDirectory)).exists()) {
                System.out.println("output directory " + outputDirectory + "does not exist!");
                return;
            }
        }
        if (cl.hasOption("prefix")) {
            defaultLocalURIPrefix = cl.getOptionValue("prefix");
        }
        if (cl.hasOption("w")) {
            stdout = false;
        }

        if (cl.hasOption("i")) {
            inputFile = cl.getOptionValue("i");
            File inputFileFile = new File(inputFile);
            if (!inputFileFile.exists()) {
                System.out.println("input file " + inputFile + " does not exist!");
                return;
            }
            filename = inputFileFile.getName();
        }
        if (cl.hasOption("I")) {
            imports = cl.getOptionValue("I");
        }
        // output Format
        if (cl.hasOption("F")) {
            outputFormat = cl.getOptionValue("F");
        }
        if (!outputFormats.contains(outputFormat)) {
            FimsPrinter.out.println("Error: invalid output Format");
            helpf.printHelp("fims ", options, true);
            return;
        }
        if (cl.hasOption("configFile")) {
            configFile = cl.getOptionValue("configFile");
            if (!(new File(configFile).exists())) {
                System.out.println("configuration file does not exist!");
                return;
            }
        }


        if (configFile.isEmpty() || outputDirectory.isEmpty() || inputFile.isEmpty()) {
            FimsPrinter.out.println("Incorrect options");
            helpf.printHelp("fims ", options, true);

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

        ArrayNode fimsMetadata = new JsonTabularDataConverter(tdr).convert(
                mapping.getDefaultSheetAttributes(),
                mapping.getDefaultSheetName()
        );

        boolean isValid = validation.run(tdr, "test", outputDirectory, mapping, fimsMetadata, mapping.getDefaultSheetName());

        // add messages to process controller and print
        processController.addMessages(validation.getMessages());

        if (isValid) {
            Triplifier t = new Triplifier(
                    filename,
                    outputDirectory,
                    processController,
                    true,
                    defaultLocalURIPrefix,
                    outputFormat);

            // TODO: come up with a more generic way to set prefixes. for now, these are hardcoded
            t.setPrefixes(
                    "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
                            "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n" +
                            "@prefix ark: <http://biscicol.org/id/ark:> .\n" +
                            "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n" +
                            "@prefix dwc: <http://rs.tdwg.org/dwc/terms/> . \n" +
                            "@prefix dc: <http://purl.org/dc/elements/1.1/> .\n" +
                            "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n" +
                            "@prefix ppo: <http://www.plantphenology.org/id/> .\n" +
                            "@prefix obo: <http://purl.obolibrary.org/obo/> .\n");
            // Add the imports declaration using N3 syntax
            // TODO: figure out a more standards-worthy way to designate the import declaration instance identifier
            // TODO: enable multiple imports
            if (!imports.equals("")) {
                t.setImports(
                        "<urn:importInstance> " +
                                "owl:imports <" + imports + "> .\n");
            }

            ObjectNode resource = (ObjectNode) fimsMetadata.get(0);
            t.run(validation.getSqliteFile(), Lists.newArrayList(resource.fieldNames()));

            if (stdout) {
                try (BufferedReader br = new BufferedReader(new FileReader(t.getTripleOutputFile()))) {
                    String line = null;
                    while ((line = br.readLine()) != null) {
                        System.out.println(line);
                    }
                }
            } else {
                System.out.println("    writing " + t.getTripleOutputFile());
            }
        } else {
            //System.err.println(processController.getMessages().toString());
            System.err.println(processController.printMessages());
            System.err.println("    unable to create triples until errors are fixed");
        }
    }


}
