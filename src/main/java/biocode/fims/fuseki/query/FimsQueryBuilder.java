package biocode.fims.fuseki.query;

import biocode.fims.digester.Mapping;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import biocode.fims.run.TemplateProcessor;
import biocode.fims.settings.PathManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Class for building queries against FIMS Database
 * <p>
 * The FIMS Database is a fuseki triplestore composed of many small graphs, grouped by project.
 * <p>
 * The order of operations for querying the FIMS Database looks like this:
 * <p>
 * 1. Do a simple (?s ?p ?o) query of all graphs of a specified set and return a Model
 * 2. Query the returned model and run filter statements on it.
 * 3. Loop through specified properties (or not) and call the FimsModel (using Excel data structure)
 * <p>
 * This approach is actually MORE efficient than using just one SPARQL query on multiple graphs.
 */
public class FimsQueryBuilder {
    private final static Logger logger = LoggerFactory.getLogger(FimsQueryBuilder.class);
    String graphArray[];
    Mapping mapping;
    String sparqlServer;
    private final int naan;
    String outputDirectory;// = System.getProperty("user.dir") + File.separator + "tripleOutput";

    // ArrayList of filter conditions
    private ArrayList<FimsFilterCondition> filterArrayList = new ArrayList<FimsFilterCondition>();

    public FimsQueryBuilder(Mapping mapping, String[] graphArray, String outputDirectory, int naan) {
        this.mapping = mapping;
        this.outputDirectory = outputDirectory;

        this.graphArray = graphArray;

        // Build the "query" location for SPARQL queries
        sparqlServer = mapping.getMetadata().getQueryTarget().toString() + "/query";
        this.naan = naan;
    }

    /**
     * Add a single Filter condition
     *
     * @param f
     */
    public void addFilter(FimsFilterCondition f) {
        if (f != null)
            filterArrayList.add(f);
    }

    /**
     * Add multipler Filter conditions
     *
     * @param f
     */
    public void addFilter(ArrayList<FimsFilterCondition> f) {
        if (f != null && f.size() > 0)
            filterArrayList.addAll(f);
    }


    /**
     * Query a Model and  pass in Filter conditions and then return another model with those conditions applied
     *
     * @param model
     * @return
     */
    public Model getFilteredModel(Model model) {
        String queryString = "CONSTRUCT {?s ?p ?o} \n" +
                //buildFromStatement() +
                "WHERE {\n" +
                "   ?s a <http://www.w3.org/2000/01/rdf-schema#Resource> . \n" +
                "   ?s ?p ?o . \n" +
                buildFilterStatements() +
                "}";

        logger.debug(queryString);
        QueryExecution qexec = QueryExecutionFactory.create(queryString, model);
        Model outputModel = qexec.execConstruct();

        qexec.close();
        return outputModel;
    }

    /**
     * Build the model by using the CONSTRUCT statement
     *
     * @return
     */
    public Model getModel() {
        String queryString = "CONSTRUCT {?s ?p ?o} \n" +
                buildFromStatement() +
                "WHERE {\n" +
                "   ?s a <http://www.w3.org/2000/01/rdf-schema#Resource> . \n" +
                "   ?s ?p ?o . \n" +
                "}";

        logger.debug(queryString);
        QueryExecution qexec = QueryExecutionFactory.sparqlService(sparqlServer, queryString);
        Model model = qexec.execConstruct();
        qexec.close();
        return model;
    }

    /**
     * Take the filter statements that the user has specified and put them together to form the portion of the SPARQL
     * statement that asks particular questions of the data.
     *
     * @return
     */
    private String buildFilterStatements() {
        StringBuilder sb = new StringBuilder();
        Iterator filterArrayListIt = filterArrayList.iterator();
        int count = 1;
        while (filterArrayListIt.hasNext()) {
            FimsFilterCondition f = (FimsFilterCondition) filterArrayListIt.next();

            // The FimsFilterCondition uriProperty corresponds to the uri value in the configuration file
            if (f.uriProperty == null) {
                if (f.value != null) {
                    sb.append("\t?s ?propertyFilter" + count + " ?objectFilter" + count + " . \n");
                    sb.append("\tFILTER regex(?objectFilter" + count + ",\"" + f.value + "\") . \n");
                }
            } else {
                if (f.value != null) {
                    sb.append("\t?s <" + f.uriProperty.toString() + "> \"" + f.value + "\" .\n");
                }
            }
            count++;
            // TODO: the current filter statement only builds AND conditions, need to account for OR and NOT
        }
        return sb.toString();
    }

    /**
     * build the FROM statements
     *
     * @return
     */
    private String buildFromStatement() {
        StringBuilder sb = new StringBuilder();

        for (String graph : graphArray) {
            if (graph != null)
                sb.append(" FROM <" + graph + "> \n");
        }
        return sb.toString();
    }

    /**
     * Print the Model
     *
     * @param model
     */
    public static void print(Model model) {
        // list the statements in the Model
        StmtIterator iter = model.listStatements();

        // print out the predicate, subject and object of each statement
        while (iter.hasNext()) {
            Statement stmt = iter.nextStatement();  // get next statement
            Resource subject = stmt.getSubject();     // get the subject
            Property predicate = stmt.getPredicate();   // get the predicate
            RDFNode object = stmt.getObject();      // get the object

            System.out.print(subject.toString());
            System.out.print(" " + predicate.toString() + " ");
            if (object instanceof Resource) {
                System.out.print(object.toString());
            } else {
                // object is a literal
                System.out.print(" \"" + object.toString() + "\"");
            }

            System.out.println(" .");
        }
    }

    public String queryHtml() {
        FimsModel fimsModel = run();
        String filepath = fimsModel.writeHTML(PathManager.createUniqueFile("output.html", outputDirectory));

        fimsModel.close();
        return filepath;
    }

    public ArrayNode getJSON() {
        FimsModel fimsModel = run();

        ArrayNode json = fimsModel.getJSON();

        fimsModel.close();
        return json;
    }

    public String writeKML() {
        FimsModel fimsModel = run();
        String filepath = fimsModel.writeKML(PathManager.createUniqueFile("output.kml", outputDirectory));

        fimsModel.close();
        return filepath;
    }

    public String writeTAB() {
        FimsModel fimsModel = run();
        String filepath = fimsModel.writeTAB(PathManager.createUniqueFile("output.tsv", outputDirectory));

        fimsModel.close();
        return filepath;
    }

    public String writeCSV(boolean getOnlySpecifiedProperties) {
        FimsModel fimsModel = run(getOnlySpecifiedProperties);
        String filepath = fimsModel.writeCSV(PathManager.createUniqueFile("output.csv", outputDirectory));

        fimsModel.close();
        return filepath;
    }

    public String writeExcel(int projectId) {
        FimsModel fimsModel = run();
        String outputPath = fimsModel.writeExcel(PathManager.createUniqueFile("output.xlsx", outputDirectory));

        // Here we attach the other components of the excel sheet found with
        XSSFWorkbook justData = null;
        try {
            justData = new XSSFWorkbook(new FileInputStream(outputPath));
        } catch (IOException e) {
            logger.error("", e);
        }

        TemplateProcessor t = new TemplateProcessor(projectId, outputDirectory, justData, naan);
        String filepath = t.createExcelFileFromExistingSources("Samples", outputDirectory).getAbsolutePath();

        fimsModel.close();
        return filepath;
    }

    private FimsModel run() {
        /* Set the flag of whether to look at only specified properties (from configuration file)
        when returning data or filtering */
        return run(true);
    }
    /**
     * builds the Model from all the information we have collected by specifying the SPARQL query
     * service endpoint and filter conditions.  It builds a "FimsModel" from which we can re-direct to the specified
     * formats.
     * @return
     */
    private FimsModel run(boolean getOnlySpecifiedProperties) {
        FimsModel fimsModel;

        // Construct a FimsModel, wrapping a filtered model around a model only when necessary
        if (filterArrayList.size() > 0) {
            fimsModel = getFIMSModel(getFilteredModel(getModel()), getOnlySpecifiedProperties);
        } else {
            fimsModel = getFIMSModel(getModel(), getOnlySpecifiedProperties);
        }

        return fimsModel;
   }

    /**
     * Used only for testing this class directly
     *
     * @param args
     */
    public static void main(String[] args) {
        String output_directory = System.getProperty("user.dir") + File.separator + "tripleOutput";
        int projectId = 1;
        ArrayList<FimsFilterCondition> filters = new ArrayList<FimsFilterCondition>();
        //String graphs;

//        // configuration file
//        File file = new ConfigurationFileFetcher(projectId, output_directory, false).getOutputFile();
//
//        Process p = new Process(
//                projectId,
//                output_directory,
//                file
//        );


        // Build the query Object
//         FimsQueryBuilder q = new FimsQueryBuilder(p, graphArray, outputDirectory);
//        FimsQueryBuilder q = new FimsQueryBuilder(p, getAllGraphs(5), output_directory);

        // Add filter conditions to the object
        //q.addFilter(new FimsFilterCondition(new URI("urn:phylum"), "Echinodermata", FimsFilterCondition.AND));
        //q.addFilter(new FimsFilterCondition(new URI("urn:materialSampleID"), "IN0123.01", FimsFilterCondition.AND));


//        q.addFilter(new FimsFilterCondition(null, "Magnoliophyta", FimsFilterCondition.AND));
        //q.addFilter(new FimsFilterCondition(null, "Kimana", FimsFilterCondition.AND));


        // TODO: clean up the filter conditions here to handle all cases
        //q.addFilter(new FimsFilterCondition(null, "10", FimsFilterCondition.AND));

        //FimsModel FimsModel = new FimsModel(q.getModel());


        // Run the query and specify the output format
//        String outputFileLocation = q.run("json");

        // Print out the file location
//        System.out.println("File location: " + outputFileLocation);
    }

    /**
     * Create a FimsModel to store the results from this query
     */
    public FimsModel getFIMSModel(Model model, boolean getOnlySpecifiedProperties) {

        // Create a queryWriter object
        QueryWriter queryWriter = new QueryWriter(
                mapping.getDefaultSheetAttributes(),
                mapping.getDefaultSheetName());

        // Construct the FIMS model
        FimsModel fimsModel = new FimsModel(
                model,
                queryWriter,
                mapping,
                getOnlySpecifiedProperties);

        // Read rows starting at the Resource node
        fimsModel.readRows("http://www.w3.org/2000/01/rdf-schema#Resource");

        // Return the FimsModel
        return fimsModel;
    }


}