package biocode.fims.fuseki.triplify;

import biocode.fims.fimsExceptions.FimsRuntimeException;
import biocode.fims.settings.PathManager;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.ontology.OntResource;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.util.FileUtils;
import de.fuberlin.wiwiss.d2rq.jena.ModelD2RQ;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

import com.hp.hpl.jena.vocabulary.RDFS;
import org.semarglproject.vocab.RDF;


/**
 * Created by jdeck on 12/9/16.
 */
public class ProcessD2RQFile {

    private Model model;
    private String outputLanguage = FileUtils.langNTriple;
    String outputFolder = "/Users/jdeck/IdeaProjects/biocode-fims-fuseki/output";


    /**
     * Return triples
     *
     * @return
     */
    private void getTriples(String mappingFilepath) {

        System.out.println("\tWriting Temporary Output ...");
        String filenamePrefix = "output";

        // Write the model

        String mappingFilePathStringURL = FileUtils.toURL(mappingFilepath);
        String serializationFormat = FileUtils.langN3;
        model = new ModelD2RQ(
                mappingFilePathStringURL,
                serializationFormat,
                "urn:x-biscicol:");
        model.setNsPrefix("ark", "http://ezid.cdlib.org/id/ark");



        // Write the model
        File tripleFile = PathManager.createUniqueFile(filenamePrefix + ".n3", outputFolder);
        try {
            FileOutputStream fos = new FileOutputStream(tripleFile);

            model.write(fos, getOutputLanguage(), null);
            fos.close();
        } catch (FileNotFoundException e) {
            throw new FimsRuntimeException(500, e);
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            cleanPropertyExpressions(tripleFile);
        } catch (IOException e) {
            throw new FimsRuntimeException(500, e);
        }


        System.out.println("triple output file: " + tripleFile.getAbsolutePath());

        if (tripleFile.length() < 1)
            throw new FimsRuntimeException("No triples to write!", 500);
    }

    public String getOutputLanguage() {
        return outputLanguage;
    }

    public void setOutputLanguage(String outputLanguage) {
        this.outputLanguage = outputLanguage;
    }

    public static void main(String[] args) {
        ProcessD2RQFile p = new ProcessD2RQFile();
        p.getTriples(p.outputFolder + "/npn_test8_output.mapping.n3");
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
            //else {
            //    System.out.println("removing "  + currentLine);
            //}
        }
        writer.close();
        reader.close();
        reader2.close();
        return tempFile.renameTo(inputFile);
    }
}
