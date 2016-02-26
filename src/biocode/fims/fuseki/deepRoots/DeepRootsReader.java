package biocode.fims.fuseki.deepRoots;

import biocode.fims.bcid.ExpeditionMinter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
 * A sample for reading a deepLinks data file in JSON.  The following libraries are required:
 * commons-beanutils-1.8.3.jar
 * commons-collections-3.2.1.jar
 * commons-lang-2.6.jar
 * commons-logging-1.1.jar
 * ezmorph-1.0.6.jar
 * json-lib-2.4-jdk15.jar
 */
public class DeepRootsReader {

    public DeepRoots createRootData(Integer projectId, String expeditionCode) {
        // Create the deepLinks.rootData Class
        DeepRoots rootData = new DeepRoots(projectId, expeditionCode);
        // Get deepLinks json object
        // TODO pass in the deepRoots data from the frontend class
        ExpeditionMinter expeditionMinter = new ExpeditionMinter();
        JSONObject deepRoots = expeditionMinter.getDeepRoots(expeditionCode, projectId);
        expeditionMinter.close();

        rootData.setDescription(((JSONObject) deepRoots.get("metadata")).get("expeditionTitle").toString());
        rootData.setDate(((JSONObject) deepRoots.get("metadata")).get("date").toString());
        rootData.setShortName(((JSONObject) deepRoots.get("metadata")).get("expeditionCode").toString());

        // Create the Hashmap to store in the deepLinks.rootData class
        HashMap<String, String> data = new HashMap<String, String>();
        // Loop the data elements
        for (Object d : ((JSONArray) deepRoots.get("data"))) {
            JSONObject dataObject = (JSONObject) d;
            String title = (String) dataObject.get("title");
            String identifier = (String) dataObject.get("identifier");
            data.put(title, identifier);
        }
        rootData.setData(data);
        // Assign the actual data to the deepLinks.rootData element

        return rootData;
    }

    /**
     * Main method used for local testing
     *
     * @param args
     */
    public static void main(String[] args) {
        DeepRootsReader reader = new DeepRootsReader();
        // Some path name to the file
        String filePath = "file:////Users/jdeck/IdeaProjects/bcid/src/deepRoots/test.json";
        // Creating the object
       DeepRoots rootData = reader.createRootData(5,"TEST214");
        // Output for testing
        System.out.println(rootData.toString());
    }

}
