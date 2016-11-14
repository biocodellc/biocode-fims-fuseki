package biocode.fims.fuseki.fileManagers.dataset;

import biocode.fims.entities.Bcid;
import biocode.fims.fileManagers.dataset.DatasetPersistenceManager;
import biocode.fims.fuseki.Uploader;
import biocode.fims.fuseki.query.FimsQueryBuilder;
import biocode.fims.fuseki.triplify.Triplifier;
import biocode.fims.run.ProcessController;
import biocode.fims.service.BcidService;
import biocode.fims.service.ExpeditionService;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.util.*;

/**
 * {@link DatasetPersistenceManager} for Fuseki tdb
 */
public class FusekiDatasetPersistenceManager implements DatasetPersistenceManager {
    private final ExpeditionService expeditionService;
    private final BcidService bcidService;
    private String graph;
    private String webAddress;
    private JSONArray dataset;

    @Autowired
    public FusekiDatasetPersistenceManager(ExpeditionService expeditionService, BcidService bcidService) {
        this.expeditionService = expeditionService;
        this.bcidService = bcidService;
    }

    @Override
    public void upload(ProcessController processController, JSONArray dataset) {
        this.dataset = dataset;
        String outputPrefix = processController.getExpeditionCode() + "_output";

        // run the triplifier
        Triplifier triplifier = new Triplifier(outputPrefix, processController.getOutputFolder(), processController);

        expeditionService.setEntityIdentifiers(
                processController.getMapping(),
                processController.getExpeditionCode(),
                processController.getProjectId()
        );

        // the D2Rq mapping file must match the
        JSONObject sample = (JSONObject) dataset.get(0);
        triplifier.run(processController.getValidation().getSqliteFile(), new ArrayList<String>(sample.keySet()));

        // upload the dataset
        Uploader uploader = new Uploader(processController.getMapping().getMetadata().getTarget(),
                new File(triplifier.getTripleOutputFile()));

        uploader.execute();

        graph = uploader.getGraphID();
        webAddress = uploader.getEndpoint();
    }

    @Override
    public boolean validate(ProcessController processController) {
        return true;
    }

    @Override
    public String getWebAddress() {
        return webAddress;
    }

    @Override
    public String getGraph() {
        return graph;
    }

    @Override
    public JSONArray getDataset(ProcessController processController) {
        if (dataset == null) {
            dataset = fetchLatestDataset(processController);
        }

        return dataset;
    }

    private JSONArray fetchLatestDataset(ProcessController processController) {
        List<Bcid> datasetBcids = bcidService.getDatasets(processController.getProjectId(), processController.getExpeditionCode());

        JSONArray fimsMetadata = new JSONArray();

        if (!datasetBcids.isEmpty()) {
            FimsQueryBuilder q = new FimsQueryBuilder(
                    processController.getMapping(),
                    new String[]{datasetBcids.get(0).getGraph()},
                    processController.getOutputFolder());

            fimsMetadata.addAll(q.getJSON());
        }

        return fimsMetadata;
    }
}
