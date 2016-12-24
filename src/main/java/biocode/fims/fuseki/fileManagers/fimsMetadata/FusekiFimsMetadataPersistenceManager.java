package biocode.fims.fuseki.fileManagers.fimsMetadata;

import biocode.fims.entities.Bcid;
import biocode.fims.fileManagers.fimsMetadata.AbstractFimsMetadataPersistenceManager;
import biocode.fims.fileManagers.fimsMetadata.FimsMetadataPersistenceManager;
import biocode.fims.fuseki.Uploader;
import biocode.fims.fuseki.query.FimsQueryBuilder;
import biocode.fims.fuseki.triplify.Triplifier;
import biocode.fims.run.ProcessController;
import biocode.fims.service.BcidService;
import biocode.fims.service.ExpeditionService;
import biocode.fims.settings.SettingsManager;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.util.*;

/**
 * {@link FimsMetadataPersistenceManager} for Fuseki tdb
 */
public class FusekiFimsMetadataPersistenceManager extends AbstractFimsMetadataPersistenceManager implements FimsMetadataPersistenceManager {
    private final ExpeditionService expeditionService;
    private final BcidService bcidService;
    private String graph;
    private String webAddress;
    private JSONArray dataset;

    @Autowired
    public FusekiFimsMetadataPersistenceManager(ExpeditionService expeditionService, BcidService bcidService,
                                                SettingsManager settingsManager) {
        super(settingsManager);
        this.expeditionService = expeditionService;
        this.bcidService = bcidService;
    }

    @Override
    public void upload(ProcessController processController, JSONArray dataset, String filename) {
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
        JSONObject resource = (JSONObject) dataset.get(0);
        triplifier.run(processController.getValidation().getSqliteFile(), new ArrayList<>(resource.keySet()));

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
        List<Bcid> datasetBcids = bcidService.getFimsMetadataDatasets(processController.getProjectId(), processController.getExpeditionCode());

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
