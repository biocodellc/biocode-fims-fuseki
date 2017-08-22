package biocode.fims.fuseki.fileManagers.fimsMetadata;

import biocode.fims.application.config.FimsProperties;
import biocode.fims.entities.BcidTmp;
import biocode.fims.fileManagers.fimsMetadata.AbstractFimsMetadataPersistenceManager;
import biocode.fims.fileManagers.fimsMetadata.FimsMetadataPersistenceManager;
import biocode.fims.fuseki.Uploader;
import biocode.fims.fuseki.query.FimsQueryBuilder;
import biocode.fims.fuseki.triplify.Triplifier;
import biocode.fims.rest.SpringObjectMapper;
import biocode.fims.run.ProcessController;
import biocode.fims.service.BcidService;
import biocode.fims.service.ExpeditionService;
import biocode.fims.settings.SettingsManager;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Lists;
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
    private ArrayNode dataset;

    @Autowired
    public FusekiFimsMetadataPersistenceManager(ExpeditionService expeditionService, BcidService bcidService,
                                                FimsProperties props) {
        super(props);
        this.expeditionService = expeditionService;
        this.bcidService = bcidService;
    }

    @Override
    public void upload(ProcessController processController, ArrayNode dataset, String filename) {
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
        triplifier.run(processController.getValidation().getSqliteFile(), Lists.newArrayList(dataset.get(0).fieldNames()));

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
    public ArrayNode getDataset(ProcessController processController) {
        if (dataset == null) {
            dataset = fetchLatestDataset(processController);
        }

        return dataset;
    }

    private ArrayNode fetchLatestDataset(ProcessController processController) {
//        List<BcidTmp> datasetBcids = bcidService.getFimsMetadataDatasets(processController.getProjectId(), processController.getExpeditionCode());

        ArrayNode fimsMetadata = new SpringObjectMapper().createArrayNode();

//        if (!datasetBcids.isEmpty()) {
//            FimsQueryBuilder q = new FimsQueryBuilder(
//                    processController.getMapping(),
//                    new String[]{datasetBcids.get(0).getGraph()},
//                    processController.getOutputFolder());

//            fimsMetadata.addAll(q.getJSON());
//        }

        return fimsMetadata;
    }

    @Override
    public void deleteDataset(ProcessController processController) {
        // currently we don't want to delete anything from fuseki as we store every dataset version
    }
}
