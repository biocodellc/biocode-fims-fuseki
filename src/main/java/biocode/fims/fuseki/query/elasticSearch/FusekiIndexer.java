package biocode.fims.fuseki.query.elasticSearch;

import biocode.fims.application.config.FimsAppConfig;
import biocode.fims.config.ConfigurationFileFetcher;
import biocode.fims.digester.Mapping;
import biocode.fims.entities.Expedition;
import biocode.fims.entities.Project;
import biocode.fims.fileManagers.fimsMetadata.FimsMetadataFileManager;
import biocode.fims.elasticSearch.ElasticSearchIndexer;
import biocode.fims.fuseki.fileManagers.fimsMetadata.FusekiFimsMetadataPersistenceManager;
import biocode.fims.run.ProcessController;
import biocode.fims.service.BcidService;
import biocode.fims.service.ExpeditionService;
import biocode.fims.service.ProjectService;
import biocode.fims.settings.FimsPrinter;
import biocode.fims.settings.SettingsManager;
import biocode.fims.settings.StandardPrinter;
import org.apache.commons.cli.*;
import org.elasticsearch.client.Client;
import org.json.simple.JSONArray;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.File;
import java.util.List;

/**
 * class for indexing datasets already loaded into Fuseki
 */
public class FusekiIndexer {
    private final Client esClient;
    private final ProjectService projectService;
    private ExpeditionService expeditionService;
    private BcidService bcidService;

    public FusekiIndexer(Client esClient, ProjectService projectService, ExpeditionService expeditionService, BcidService bcidService) {
        this.esClient = esClient;
        this.projectService = projectService;
        this.expeditionService = expeditionService;
        this.bcidService = bcidService;
    }

    public void index(int projectId, String outputDirectory) {
        Project project = projectService.getProjectWithExpeditions(projectId);

        File configFile = new ConfigurationFileFetcher(projectId, outputDirectory, true).getOutputFile();

        Mapping mapping = new Mapping();
        mapping.addMappingRules(configFile);

        // we need to fetch each Expedition individually as the SheetUniqueKey is only unique on the Expedition level
        for (Expedition expedition : project.getExpeditions()) {

            FusekiFimsMetadataPersistenceManager persistenceManager = new FusekiFimsMetadataPersistenceManager(expeditionService, bcidService);
            FimsMetadataFileManager fimsMetadataFileManager = new FimsMetadataFileManager(
                    persistenceManager, SettingsManager.getInstance(), expeditionService, bcidService);

            ProcessController processController = new ProcessController(projectId, expedition.getExpeditionCode());
            processController.setOutputFolder(outputDirectory);
            processController.setMapping(mapping);
            fimsMetadataFileManager.setProcessController(processController);


            System.out.println("\nQuerying expedition: " + expedition.getExpeditionCode() + "\n");

            JSONArray fimsMetadata = fimsMetadataFileManager.index();

            System.out.println("\nIndexing results ....\n");

            ElasticSearchIndexer indexer = new ElasticSearchIndexer(esClient);
            indexer.indexDataset(
                    project.getProjectId(),
                    expedition.getExpeditionCode(),
                    fimsMetadata
            );

        }
    }

    public static void main(String[] args) throws Exception {
        ApplicationContext applicationContext = new AnnotationConfigApplicationContext(FimsAppConfig.class);
        Client esClient = applicationContext.getBean(Client.class);
        ProjectService projectService = applicationContext.getBean(ProjectService.class);
        ExpeditionService expeditionService = applicationContext.getBean(ExpeditionService.class);
        BcidService bcidService = applicationContext.getBean(BcidService.class);

        int projectId = 0;
        boolean allProjects = false;
        String output_directory = "tripleOutput/";

        // Direct output using the standardPrinter subClass of fimsPrinter which send to fimsPrinter.out (for command-line usage)
        FimsPrinter.out = new StandardPrinter();

        // Some classes to help us
        CommandLineParser clp = new GnuParser();
        HelpFormatter helpf = new HelpFormatter();
        CommandLine cl;

        // Define our commandline options
        Options options = new Options();
        options.addOption("h", "help", false, "print this help message and exit");
        options.addOption("p", "project_id", false, "Project Identifier.  A numeric integer corresponding to your project");
        options.addOption("o", "output_directory", true, "Output Directory");
        options.addOption("--allProjects", "all_projects", false, "Output Directory");

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

        if (cl.hasOption("p")) {
            projectId = Integer.parseInt(cl.getOptionValue("p"));
        } else if (cl.hasOption("--allProjects")) {
            allProjects = true;
        } else {
            throw new Exception("either ProjectId or --allProjects is required");
        }
        if (cl.hasOption("o")) {
            output_directory = cl.getOptionValue("o");
        }

        FusekiIndexer fusekiIndexer = new FusekiIndexer(esClient, projectService, expeditionService, bcidService);

        if (allProjects) {
            List<Project> projectList = projectService.getProjects();

            for (Project project: projectList) {
                fusekiIndexer.index(project.getProjectId(), output_directory);
            }
        } else {
            fusekiIndexer.index(projectId, output_directory);
        }
    }
}
