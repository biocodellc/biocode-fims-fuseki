package biocode.fims.fuseki.query.elasticSearch;

import biocode.fims.application.config.FimsAppConfig;
import biocode.fims.config.ConfigurationFileFetcher;
import biocode.fims.digester.Mapping;
import biocode.fims.entities.Expedition;
import biocode.fims.entities.Project;
import biocode.fims.fileManagers.dataset.Dataset;
import biocode.fims.fileManagers.dataset.DatasetFileManager;
import biocode.fims.query.elasticSearch.ElasticSearchIndexer;
import biocode.fims.run.ProcessController;
import biocode.fims.service.ProjectService;
import biocode.fims.settings.FimsPrinter;
import biocode.fims.settings.StandardPrinter;
import org.apache.commons.cli.*;
import org.elasticsearch.client.Client;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;

/**
 * class for indexing datasets already loaded into Fuseki
 */
public class FusekiIndexer {
    private final Client esClient;
    private final ProjectService projectService;
    private final DatasetFileManager datasetFileManager;

    @Autowired
    public FusekiIndexer(Client esClient, ProjectService projectService, DatasetFileManager datasetFileManager) {
        this.esClient = esClient;
        this.projectService = projectService;
        this.datasetFileManager = datasetFileManager;
    }

    public void index(int projectId, String outputDirectory) {
        Project project = projectService.getProjectWithExpeditions(projectId);

        File configFile = new ConfigurationFileFetcher(projectId, outputDirectory, true).getOutputFile();

        Mapping mapping = new Mapping();
        mapping.addMappingRules(configFile);

        // we need to fetch each Expedition individually as the SheetUniqueKey is only unique on the Expedition level
        for (Expedition expedition : project.getExpeditions()) {

            ProcessController processController = new ProcessController(projectId, expedition.getExpeditionCode());
            processController.setOutputFolder(outputDirectory);
            processController.setMapping(mapping);
            datasetFileManager.setProcessController(processController);


            System.out.println("\nQuerying expedition: " + expedition.getExpeditionCode() + "\n");

            Dataset dataset = datasetFileManager.getDataset();

            System.out.println("\nIndexing results ....\n");

            ElasticSearchIndexer indexer = new ElasticSearchIndexer(esClient);
            indexer.indexDataset(
                    project.getProjectId(),
                    expedition.getExpeditionCode(),
                    mapping.getDefaultSheetUniqueKey(),
                    dataset.getSamples()
            );

        }
    }

    public static void main(String[] args) {
        ApplicationContext applicationContext = new AnnotationConfigApplicationContext(FimsAppConfig.class);
        FusekiIndexer fusekiIndexer = applicationContext.getBean(FusekiIndexer.class);

        int projectId = 0;
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
        options.addOption("p", "project_id", true, "Project Identifier.  A numeric integer corresponding to your project");
        options.addOption("o", "output_directory", true, "Output Directory");

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
        }
        if (cl.hasOption("o")) {
            output_directory = cl.getOptionValue("o");
        }

        fusekiIndexer.index(projectId, output_directory);
    }
}
