package biocode.fims.fuseki.query.elasticSearch;

import biocode.fims.entities.Bcid;
import biocode.fims.entities.Expedition;
import biocode.fims.entities.Project;
import biocode.fims.fimsExceptions.ServerErrorException;
import biocode.fims.fuseki.query.FimsQueryBuilder;
import biocode.fims.query.elasticSearch.ElasticSearchIndexer;
import biocode.fims.run.Process;
import biocode.fims.run.ProcessController;
import biocode.fims.service.BcidService;
import biocode.fims.service.ExpeditionService;
import biocode.fims.service.ProjectService;
import biocode.fims.settings.FimsPrinter;
import biocode.fims.settings.StandardPrinter;
import org.apache.commons.cli.*;
import org.elasticsearch.client.Client;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * class for indexing datasets already loaded into Fuseki
 */
public class FusekiIndexer {
    private final Client esClient;
    private final BcidService bcidService;
    private final ExpeditionService expeditionService;
    private final ProjectService projectService;

    @Autowired
    public FusekiIndexer(Client esClient, BcidService bcidService, ExpeditionService expeditionService,
                         ProjectService projectService) {
        this.esClient = esClient;
        this.bcidService = bcidService;
        this.expeditionService = expeditionService;
        this.projectService = projectService;
    }

    public void index(int projectId, String outputDirectory) {
        JSONArray dataset;
        Project project = projectService.getProjectWithExpeditions(projectId);

        ProcessController pc = new ProcessController(projectId, null);
        Process p = new Process(outputDirectory, pc, expeditionService);

        // we need to fetch each Expedition individually as the SheetUniqueKey is only unique on the Expedition level
        for (Expedition expedition : project.getExpeditions()) {
            String[] graph = new String[1];
            Bcid bcid = bcidService.getLatestDatasetForExpedition(expedition);
            graph[0] = bcid.getGraph();

            System.out.println("\nQuerying expedition: " + expedition.getExpeditionId() + "\n");
            // Build the Query
            FimsQueryBuilder q = new FimsQueryBuilder(p.getMapping(), p.configFile, graph, outputDirectory);

            try {
                dataset = (JSONArray) new JSONParser().parse(q.run("esJSON", pc.getProjectId()));
            } catch (org.json.simple.parser.ParseException e) {
                throw new ServerErrorException(e);
            }

            System.out.println("\nIndexing results ....\n");

            ElasticSearchIndexer indexer = new ElasticSearchIndexer(esClient);
            indexer.bulkIndex(
                    project.getProjectId(), String.valueOf(expedition.getExpeditionId()),
                    p.getMapping().getDefaultSheetUniqueKey(), dataset
            );

        }
    }

    public static void main(String[] args) {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("/applicationContext.xml");
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
