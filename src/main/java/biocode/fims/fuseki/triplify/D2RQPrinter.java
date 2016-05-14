package biocode.fims.fuseki.triplify;

import biocode.fims.fuseki.deepRoots.DeepRoots;
import biocode.fims.digester.Attribute;
import biocode.fims.digester.Entity;
import biocode.fims.digester.Mapping;
import biocode.fims.digester.Relation;
import biocode.fims.run.ProcessController;
import biocode.fims.settings.Connection;
import biocode.fims.settings.DBsystem;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class has the necessary functions for generating the D2RQ Mapping Language representation of
 * a Mapping's connection, entites, and relations.
 */
public class D2RQPrinter {
    private PrintWriter pw;
    private Connection connection;
    private Mapping mapping;
    private DeepRoots dRoots = null;
    private List<String> colNames;
    private ProcessController processController;

    public D2RQPrinter(Mapping mapping, PrintWriter pw, Connection connection, DeepRoots dRoots, ProcessController processController) {
        this.mapping = mapping;
        this.pw = pw;
        this.connection = connection;
        this.dRoots = dRoots;
        this.processController = processController;
    }

    /**
     * Generate D2RQ Mapping Language representation of this Mapping's connection, entities and relations.
     */
    public void printD2RQ(List<String> colNames) {
        this.colNames = colNames;
        printPrefixes();
        printConnectionD2RQ();
        for (Entity entity : mapping.getEntities())
            printEntityD2RQ(entity);
        for (Relation relation : mapping.getRelations())
            printRelationD2RQ(relation);
    }

    /**
     * Generate D2RQ Mapping Language representation of this Relation.
     *
     */
    public void printRelationD2RQ(Relation relation) {

        Entity subjEntity = mapping.findEntity(relation.getSubject());
        Entity objEntity = mapping.findEntity(relation.getObject());

        if (subjEntity == null || objEntity == null)
            return;

        String subjClassMap = getClassMap(subjEntity);
        String objClassMap = getClassMap(objEntity);

        pw.println("map:" + subjClassMap + "_" + objClassMap + "_rel" + " a d2rq:PropertyBridge;");
        pw.println("\td2rq:belongsToClassMap " + "map:" + subjClassMap + ";");
        pw.println("\td2rq:property <" + relation.getPredicate() + ">;");
        pw.println(getPersistentIdentifier(objEntity));
        pw.println("\td2rq:condition \"" + objEntity.getWorksheetUniqueKey() + " <> ''\";");
        pw.println("\t.");
    }

    /**
     * Generate D2RQ Mapping Language ClassMap name for a given Entity.
     *
     * @return D2RQ Mapping ClassMap name.
     */
    private String getClassMap(Entity entity) {
        return entity.getWorksheet() + "_" + entity.getWorksheetUniqueKey() + "_" + entity.getConceptAlias();
    }

    /**
     * Generate D2RQ Mapping Language representation of this Entity with Attributes.
     */
    private void printEntityD2RQ(Entity entity) {
        pw.println("map:" + getClassMap(entity) + " a d2rq:ClassMap;");
        pw.println("\td2rq:dataStorage " + "map:database;");
        pw.println(getPersistentIdentifier(entity));
        pw.println("\td2rq:class <" + entity.getConceptURI() + ">;");
        // ensures non-null values ... don't apply if this is a hash
        if (!entity.getColumn().contains("hash"))
            pw.println("\td2rq:condition \"" + entity.getColumn() + " <> ''\";");

        // TODO: add in extra conditions (May not be necessary)
        //pw.println(getExtraConditions());
        pw.println("\t.");

        // Get a list of colNames that we know are good from the spreadsheet
        // Normalize the column names so they can be mapped according to how they appear in SQLite
        ArrayList<String> normalizedColNames = new ArrayList<String>();
        Iterator it = colNames.iterator();
        while (it.hasNext()) {
            String colName = (String) it.next();
            normalizedColNames.add(colName.replace(" ", "_").replace("/", ""));
        }

        // Loop through attributes associated with this Entity
        if (entity.getAttributes().size() > 0) {
            for (Attribute attribute : entity.getAttributes())
                printAttributeD2RQ(attribute, entity, normalizedColNames);
        }
    }

    /**
     * * Generate D2RQ Mapping Language representation of this Attribute.
     *
     * @param parent
     * @param colNames
     */
    public void printAttributeD2RQ(Attribute attribute, Entity parent, List<String> colNames) {

        String classMap = getClassMap(parent);
        String table = parent.getWorksheet();
        String classMapStringEquivalence = "";

        Boolean runColumn = false;

        if (colNames.contains(attribute.getColumn())) {
            runColumn = true;
        }

        // Only print this column if it is in a list of colNames
        if (runColumn) {
            String classMapString = "map:" + classMap + "_" + attribute.getColumn();
            pw.println(classMapString + " a d2rq:PropertyBridge;");
            pw.println("\td2rq:belongsToClassMap " + "map:" + classMap + ";");
            pw.println("\td2rq:property <" + attribute.getUri() + ">;");
            pw.println("\td2rq:column \"" + table + "." + attribute.getColumn() + "\";");
            pw.println("\td2rq:condition \"" + table + "." + attribute.getColumn() + " <> ''\";");
            // Specify an equivalence, which is isDefinedBy
            classMapStringEquivalence = classMapString + "_Equivalence";
            pw.println("\td2rq:additionalPropertyDefinitionProperty " + classMapStringEquivalence + ";");
            pw.println("\t.");

            // Always use isDefinedBy, even if the user has not expressed it explicitly.  We do this by
            // using the uri value if NO isDefinedBy is expressed.
            pw.println(classMapStringEquivalence + " a d2rq:AdditionalProperty;");
            pw.println("\td2rq:propertyName <" + attribute.getIsDefinedByURIString() + ">;");
            if (attribute.getDefined_by()!= null) {
                pw.println("\td2rq:propertyValue <" + attribute.getDefined_by() + ">;");
            } else {
                pw.println("\td2rq:propertyValue <" + attribute.getUri() + ">;");
            }
            pw.println("\t.");
            /*
           Loop multi-value columns
           This is used when the Configuration file indicates an attribute that should be composed of more than one column
            */
        } else if (attribute.getColumn().contains(",")) {

            // TODO: clean this up and integrate with above code.
            String tempColumnName = attribute.getColumn().replace(",", "");

            String[] columns = attribute.getColumn().split(",");

            // Check if we should run this -- all columns need to be present in colNames list
            Boolean runMultiValueColumn = true;
            for (int i = 0; i < columns.length; i++) {
                if (!colNames.contains(columns[i])) {
                    runMultiValueColumn = false;
                }
            }

            // Only run this portion if the tempColumnName appears
            if (runMultiValueColumn) {

                String classMapString = "map:" + classMap + "_" + tempColumnName;
                pw.println(classMapString + " a d2rq:PropertyBridge;");
                pw.println("\td2rq:belongsToClassMap " + "map:" + classMap + ";");
                pw.println("\td2rq:property <" + attribute.getUri() + ">;");

                // Construct SQL Expression
                StringBuilder result = new StringBuilder();

                // Call this a sqlExpression
                result.append("\td2rq:sqlExpression \"");

                // Append ALL columns together using the delimiter... ALL are required
                if (attribute.getType().equals("all")) {
                    for (int i = 0; i < columns.length; i++) {
                        if (i != 0)
                            result.append(" || '" + attribute.getDelimited_by() + "' || ");
                        // Set required function parameters
                        if (attribute.getType().equals("all"))
                            pw.println("\td2rq:condition \"" + table + "." + columns[i] + " <> ''\";");
                        result.append(columns[i]);
                    }
                    result.append("\";");
                }

                // This is the YMD case using a very special SQLIte function to format data
                // Assume that columns are Year, Month, and Day EXACTLY
                else if (attribute.getType().equals("ymd")) {
                    // Require Year
                    pw.println("\td2rq:condition \"" + table + "." + columns[0] + " <> ''\";");

                    result.append("yearCollected ||  ifnull(nullif('-'||substr('0'||monthCollected,-2,2),'-0') || " +
                            "ifnull(nullif('-'||substr('0'||dayCollected,-2,2),'-0'),'')" +
                            ",'') ");
                    result.append("\";");

                }

                pw.println(result.toString());

                //pw.println("\td2rq:column \"" + table + "." + column + "\";");
                //pw.println("\td2rq:condition \"" + table + "." + column + " <> ''\";");

                // Specify an equivalence, which is isDefinedBy
                classMapStringEquivalence = classMapString + "_Equivalence";
                pw.println("\td2rq:additionalPropertyDefinitionProperty " + classMapStringEquivalence + ";");
                pw.println("\t.");

                // Always use isDefinedBy, even if the user has not expressed it explicitly.  We do this by
                // using the uri value if NO isDefinedBy is expressed.
                pw.println(classMapStringEquivalence + " a d2rq:AdditionalProperty;");
                pw.println("\td2rq:propertyName <" + attribute.getIsDefinedByURIString() + ">;");
                if (attribute.getDefined_by() != null) {
                    pw.println("\td2rq:propertyValue <" + attribute.getDefined_by() + ">;");
                } else {
                    pw.println("\td2rq:propertyValue <" + attribute.getUri() + ">;");
                }
                pw.println("\t.");

            }
        }
    }


    /**
     * Generate D2RQ Mapping Language representation of this Connection.
     */
    public void printConnectionD2RQ() {
        pw.println("map:database a d2rq:Database;");
        pw.println("\td2rq:jdbcDriver \"" + connection.system.driver + "\";");
        pw.println("\td2rq:jdbcDSN \"" + connection.getJdbcUrl() + "\";");
        if (connection.username != null && !connection.username.isEmpty())
            pw.println("\td2rq:username \"" + connection.username + "\";");
        if (connection.password != null && !connection.password.isEmpty())
            pw.println("\td2rq:password \"" + connection.password + "\";");
        pw.println("\td2rq:fetchSize \"" + (connection.system == DBsystem.mysql ? Integer.MIN_VALUE : 500) + "\";");
        pw.println("\t.");
    }

    /**
     * Generate all possible RDF prefixes.
     */
    private void printPrefixes() {
        // TODO: Allow configuration files to specify namespace prefixes!
        pw.println("@prefix map: <" + "" + "> .");
        pw.println("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .");
        pw.println("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .");
        pw.println("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .");
        pw.println("@prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> .");
        pw.println("@prefix jdbc: <http://d2rq.org/terms/jdbc/> .");
        pw.println("@prefix ro: <http://www.obofoundry.org/ro/ro.owl#> .");
        pw.println("@prefix bsc: <http://biscicol.org/terms/index.html#> .");
        pw.println("@prefix urn: <http://biscicol.org/terms/index.html#> .");
        // TODO: update this prefix to EZID location when suffixPassthrough is ready
        pw.println("@prefix ark: <http://biscicol.org/id/ark:> .");


        pw.println();
    }

    /**
     * Sets the URI as a identifier to a column, or not, according to D2RQ conventions
     *
     * @param entity
     * @return
     */
    private String getPersistentIdentifier(Entity entity) {

        String columnName = "@@" + entity.getColumn() + "@@";

        // Use the DeepRoots System to lookup Key
        String bcid = null;
        if (dRoots != null) {
            bcid = dRoots.lookupPrefix(entity, processController.getUser().getUserId());
        }

        // Use the default namespace value if dRoots is unsuccesful...
        if (bcid == null) {
            bcid = "urn:x-biscicol:" + entity.getConceptAlias() + ":";
        }

        //System.out.println("\td2rq:uriPattern \"" + Bcid + columnName + "\";");
        return "\td2rq:uriPattern \"" + bcid + columnName + "\";";
    }
}
