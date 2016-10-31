package biocode.fims.fuseki.triplify;

import biocode.fims.digester.*;
import biocode.fims.fimsExceptions.ServerErrorException;
import biocode.fims.settings.Connection;
import biocode.fims.settings.DBsystem;
import org.apache.commons.digester3.Rules;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * This class has the necessary functions for generating the D2RQ Mapping Language representation of
 * a Mapping's connection, entites, and relations.
 */
public class D2RQPrinter {
    private static List<Rule> rules;
    private static Validation validation;

    /**
     * Generate D2RQ Mapping Language representation of this Mapping's connection, entities and relations.
     */
    public static void printD2RQ(List<String> colNames, Mapping mapping, Validation pValidation, File d2rqMappingFile, Connection connection) {
        validation = pValidation;
        rules = validation.getWorksheets().getFirst().getRules();

        try (PrintWriter pw = new PrintWriter(d2rqMappingFile)) {
            printPrefixes(pw);
            printConnectionD2RQ(pw, connection);
            for (Entity entity : mapping.getEntities())
                printEntityD2RQ(pw, entity, colNames);
            for (Relation relation : mapping.getRelations()) {
                printRelationD2RQ(pw, relation, mapping);
            }
        } catch (FileNotFoundException e) {
            throw new ServerErrorException(e);
        }
    }

    /**
     * getting a translation table looks up values in a list that have
     * defined_by in the list and translates the actual values to the
     * defined_by values, which is useful in RDF mappings.
     *
     * @param columnName
     *
     * @return
     */
    private static String getTranslationTable(String columnName) {
        StringBuilder sb = new StringBuilder();
        sb.append("map:" + columnName + "TranslationTable a d2rq:TranslationTable;\n");
        Iterator it = rules.iterator();
        while (it.hasNext()) {
            Rule r = (Rule) it.next();
            String listName = null;
            if (columnName.equals(r.getColumn()) &&
                    (r.getType().equals("controlledVocabulary") ||
                            r.getType().equals("checkInXMLFields"))) {
                listName = r.getList();
                // Loop all the lists
                Iterator lists = validation.getLists().iterator();
                while (lists.hasNext()) {
                    biocode.fims.digester.List l = (biocode.fims.digester.List) lists.next();
                    if (l.getAlias().equals(listName)) {
                        // loop all the list Elements
                        Iterator listElements = l.getFields().iterator();
                        while (listElements.hasNext()) {
                            Field f = (Field) listElements.next();
                            if (!(f.getDefined_by() == null)) {
                                sb.append("\td2rq:translation " +
                                        "[d2rq:databaseValue \"" + f.getValue() + "\"; " +
                                        "d2rq:rdfValue <" + f.getDefined_by() + ">];\n");
                            }
                        }
                        sb.append("\t.");
                        // Only valid return element is here, after a single list has been looped
                        return sb.toString();
                    }
                }

            }
        }
        return null;
    }

    /**
     * Generate D2RQ Mapping Language representation of this Relation.
     */
    private static void printRelationD2RQ(PrintWriter pw, Relation relation, Mapping mapping) {

        Entity subjEntity = mapping.findEntity(relation.getSubject());
        Entity objEntity = mapping.findEntity(relation.getObject());

        if (subjEntity == null || objEntity == null)
            return;

        String subjClassMap = getClassMap(subjEntity);
        String objClassMap = getClassMap(objEntity);

        pw.println("map:" + subjClassMap + "_" + objClassMap + "_rel" + " a d2rq:PropertyBridge;");
        pw.println("\td2rq:belongsToClassMap " + "map:" + subjClassMap + ";");
        pw.println("\td2rq:property <" + relation.getPredicate() + ">;");
        pw.println(getPersistentIdentifierMapping(subjEntity, objEntity));
        //pw.println(printCondition(objEntity.getWorksheetUniqueKey()));
        //pw.println("\td2rq:condition \"" + objEntity.getWorksheetUniqueKey() + " <> ''\";");
        pw.println("\t.");
    }

    /**
     * Generate D2RQ Mapping Language ClassMap name for a given Entity.
     *
     * @return D2RQ Mapping ClassMap name.
     */
    private static String getClassMap(Entity entity) {
        return entity.getWorksheet() + "_" + entity.getWorksheetUniqueKey() + "_" + entity.getConceptAlias();
    }

    /**
     * Generate D2RQ Mapping Language representation of this Entity with Attributes.
     */
    private static void printEntityD2RQ(PrintWriter pw, Entity entity, List<String> colNames) {
        pw.println("map:" + getClassMap(entity) + " a d2rq:ClassMap;");
        pw.println("\td2rq:dataStorage " + "map:database;");

        // HACK--- hardcoding for testing
        /*if (entity.getConceptURI().equalsIgnoreCase("http://purl.obolibrary.org/obo/PPO_0000001")) {
            //pw.println(getPersistentIdentifierMapping(null, entity));
            // HACK -- a duplicate uriPattern when calling the above function, hardcoding
            // the URI pattern below....
            pw.println("\td2rq:uriPattern \"urn:x-bsc:wppsCM:@@Samples.wholePlantPhenologicalStageHASH@@\";\n");
            //pw.println("\td2rq:uriColumn \"Samples.Phenophase_Description\";\n");
            // Use a translation table to t
            String translationTable = getTranslationTable("Phenophase_Description");
            // Complete the propertyBridge appropriately before defining the translation table
            if (translationTable != null) {
                pw.println("\td2rq:translateWith map:Phenophase_DescriptionTranslationTable;\n");
            }
            pw.println("\t.");

            // Now define the translation Table
            if (translationTable != null) {
                // HACK -- don't print the translation table since i know it already exists!
                pw.println(translationTable);
            }

        } else {
        */
        pw.println(getPersistentIdentifierMapping(null, entity));
        pw.println("\td2rq:class <" + entity.getConceptURI() + ">;");
        pw.println("\t.");
        // }

        // Get a list of colNames that we know are good from the spreadsheet
        // Normalize the column names so they can be mapped according to dhow they appear in SQLite
        ArrayList<String> normalizedColNames = new ArrayList<String>();
        Iterator it = colNames.iterator();
        while (it.hasNext()) {
            String colName = (String) it.next();
            normalizedColNames.add(colName.replace(" ", "_").replace("/", ""));
        }

        // Loop through attributes associated with this Entity
        if (entity.getAttributes().size() > 0) {
            for (Attribute attribute : entity.getAttributes())
                printAttributeD2RQ(pw, attribute, entity, normalizedColNames);
        }
    }

    /**
     * All conditions that require some value, use this convenience method
     * which strips off all references to BNODE uniquekey
     *
     * @param columnName
     *
     * @return
     */
    private static String printCondition(String columnName) {
        // BNODE columns do not actually have BNODE on the end, so remove it
        String actualColumnName = columnName.split("BNODE")[0];

        return "\td2rq:condition \"" + actualColumnName + " <> ''\";";
    }

    /**
     * * Generate D2RQ Mapping Language representation of this Attribute.
     *
     * @param parent
     * @param colNames
     */
    private static void printAttributeD2RQ(PrintWriter pw, Attribute attribute, Entity parent, List<String> colNames) {

        String classMap = getClassMap(parent);
        String table = parent.getWorksheet();
        String classMapStringEquivalence = "";

        Boolean runColumn = false;

        if (colNames.contains(attribute.getColumn())) {
            runColumn = true;
        }

        // Only print this column if it is in a list of colNames
        if (runColumn) {
            StringBuilder sb = new StringBuilder();
            String classMapString = "map:" + classMap + "_" + attribute.getColumn();
            sb.append(classMapString + " a d2rq:PropertyBridge;\n");
            sb.append("\td2rq:belongsToClassMap " + "map:" + classMap + ";\n");
            sb.append(printCondition(table + "." + attribute.getColumn()) + "\n");
            // Specify an equivalence, which is isDefinedBy
            classMapStringEquivalence = classMapString + "_Equivalence";
            sb.append("\td2rq:additionalPropertyDefinitionProperty " + classMapStringEquivalence + ";\n");

            // Get a translation, if appropriate
            String translationTable = getTranslationTable(attribute.getColumn());
            if (translationTable != null) {
                // Print the property bridge spec that references the translation table
                StringBuilder translationTableSB = sb;
                translationTableSB.append("\td2rq:property <" + attribute.getUri() + ">;\n");
                translationTableSB.append("\td2rq:translateWith map:" + attribute.getColumn() + "TranslationTable;\n");
                translationTableSB.append("\td2rq:uriColumn \"" + table + "." + attribute.getColumn() + "\";\n");
                translationTableSB.append("\t.\n");
                pw.println(translationTableSB.toString());

                // Print the translation table itself
                pw.println(translationTable);

                // Now print a regular version of this column
                StringBuilder sb2 = new StringBuilder();
                sb2.append("map:" + classMap + "_" + attribute.getColumn() + "Label a d2rq:PropertyBridge;\n");
                sb2.append("\td2rq:belongsToClassMap " + "map:" + classMap + ";\n");
                sb2.append(printCondition(table + "." + attribute.getColumn()) + "\n");
                sb2.append("\td2rq:column \"" + table + "." + attribute.getColumn() + "\";\n");
                // This version, with translation Table should just be a Label
                sb2.append("\td2rq:property <rdfs:Label>;\n");
                sb2.append("\t.\n");
                pw.println(sb2.toString());
            }
            // Al other cases just specify the column and finish
            else {
                sb.append("\td2rq:property <" + attribute.getUri() + ">;\n");
                sb.append("\td2rq:column \"" + table + "." + attribute.getColumn() + "\";\n");
                sb.append("\t.\n");
                pw.println(sb.toString());
            }

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
                            pw.println(printCondition(table + "." + columns[i]));
                        //pw.println("\td2rq:condition \"" + table + "." + columns[i] + " <> ''\";");
                        result.append(columns[i]);
                    }
                    result.append("\";");
                }

                // This is the YMD case using a very special SQLIte function to format data
                // Assume that columns are Year, Month, and Day EXACTLY
                else if (attribute.getType().equals("ymd")) {
                    // Require Year
                    pw.println(printCondition(table + "." + columns[0]));
                    //pw.println("\td2rq:condition \"" + table + "." + columns[0] + " <> ''\";");

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
    private static void printConnectionD2RQ(PrintWriter pw, Connection connection) {
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
    private static void printPrefixes(PrintWriter pw) {
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
     * @param subjEntity
     * @param objEntity
     *
     * @return
     */
    private static String getPersistentIdentifierMapping(Entity subjEntity, Entity objEntity) {
        String identifier = String.valueOf(objEntity.getIdentifier());

        // apply the scheme urn: and the x-factor sub-scheme biscicol for
        // all identifiers that we have no information for.
        if (identifier == null || identifier.equals("null")) {
            identifier = "urn:x-biscicol:" + objEntity.getConceptAlias() + ":";
        }

        // work with BNODE specification as the persistent identifier mapping
        if (objEntity.getWorksheetUniqueKey().contains("BNODE")) {
            // If there is a subject Entity then we want to specify this is the relationship type
            // This is the case when constructing property bridges so we say that this is
            // referring to a particular classmap
            if (subjEntity != null) {
                return "\td2rq:refersToClassMap map:" + getClassMap(objEntity) + ";";
            }
            // If there is no subjectEntity then this is a classmap definition
            else {
                // Get all of the attributes that make up this bNode... important that
                // we're explicit with the attributes so we know that this bNode is unique
                LinkedList bNodeAttributes = objEntity.getAttributes();
                Iterator it = bNodeAttributes.iterator();
                StringBuilder columnNames = new StringBuilder();
                while (it.hasNext()) {
                    // if this is the 2nd column name encountered, append a comma
                    if (columnNames.length() > 0) columnNames.append(",");
                    Attribute attribute = (Attribute) it.next();

                    columnNames.append(objEntity.getWorksheet() + "." + attribute.getColumn());
                }
                return "\td2rq:bNodeIdColumns \"" + columnNames + "\";";
            }
        }
        // Any other identifier type besides bNode uses uriPattern to define the actual URI
        // Also, any other identifier type should have a condition associated with it
        else {
            String returnValue = "\td2rq:uriPattern \"" + identifier + "@@" + objEntity.getColumn() + "@@\";";
            // ensures non-null values ... don't apply if this is a hash
            // otherwise, print a conditional statement
            if (!objEntity.getColumn().toLowerCase().contains("hash")) {
                returnValue += printCondition(objEntity.getColumn());
            }
            return returnValue;

        }

    }
}
