package biocode.fims.fuseki.triplify;

import biocode.fims.digester.*;
import biocode.fims.fimsExceptions.ServerErrorException;
import biocode.fims.settings.Connection;
import biocode.fims.settings.DBsystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * This class has the necessary functions for generating the D2RQ Mapping Language
 * File, which triplifies.
 */
public class D2RQPrinter {
    private static String defaultLocalURIPrefix;

    /**
     * Generate D2RQ Mapping Language representation of this Mapping's connection, entities and relations.
     */
    public static void printD2RQ(List<String> colNames, Mapping mapping, Validation pValidation, File d2rqMappingFile, Connection connection, String pdefaultLocalURIPrefix) {
        defaultLocalURIPrefix = pdefaultLocalURIPrefix;
        try (PrintWriter pw = new PrintWriter(d2rqMappingFile)) {
            printPrefixes(pw);
            printConnectionD2RQ(pw, connection);
            for (Entity entity : mapping.getEntities())
                // we only want to persist entities that have a worksheet in the tdb
                if (entity.hasWorksheet()) {
                    printEntityD2RQ(pw, entity, colNames, pValidation);
                }
            for (Relation relation : mapping.getRelations()) {
                printRelationD2RQ(pw, relation, mapping);
            }
        } catch (FileNotFoundException e) {
            throw new ServerErrorException(e);
        }
    }

    /**
     * Getting a translation table looks up values in a list that have
     * defined_by in a validation list element and translates the actual values to the
     * defined_by values, which is useful in RDF mappings.
     *
     * @param columnName
     *
     * @return
     */
    private static String getTranslationTable(String columnName, Validation validation) {
        StringBuilder sb = new StringBuilder();

        for (Rule r : validation.getWorksheets().getFirst().getRules()) {

            if (columnName.equals(r.getColumn()) &&
                    (r.getType().equals("controlledVocabulary") ||
                            r.getType().equals("checkInXMLFields"))) {

                biocode.fims.digester.List list = validation.findList(r.getList());

                if (list != null) {

                    for (Field f : list.getFields()) {
                        if (f.getDefined_by() != null) {
                            sb.append("\td2rq:translation " +
                                    "[d2rq:databaseValue \"" + f.getValue() + "\"; " +
                                    "d2rq:rdfValue <" + f.getDefined_by() + ">];\n");
                        }
                    }

                    // only return the translationTable if the list fields contain a defined_by
                    if (sb.length() > 0) {
                        sb.append("\t.");
                        return "map:" + columnName + "TranslationTable a d2rq:TranslationTable;\n" + sb.toString();
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

        if (subjEntity == null || !subjEntity.hasWorksheet() || objEntity == null)
            return;

        String subjClassMap = getClassMap(subjEntity);
        String objClassMap = getClassMap(objEntity);

        pw.println("map:" + subjClassMap + "_" + objClassMap + "_rel" + " a d2rq:PropertyBridge;");
        pw.println("\td2rq:belongsToClassMap " + "map:" + subjClassMap + ";");
        pw.println("\td2rq:property <" + relation.getPredicate() + ">;");
        pw.println(getPersistentIdentifierMapping(subjEntity, objEntity));
        //pw.println("\td2rq:additionalPropertyDefinitionProperty map:" + subjClassMap + "_" + objClassMap + "_rel_aPD");
        // Every relation expression should have an owl:ObjectProperty expressed
        pw.println("\td2rq:additionalPropertyDefinitionProperty map:owlobjectproperty;\n");

        pw.println("\t.");

        // For every relation expression, build an additional property to be sure we get an objectProperty expressed
        //pw.println("map:" + subjClassMap + "_" + objClassMap + "_rel_aPD a d2rq:AdditionalProperty;");
        //pw.println("\td2rq:propertyName <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>;");
        //pw.println("\td2rq:propertyValue <http://www.w3.org/2002/07/owl#ObjectProperty>;");
        //pw.println("\t.");

    }

    /**
     * Generate D2RQ Mapping Language ClassMap name for a given Entity.
     *
     * @return D2RQ Mapping ClassMap name.
     */
    private static String getClassMap(Entity entity) {
        return entity.getWorksheet() + "_" + entity.getUniqueKey() + "_" + entity.getConceptAlias();
    }

    /**
     * Generate D2RQ Mapping Language representation of this Entity with Attributes.
     * Note that i attempted translation Table mappings against ClassMap but
     * wasn't able to get it to work.
     *
     * @param pw
     * @param entity
     * @param colNames
     */
    private static void printEntityD2RQ(PrintWriter pw, Entity entity, List<String> colNames, Validation validation) {
        pw.println("map:" + getClassMap(entity) + " a d2rq:ClassMap;");
        pw.println("\td2rq:dataStorage " + "map:database;");
        pw.println(getPersistentIdentifierMapping(null, entity));
        pw.println("\td2rq:class <" + entity.getConceptURI() + ">;");
        pw.println("\t.");

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
                printAttributeD2RQ(pw, attribute, entity, normalizedColNames, validation);
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
     * Generate D2RQ Mapping Language representation of this Attribute.
     * Most attributes will be represented as  Property Bridges, that have
     * belong to a ClassMap
     *
     * @param parent
     * @param colNames
     */
    private static void printAttributeD2RQ(PrintWriter pw, Attribute attribute, Entity parent, List<String> colNames, Validation validation) {

        String classMap = getClassMap(parent);
        String table = parent.getWorksheet();
        String classMapStringEquivalence = "";

        // Check if this column name is good
        Boolean runColumn = false;
        if (colNames.contains(attribute.getColumn())) {
            if (!attribute.getColumn().contains(",")) {
                runColumn = true;
            }
        }


        if (runColumn) {
            // Define the start of a Property Bridge
            StringBuilder sb = new StringBuilder();
            String classMapString = "map:" + classMap + "_" + attribute.getColumn();
            sb.append(classMapString + " a d2rq:PropertyBridge;\n");
            sb.append("\td2rq:belongsToClassMap " + "map:" + classMap + ";\n");
            sb.append(printCondition(table + "." + attribute.getColumn()) + "\n");
            // Specify an equivalence, which is isDefinedBy
            classMapStringEquivalence = classMapString + "_Equivalence";
            sb.append("\td2rq:additionalPropertyDefinitionProperty " + classMapStringEquivalence + ";\n");

            // NOTE: all attribute columns should be datatype properties.  Adding this so these
            // are specified appropriately
            sb.append("\td2rq:additionalPropertyDefinitionProperty map:owldatatypeproperty;\n");

            // Get a translation.  If it is not null then process it
            String translationTable = getTranslationTable(attribute.getColumn(), validation);
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

                // Now print another Property Bridge to define outputs for rdfs:Label
                // if the user specified to not display this then we don't want the label
                if (attribute.getDisplayAnnotationProperty() != false) {
                    StringBuilder sb2 = new StringBuilder();
                    sb2.append("map:" + classMap + "_" + attribute.getColumn() + "Label a d2rq:PropertyBridge;\n");
                    sb2.append("\td2rq:belongsToClassMap " + "map:" + classMap + ";\n");
                    sb2.append(printCondition(table + "." + attribute.getColumn()) + "\n");
                    sb2.append("\td2rq:column \"" + table + "." + attribute.getColumn() + "\";\n");
                    // Set xsd datatype if it is available
                    if (attribute.getDatatype().toString() != null && !attribute.getDatatype().toString().equals("")) {
                        sb2.append("\td2rq:datatype xsd:" + attribute.getDatatype().toString().toLowerCase() + ";\n");
                    }
                    // This version, with translation Table should just be a Label
                    sb2.append("\td2rq:property <http://www.w3.org/2000/01/rdf-schema#comment>;\n");
                    sb2.append("\t.\n");
                    pw.println(sb2.toString());
                }
            }
            // If no translation table is found, just declare the property bridge
            else {
                // if this is not a translationTable and annotationproperty is false we can exit
                // don't create an attribute if createAnnotationProperty is false (the default is true)
                if (!attribute.getDisplayAnnotationProperty()) {
                    return;
                }
                sb.append("\td2rq:property <" + attribute.getUri() + ">;\n");
                sb.append("\td2rq:column \"" + table + "." + attribute.getColumn() + "\";\n");
                // Set xsd datatype if it is available
                if (attribute.getDatatype().toString() != null && !attribute.getDatatype().toString().equals("")) {
                    sb.append("\td2rq:datatype xsd:" + attribute.getDatatype().toString().toLowerCase() + ";\n");
                }
                sb.append("\t.\n");
                pw.println(sb.toString());
            }

            // Express an isDefinedBy additionalproperty if it differs from the URI itself
            //(!attribute.getDefined_by().equalsIgnoreCase(attribute.getUri()))) {
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


        // Putting DatatypeProperty and ObjectProperty mappings with the prefixes at the top
        // of the mapping file since they are used only once
        pw.println("map:owldatatypeproperty a d2rq:AdditionalProperty;\n");
        pw.println("\td2rq:propertyName rdf:type;\n");
        pw.println("\td2rq:propertyValue <http://www.w3.org/2002/07/owl#DatatypeProperty>;\n");
        pw.println(".\n\n");

        pw.println("map:owlobjectproperty a d2rq:AdditionalProperty;\n");
        pw.println("\td2rq:propertyName rdf:type;\n");
        pw.println("\td2rq:propertyValue <http://www.w3.org/2002/07/owl#ObjectProperty>;\n");
        pw.println(".\n\n");

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

        // apply the defaultLocalURIPrefix for all identifiers we don't have actual prefixes for
        if (identifier == null || identifier.equals("null")) {
            identifier = defaultLocalURIPrefix + "?" + objEntity.getConceptAlias() + "=";
        }

        // work with BNODE specification as the persistent identifier mapping
        if (objEntity.getUniqueKey().contains("BNODE")) {
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
            // Ensure that the ark identifier is prefixed appropriately
            //if (identifier.contains("ark") && !identifier.contains("n2t")) {
            //    identifier = "http://n2t.net/" + identifier;
            //}
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
