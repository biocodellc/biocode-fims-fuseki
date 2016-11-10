package biocode.fims.fuseki;

import java.util.Iterator;
import com.hp.hpl.jena.ontology.*;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;

public class findRelationsAlternateMethod
{
    /***********************************/
    /* Constants                       */
    /***********************************/
    public static String BASE = "http://purl.obolibrary.org/obo/ppo-simple.owl";
    public static String owlFile = "file:///Users/jdeck/IdeaProjects/ppo_fims/ontology/ppo_simple_inferred.owl";
    public static String NS = BASE + "/";

    /***********************************/
    /* External signature methods      */
    /***********************************/

    public static void main( String[] args ) {
        new findRelationsAlternateMethod().run();
    }

    public void run() {
        OntModel m = getPizzaOntology();

        OntClass american = m.getOntClass( NS + "PPO_0001006" );

        for (Iterator<OntClass> supers = american.listSuperClasses(); supers.hasNext(); ) {
            displayType( supers.next() );
        }
    }

    /***********************************/
    /* Internal implementation methods */
    /***********************************/

    protected OntModel getPizzaOntology() {
        OntModel m = ModelFactory.createOntologyModel( OntModelSpec.OWL_MEM );
        m.setStrictMode(false);
        m.read( owlFile,"Turtle" );
        return m;
    }

    protected void displayType( OntClass sup ) {
        if (sup.isRestriction()) {
            displayRestriction( sup.asRestriction() );
        }
    }

    protected void displayRestriction( Restriction sup ) {
        if (sup.isAllValuesFromRestriction()) {
            displayRestriction( "all", sup.getOnProperty(), sup.asAllValuesFromRestriction().getAllValuesFrom() );
        }
        else if (sup.isSomeValuesFromRestriction()) {
            displayRestriction( "some", sup.getOnProperty(), sup.asSomeValuesFromRestriction().getSomeValuesFrom() );
        }
    }

    protected void displayRestriction( String qualifier, OntProperty onP, Resource constraint ) {
        String out = String.format( "%s %s %s",
                                    qualifier, renderURI( onP ), renderConstraint( constraint ) );
        System.out.println( "restriction: " + out );
    }

    protected Object renderConstraint( Resource constraint ) {
        if (constraint.canAs( UnionClass.class )) {
            UnionClass uc = constraint.as( UnionClass.class );
            // this would be so much easier in ruby ...
            String r = "union{ ";
            for (Iterator<? extends OntClass> i = uc.listOperands(); i.hasNext(); ) {
                r = r + " " + renderURI( i.next() );
            }
            return r + "}";
        }
        else {
            return renderURI( constraint );
        }
    }

    protected Object renderURI( Resource onP ) {
        String qName = onP.getModel().qnameFor( onP.getURI() );
        return qName == null ? onP.getLocalName() : qName;
    }
}