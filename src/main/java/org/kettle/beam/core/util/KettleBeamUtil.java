package org.kettle.beam.core.util;

import org.kettle.beam.core.KettleRow;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.core.xml.XMLHandlerCache;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;

public class KettleBeamUtil {

  public static final String createTargetTupleId(String stepname, String targetStepname){
    return stepname+" - TARGET - "+targetStepname;
  }

  public static final String createMainOutputTupleId(String stepname){
    return stepname+" - OUTPUT";
  }

  public static final String createInfoTupleId(String stepname, String infoStepname){
    return infoStepname+" - INFO - "+stepname;
  }

  public static final String createMainInputTupleId(String stepname){
    return stepname+" - INPUT";
  }


  public static final KettleRow copyKettleRow( KettleRow kettleRow, RowMetaInterface rowMeta ) throws KettleException {
    Object[] newRow = RowDataUtil.createResizedCopy(kettleRow.getRow(), rowMeta.size());
    return new KettleRow(newRow);
  }

  private static Object object = new Object();

  public static void loadStepMetadataFromXml( String stepname, StepMetaInterface stepMetaInterface, String stepMetaInterfaceXml, IMetaStore metaStore ) throws KettleException {
    synchronized ( object ) {
      Document stepDocument = XMLHandler.loadXMLString( stepMetaInterfaceXml );
      if ( stepDocument == null ) {
        throw new KettleException( "Unable to load step XML document from : " + stepMetaInterfaceXml );
      }
      Node stepNode = XMLHandler.getSubNode( stepDocument, StepMeta.XML_TAG );
      if ( stepNode == null ) {
        throw new KettleException( "Unable to find XML tag " + StepMeta.XML_TAG + " from : " + stepMetaInterfaceXml );
      }
      try {
        stepMetaInterface.loadXML( stepNode, new ArrayList<>(), metaStore );
      } catch ( Exception e ) {
        throw new KettleException( "There was an error loading step metadata information (loadXML) for step '" + stepname + "'", e );
      } finally {
        XMLHandlerCache.getInstance().clear();
      }
    }
  }
}
