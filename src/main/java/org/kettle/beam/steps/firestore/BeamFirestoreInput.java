package org.kettle.beam.steps.firestore;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import java.util.ArrayList;
import java.util.List;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 *
 * @author Thiago Teodoro Rodrigues <thiago.rodrigues@callink.com.br>
 */
public class BeamFirestoreInput extends BaseStep implements StepInterface {


  /**
   * This is the base step that forms that basis for all steps. You can derive from this class to implement your own
   * steps.
   *
   * @param stepMeta          The StepMeta object to run.
   * @param stepDataInterface the data object to store temporary data, database connections, caches, result sets,
   *                          hashtables etc.
   * @param copyNr            The copynumber for this step.
   * @param transMeta         The TransInfo of which the step stepMeta is part of.
   * @param trans             The (running) transformation to obtain information shared among the steps.
   */
  public BeamFirestoreInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    
      super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override 
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    // Outside of a Beam Runner this step doesn't actually do anything, it's just metadata
    // This step gets converted into Beam API calls in a pipeline
    //
    //return false;
    
//    Object[] row = getRow();
//    
//    if (row == null) {
//    
//        setOutputDone();
//        return false;
//    }
//    

    BeamFirestoreInputMeta metaData = (BeamFirestoreInputMeta)this.getStepMeta().getStepMetaInterface();
    System.out.println(BeamConst.STRING_BEAM_FIRESTORE_INPUT_PLUGIN_ID + "-> " + metaData.getEntity());
        
    // Obtendo instancia de operações com o Datastore.
    Datastore datastore = DatastoreOptions.getDefaultInstance().getService();

    String kind = "";
    
    // The kind for the new entity
    if(metaData.getEntity() != null){
        
        kind = metaData.getEntity();
        

        Query<Entity> query = Query.newEntityQueryBuilder()
                .setKind(kind)
                .build();

        QueryResults<Entity> results = datastore.run(query);
        
//        List<Entity> allData = new ArrayList<>();
        
        while (results.hasNext()) {
            
            Entity currentEntity = results.next();

//            allData.add(currentEntity);
        }
        
//        Object[] aux = new Object[allData.size()];
//        
//        for(int i = 0; i < allData.size(); i ++){
//            
//            aux[i] = allData.get(i);
//        }
//        
//        Object[] out_row = RowDataUtil.createResizedCopy(row, smi.getTableFields().size() - 1);
//
//        Object[] outputRow = RowDataUtil.addValueData(row, smi.getTableFields().size() - 1, "dummy value");        
//  
//        RowDataUtil.cr
//        
//        putRow(getInputRowMeta(), outputRow);   
    } else {
        
        log.logError("Kind/Entity vazia!");
        return false;
    }
        
    return true;
  }
}

