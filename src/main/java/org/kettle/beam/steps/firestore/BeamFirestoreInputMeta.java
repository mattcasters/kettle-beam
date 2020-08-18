package org.kettle.beam.steps.firestore;

import java.util.List;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * Classe respnsável por gerencier os meta dados spoon para o componente
 * Firestore Input.
 * 
 * @author Thiago Teodoro Rodrigues <thiago.rodrigues@callink.com.br>
 */
@Step(
  id = BeamConst.STRING_BEAM_FIRESTORE_INPUT_PLUGIN_ID,
  name = "Beam GCP Firestore : Input",
  description = "Input data from GCP Firestore",
  image = "beam-gcp-firestore-input.svg",
  categoryDescription = "Big Data"
)
public class BeamFirestoreInputMeta extends BaseStepMeta implements StepMetaInterface {

    public static final String ENTITY = "entity";
    public static final String MESSAGE_TYPE = "message_type";
    public static final String MESSAGE_FIELD = "message_field";

    private String entity;
    private String messageType;
    private String messageField;

    /**
     * Construtor padrão
     */
    public BeamFirestoreInputMeta() {
        super();
    }

    /**
     * Setando valores padrões da Janela.
     */
    @Override
    public void setDefault() {
        entity = "Entity";
        messageType = "String";
        messageField = "message";
    }

    /**
     * Direcionamento de processamento.
     *
     * @param stepMeta
     * @param stepDataInterface
     * @param copyNr
     * @param transMeta
     * @param trans
     * @return
     */
    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        
        StepInterface step = null;        
        
        if (BeamConst.STRING_BEAM_FIRESTORE_INPUT_PLUGIN_ID.equalsIgnoreCase(stepMeta.getStepID())) {
                        
            step = new BeamFirestoreInput(stepMeta, stepDataInterface, copyNr, transMeta, trans);
        }
        
        return step;
    }
    
    /**
     * Etapa de apontamento da calsse Data.
     * @return 
     */
    @Override
    public StepDataInterface getStepData() {
                        
        return new BeamFirestoreInputData();
    }

    /**
     * Apontamento para classe responsável pela Janela de Dialog do componente
     * 
     * @return 
     */
    @Override
    public String getDialogClassName() {
                
        return BeamFirestoreInputDialog.class.getName();
    }

    /**
     * Etapa de apontamento outpout no padrão beam iguinoramos essa etapa.
     * 
     * @param inputRowMeta
     * @param name
     * @param info
     * @param nextStep
     * @param space
     * @param repository
     * @param metaStore
     * @throws KettleStepException 
     */
    @Override
    public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {

        // append the outputField to the output
        ValueMetaInterface v = new ValueMeta();
        v.setName("teste");
        v.setType(ValueMeta.TYPE_STRING);
        v.setTrimType(ValueMeta.TRIM_TYPE_BOTH);
        v.setOrigin("oriteg-Teste");

        inputRowMeta.addValueMeta(v);                
    }

    /**
     * Configurações referentes a tela.
     * 
     * @return
     * @throws KettleException 
     */
    @Override
    public String getXML() throws KettleException {
        
        StringBuffer xml = new StringBuffer();

        xml.append(XMLHandler.addTagValue(ENTITY, entity));
        xml.append(XMLHandler.addTagValue(MESSAGE_TYPE, messageType));
        xml.append(XMLHandler.addTagValue(MESSAGE_FIELD, messageField));
        return xml.toString();
    }

    /**
     * Load dos dados da Tela.
     * 
     * @param stepnode
     * @param databases
     * @param metaStore
     * @throws KettleXMLException 
     */
    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        
        entity = XMLHandler.getTagValue(stepnode, ENTITY);
        messageType = XMLHandler.getTagValue(stepnode, MESSAGE_TYPE);
        messageField = XMLHandler.getTagValue(stepnode, MESSAGE_FIELD);
    }

    /**
     * Gets entity
     *
     * @return value of entity
     */
    public String getEntity() {
        return entity;
    }

    /**
     * @param entity The entity to set
     */
    public void setEntity(String entity) {
        this.entity = entity;
    }

    /**
     * Gets messageType
     *
     * @return value of messageType
     */
    public String getMessageType() {
        return messageType;
    }

    /**
     * @param messageType The messageType to set
     */
    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    /**
     * Gets messageField
     *
     * @return value of messageField
     */
    public String getMessageField() {
        return messageField;
    }

    /**
     * @param messageField The messageField to set
     */
    public void setMessageField(String messageField) {
        this.messageField = messageField;
    }
}
