package org.kettle.beam.steps.firestore;

import java.util.List;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.BeamDefaults;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaSerializable;
import org.pentaho.di.core.row.value.ValueMetaString;
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
 * Firestore Output.
 *
 * @author Renato Dornelas Cardoso <renato@romaconsulting.com.br>
 */
@Step(
        id = BeamConst.STRING_BEAM_FIRESTORE_OUTPUT_PLUGIN_ID,
        name = "Beam GCP Firestore : Output",
        description = "Output data from GCP Firestore",
        image = "beam-gcp-firestore-output.svg",
        categoryDescription = "Big Data"
)
public class BeamFirestoreOutputMeta extends BaseStepMeta implements StepMetaInterface {

    public static final String PROJECT_ID = "projectId";
    public static final String KIND = "kind";
    public static final String KEY_FIELD = "key";
    public static final String JSON_FIELD = "json";

    private String projectId;
    private String kind;
    private String keyField;
    private String jsonField;

    /**
     * Construtor padrão
     */
    public BeamFirestoreOutputMeta() {
        super();
    }

    /**
     * Setando valores padrões da Janela.
     */
    @Override
    public void setDefault() {
        this.projectId = "";
        this.kind = "Entidade";
        this.keyField = "id";
        this.jsonField = "";
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
        if (BeamConst.STRING_BEAM_FIRESTORE_OUTPUT_PLUGIN_ID.equalsIgnoreCase(stepMeta.getStepID())) {
            step = new BeamFirestoreOutput(stepMeta, stepDataInterface, copyNr, transMeta, trans);
        }
        return step;
    }

    /**
     * Etapa de apontamento da calsse Data.
     * @return
     */
    @Override
    public StepDataInterface getStepData() {
        return new BeamFirestoreOutputData();
    }

    /**
     * Apontamento para classe responsável pela Janela de Dialog do componente
     *
     * @return
     */
    @Override
    public String getDialogClassName() {
        return BeamFirestoreOutputDialog.class.getName();
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
        if ( StringUtils.isEmpty(this.kind) ) {
            throw new KettleStepException( "Campo 'kind' não especificado." );
        }
        if ( StringUtils.isEmpty(this.keyField) ) {
            throw new KettleStepException( "Campo Chave não especificado." );
        }

        String kind = space.environmentSubstitute(this.kind);
        String keyField = space.environmentSubstitute(this.keyField);
        String jsonField = space.environmentSubstitute(this.jsonField);

        ValueMetaInterface valueMeta;

        valueMeta = new ValueMetaString(kind);
        valueMeta.setOrigin(name);
        inputRowMeta.addValueMeta(valueMeta);

        valueMeta = new ValueMetaString(keyField);
        valueMeta.setOrigin(name);
        inputRowMeta.addValueMeta(valueMeta);

        valueMeta = new ValueMetaString(jsonField);
        valueMeta.setOrigin(name);
        inputRowMeta.addValueMeta(valueMeta);
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
        xml.append(XMLHandler.addTagValue(PROJECT_ID, this.projectId));
        xml.append(XMLHandler.addTagValue(KIND, this.kind));
        xml.append(XMLHandler.addTagValue(KEY_FIELD, this.keyField));
        xml.append(XMLHandler.addTagValue(JSON_FIELD, this.jsonField));
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
        this.projectId = XMLHandler.getTagValue(stepnode, PROJECT_ID);
        this.kind = XMLHandler.getTagValue(stepnode, KIND);
        this.keyField = XMLHandler.getTagValue(stepnode, KEY_FIELD);
        this.jsonField = XMLHandler.getTagValue(stepnode, JSON_FIELD);
    }


    /**
     * Gets project id
     *
     * @return value of project id
     */
    public String getProjectId() {
        return this.projectId;
    }

    /**
     * @param value The project id to set
     */
    public void setProjectId(String value) {
        this.projectId = value;
    }

    /**
     * Gets kind
     *
     * @return value of kind
     */
    public String getKind() {
        return this.kind;
    }

    /**
     * @param value The kind to set
     */
    public void setKind(String value) {
        this.kind = value;
    }

    /**
     * Gets key
     *
     * @return value of key
     */
    public String getKeyField() {
        return this.keyField;
    }

    /**
     * @param value The key to set
     */
    public void setKeyField(String value) {
        this.keyField = value;
    }



    /**
     * Gets key
     *
     * @return value of key
     */
    public String getJsonField() {
        return this.jsonField;
    }

    /**
     * @param value The key to set
     */
    public void setJsonField(String value) {
        this.jsonField = value;
    }

}
