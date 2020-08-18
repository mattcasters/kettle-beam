package org.kettle.beam.steps.firestore;

import java.awt.Desktop;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.kettle.beam.core.BeamDefaults;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * Classe responsável por renderizar a tela de Dialogo de configuração do
 * componente.
 *
 * @author Thiago Teodoro Rodrigues <thiago.rodrigues@callink.com.br>
 */
public class BeamFirestoreInputDialog extends BaseStepDialog implements StepDialogInterface {

    private static Class<?> PKG = BeamFirestoreInput.class; // for i18n purposes, needed by Translator2!!
    private final BeamFirestoreInputMeta input;

    int middle;
    int margin;

    private TextVar wEntity;
    private Combo wMessageType;
    private TextVar wMessageField;

    /**
     * Construtor padrão
     *
     * @param parent
     * @param in
     * @param transMeta
     * @param sname
     */
    public BeamFirestoreInputDialog(Shell parent, Object in, TransMeta transMeta, String sname) {

        super(parent, (BaseStepMeta) in, transMeta, sname);
        input = (BeamFirestoreInputMeta) in;
    }

    /**
     * Método para abrir a tela.
     *
     * @return
     */
    public String open() {

        Shell parent = getParent();
        Display display = parent.getDisplay();

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
        props.setLook(shell);
        setShellImage(shell, input);

        changed = input.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(PKG, "BeamInputFirestore.DialogTitle"));

        middle = props.getMiddlePct();
        margin = Const.MARGIN;

        // Stepname line
        wlStepname = new Label(shell, SWT.RIGHT);
        wlStepname.setText(BaseMessages.getString(PKG, "System.Label.StepName"));
        props.setLook(wlStepname);
        fdlStepname = new FormData();
        fdlStepname.left = new FormAttachment(0, 0);
        fdlStepname.top = new FormAttachment(0, margin);
        fdlStepname.right = new FormAttachment(middle, -margin);
        wlStepname.setLayoutData(fdlStepname);
        wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wStepname.setText(stepname);
        props.setLook(wStepname);
        fdStepname = new FormData();
        fdStepname.left = new FormAttachment(middle, 0);
        fdStepname.top = new FormAttachment(wlStepname, 0, SWT.CENTER);
        fdStepname.right = new FormAttachment(100, 0);
        wStepname.setLayoutData(fdStepname);
        Control lastControl = wStepname;

        Label wlEntity = new Label(shell, SWT.RIGHT);
        wlEntity.setText(BaseMessages.getString(PKG, "BeamInputFirestore.Entity"));
        props.setLook(wlEntity);
        FormData fdlEntity = new FormData();
        fdlEntity.left = new FormAttachment(0, 0);
        fdlEntity.top = new FormAttachment(lastControl, margin);
        fdlEntity.right = new FormAttachment(middle, -margin);
        wlEntity.setLayoutData(fdlEntity);
        wEntity = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wEntity);
        FormData fdEntity = new FormData();
        fdEntity.left = new FormAttachment(middle, 0);
        fdEntity.top = new FormAttachment(wlEntity, 0, SWT.CENTER);
        fdEntity.right = new FormAttachment(100, 0);
        wEntity.setLayoutData(fdEntity);
        lastControl = wEntity;

        Label wlMessageType = new Label(shell, SWT.RIGHT);
        wlMessageType.setText(BaseMessages.getString(PKG, "BeamInputFirestore.MessageType"));
        props.setLook(wlMessageType);
        FormData fdlMessageType = new FormData();
        fdlMessageType.left = new FormAttachment(0, 0);
        fdlMessageType.top = new FormAttachment(lastControl, margin);
        fdlMessageType.right = new FormAttachment(middle, -margin);
        wlMessageType.setLayoutData(fdlMessageType);
        wMessageType = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wMessageType);
        wMessageType.setItems(BeamDefaults.FIRESTORE_MESSAGE_TYPES);
        FormData fdMessageType = new FormData();
        fdMessageType.left = new FormAttachment(middle, 0);
        fdMessageType.top = new FormAttachment(wlMessageType, 0, SWT.CENTER);
        fdMessageType.right = new FormAttachment(100, 0);
        wMessageType.setLayoutData(fdMessageType);
        lastControl = wMessageType;

        Label wlMessageField = new Label(shell, SWT.RIGHT);
        wlMessageField.setText(BaseMessages.getString(PKG, "BeamInputFirestore.MessageField"));
        props.setLook(wlMessageField);
        FormData fdlMessageField = new FormData();
        fdlMessageField.left = new FormAttachment(0, 0);
        fdlMessageField.top = new FormAttachment(lastControl, margin);
        fdlMessageField.right = new FormAttachment(middle, -margin);
        wlMessageField.setLayoutData(fdlMessageField);
        wMessageField = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wMessageField);
        FormData fdMessageField = new FormData();
        fdMessageField.left = new FormAttachment(middle, 0);
        fdMessageField.top = new FormAttachment(wlMessageField, 0, SWT.CENTER);
        fdMessageField.right = new FormAttachment(100, 0);
        wMessageField.setLayoutData(fdMessageField);
        lastControl = wMessageField;

        wOK = new Button(shell, SWT.PUSH);
        wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));

        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

        setButtonPositions(new Button[]{wOK, wCancel}, margin, lastControl);

        wOK.addListener(SWT.Selection, e -> ok());
        wCancel.addListener(SWT.Selection, e -> cancel());

        lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };

        wStepname.addSelectionListener(lsDef);
        wMessageType.addSelectionListener(lsDef);
        wEntity.addSelectionListener(lsDef);

        // Detect X or ALT-F4 or something that kills this window...
        shell.addListener(SWT.Close, e -> cancel());

        getData();
        setSize();
        input.setChanged(changed);

        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
        return stepname;
    }

    /**
     * Método responsável por criar e o botão de ajuda que quando clicado
     * abre uma pagína de ajuda para o componente.
     * 
     * @param shell
     * @param stepMeta
     * @param plugin
     * @return 
     */
    @Override
    protected Button createHelpButton(Shell shell, StepMeta stepMeta, PluginInterface plugin) {

        Button helpButton = new Button(shell, SWT.PUSH);

        helpButton.setText("?");

        helpButton.addListener(SWT.Selection, e ->
            openUrlHelp("https://callink.atlassian.net/wiki/spaces/CBD/pages/584548483/Step+Firestore+Datastore+Input")
        );
        return helpButton;
    }

    /**
     * Método responsável por abrir uma no Browser.
     */
    private void openUrlHelp(String url) {
        
        if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
            
            try {
                
                Desktop.getDesktop().browse(new URI(url));
            } catch (Exception ex) {;
            
                this.log.logDetailed(BeamConst.STRING_BEAM_FIRESTORE_INPUT_PLUGIN_ID + "-> Ocorreu um erro inesperado em openUrlHelp(). Exception : ", ex);
                Logger.getLogger(BeamFirestoreInputDialog.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Populate the widgets.
     */
    public void getData() {
        wStepname.setText(stepname);
        wEntity.setText(Const.NVL(input.getEntity(), ""));
        wMessageType.setText(Const.NVL(input.getMessageType(), ""));
        wMessageField.setText(Const.NVL(input.getMessageField(), ""));

        wStepname.selectAll();
        wStepname.setFocus();
    }

    /**
     * Botão de cancelamento.
     */
    private void cancel() {
        stepname = null;
        input.setChanged(changed);
        dispose();
    }

    /**
     * Botão de aceite.
     */
    private void ok() {
        
        if (Utils.isEmpty(wStepname.getText())) {
            return;
        }

        getInfo(input);

        dispose();
    }

    /**
     * Obtendo informações.
     *
     * @param in
     */
    private void getInfo(BeamFirestoreInputMeta in) {

        stepname = wStepname.getText(); // return value

        in.setEntity(wEntity.getText());
        in.setMessageType(wMessageType.getText());
        in.setMessageField(wMessageField.getText());

        input.setChanged();
    }

}
