package org.kettle.beam.steps.firestore;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
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

import java.awt.*;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BeamFirestoreOutputDialog extends BaseStepDialog implements StepDialogInterface {

    private static Class<?> PACKAGE = BeamFirestoreOutput.class; // for i18n purposes, needed by Translator2!!
    private final BeamFirestoreOutputMeta output;

    int middle;
    int margin;

    private TextVar wProjectId;
    private TextVar wKind;
    private TextVar wKeyField;
    private TextVar wJsonField;

    /**
     * Construtor padrão
     *
     * @param parent
     * @param in
     * @param transMeta
     * @param sname
     */
    public BeamFirestoreOutputDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta) in, transMeta, sname);
        output = (BeamFirestoreOutputMeta) in;
    }

    /**
     * Método para abrir a tela.
     *
     * @return
     */
    public String open() {

        Shell parent = getParent();
        Display display = parent.getDisplay();

        this.shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
        this.props.setLook(this.shell);
        setShellImage(this.shell, this.output);

        this.changed = this.output.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        this.shell.setLayout(formLayout);
        this.shell.setText(BaseMessages.getString(PACKAGE, "BeamOutputFirestore.DialogTitle"));

        this.middle = props.getMiddlePct();
        this.margin = Const.MARGIN;

        // Stepname line
        this.wlStepname = new Label(shell, SWT.RIGHT);
        this.wlStepname.setText(BaseMessages.getString(PACKAGE, "System.Label.StepName"));
        this.props.setLook(this.wlStepname);
        this.fdlStepname = new FormData();
        this.fdlStepname.left = new FormAttachment(0, 0);
        this.fdlStepname.top = new FormAttachment(0, this.margin);
        this.fdlStepname.right = new FormAttachment(this.middle, -this.margin);
        this.wlStepname.setLayoutData(this.fdlStepname);
        this.wStepname = new Text(this.shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        this.wStepname.setText(this.stepname);
        this.props.setLook(this.wStepname);
        this.fdStepname = new FormData();
        this.fdStepname.left = new FormAttachment(this.middle, 0);
        this.fdStepname.top = new FormAttachment(this.wlStepname, 0, SWT.CENTER);
        this.fdStepname.right = new FormAttachment(100, 0);
        this.wStepname.setLayoutData(this.fdStepname);
        Control lastControl = this.wStepname;

        Label wlProjectId = new Label(shell, SWT.RIGHT);
        wlProjectId.setText(BaseMessages.getString(PACKAGE, "BeamOutputFirestore.ProjectId"));
        this.props.setLook(wlProjectId);
        FormData fdlProjectId = new FormData();
        fdlProjectId.left = new FormAttachment(0, 0);
        fdlProjectId.top = new FormAttachment(lastControl, this.margin);
        fdlProjectId.right = new FormAttachment(this.middle, -this.margin);
        wlProjectId.setLayoutData(fdlProjectId);
        this.wProjectId = new TextVar(this.transMeta, this.shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        this.props.setLook(this.wProjectId);
        FormData fdProjectId = new FormData();
        fdProjectId.left = new FormAttachment(this.middle, 0);
        fdProjectId.top = new FormAttachment(wlProjectId, 0, SWT.CENTER);
        fdProjectId.right = new FormAttachment(100, 0);
        this.wProjectId.setLayoutData(fdProjectId);
        lastControl = this.wProjectId;

        Label wlKind = new Label(shell, SWT.RIGHT);
        wlKind.setText(BaseMessages.getString(PACKAGE, "BeamOutputFirestore.Kind"));
        this.props.setLook(wlKind);
        FormData fdlKind = new FormData();
        fdlKind.left = new FormAttachment(0, 0);
        fdlKind.top = new FormAttachment(lastControl, this.margin);
        fdlKind.right = new FormAttachment(this.middle, -this.margin);
        wlKind.setLayoutData(fdlKind);
        this.wKind = new TextVar(this.transMeta, this.shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        this.props.setLook(this.wKind);
        FormData fdKind = new FormData();
        fdKind.left = new FormAttachment(this.middle, 0);
        fdKind.top = new FormAttachment(wlKind, 0, SWT.CENTER);
        fdKind.right = new FormAttachment(100, 0);
        this.wKind.setLayoutData(fdKind);
        lastControl = this.wKind;

        Label wlKeyField = new Label(shell, SWT.RIGHT);
        wlKeyField.setText(BaseMessages.getString(PACKAGE, "BeamOutputFirestore.KeyField"));
        this.props.setLook(wlKeyField);
        FormData fdlKeyField = new FormData();
        fdlKeyField.left = new FormAttachment(0, 0);
        fdlKeyField.top = new FormAttachment(lastControl, this.margin);
        fdlKeyField.right = new FormAttachment(this.middle, -this.margin);
        wlKeyField.setLayoutData(fdlKeyField);
        this.wKeyField = new TextVar(this.transMeta, this.shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        this.props.setLook(this.wKeyField);
        FormData fdKeyField = new FormData();
        fdKeyField.left = new FormAttachment(this.middle, 0);
        fdKeyField.top = new FormAttachment(wlKeyField, 0, SWT.CENTER);
        fdKeyField.right = new FormAttachment(100, 0);
        this.wKeyField.setLayoutData(fdKeyField);
        lastControl = this.wKeyField;

        Label wlJsonField = new Label(shell, SWT.RIGHT);
        wlJsonField.setText(BaseMessages.getString(PACKAGE, "BeamOutputFirestore.JsonField"));
        this.props.setLook(wlJsonField);
        FormData fdlJsonField = new FormData();
        fdlJsonField.left = new FormAttachment(0, 0);
        fdlJsonField.top = new FormAttachment(lastControl, this.margin);
        fdlJsonField.right = new FormAttachment(this.middle, -this.margin);
        wlJsonField.setLayoutData(fdlJsonField);
        this.wJsonField = new TextVar(this.transMeta, this.shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        this.props.setLook(this.wJsonField);
        FormData fdJsonField = new FormData();
        fdJsonField.left = new FormAttachment(this.middle, 0);
        fdJsonField.top = new FormAttachment(wlJsonField, 0, SWT.CENTER);
        fdJsonField.right = new FormAttachment(100, 0);
        this.wJsonField.setLayoutData(fdJsonField);
        lastControl = this.wJsonField;

        this.wOK = new Button(this.shell, SWT.PUSH);
        this.wOK.setText(BaseMessages.getString(PACKAGE, "System.Button.OK"));

        this.wCancel = new Button(shell, SWT.PUSH);
        this.wCancel.setText(BaseMessages.getString(PACKAGE, "System.Button.Cancel"));

        setButtonPositions(new Button[]{this.wOK, this.wCancel}, this.margin, lastControl);

        this.wOK.addListener(SWT.Selection, e -> ok());
        this.wCancel.addListener(SWT.Selection, e -> cancel());

        this.lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };

        this.wStepname.addSelectionListener(this.lsDef);
        this.wProjectId.addSelectionListener(this.lsDef);
        this.wKind.addSelectionListener(this.lsDef);
        this.wKeyField.addSelectionListener(this.lsDef);
        this.wJsonField.addSelectionListener(this.lsDef);

        // Detect X or ALT-F4 or something that kills this window...
        this.shell.addListener(SWT.Close, e -> cancel());

        getData();
        setSize();
        this.output.setChanged(this.changed);

        this.shell.open();
        while (!this.shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
        return this.stepname;
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
                this.log.logDetailed(BeamConst.STRING_BEAM_FIRESTORE_OUTPUT_PLUGIN_ID + "-> Ocorreu um erro inesperado em openUrlHelp(). Exception : ", ex);
                Logger.getLogger(BeamFirestoreOutputDialog.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Populate the widgets.
     */
    public void getData() {
        this.wStepname.setText(this.stepname);
        this.wProjectId.setText(Const.NVL(this.output.getProjectId(), ""));
        this.wKind.setText(Const.NVL(this.output.getKind(), "Entidade"));
        this.wKeyField.setText(Const.NVL(this.output.getKeyField(), "id"));
        this.wJsonField.setText(Const.NVL(this.output.getJsonField(), ""));
        this.wStepname.selectAll();
        this.wStepname.setFocus();
    }

    /**
     * Botão de cancelamento.
     */
    private void cancel() {
        this.stepname = null;
        this.output.setChanged(this.changed);
        dispose();
    }

    /**
     * Botão de aceite.
     */
    private void ok() {
        if (Utils.isEmpty(this.wStepname.getText())) {return;}
        this.getInfo(this.output);
        this.dispose();
    }

    /**
     * Obtendo informações.
     *
     * @param meta
     */
    private void getInfo(BeamFirestoreOutputMeta meta) {
        this.stepname = this.wStepname.getText(); // return value
        meta.setProjectId(this.wProjectId.getText());
        meta.setKind(this.wKind.getText());
        meta.setKeyField(this.wKeyField.getText());
        meta.setJsonField(this.wJsonField.getText());
        this.output.setChanged();
    }

}
