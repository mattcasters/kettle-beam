package org.kettle.beam.carte;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.CarteServlet;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.www.BaseHttpServlet;
import org.pentaho.di.www.CartePluginInterface;
import org.pentaho.di.www.SlaveServerTransStatus;
import org.pentaho.di.www.WebResult;

@CarteServlet(
    id="registerBeamMetrics",
    name="Register Beam Metrics",
    description="Captures Apache Beam Metrics regarding a running transformation"
    )
public class RegisterBeamMetrics extends BaseHttpServlet implements CartePluginInterface {

  private static final long serialVersionUID = -3954291362486792603L;
  public static final String CONTEXT_PATH = "/kettle/registerBeamMetrics";
  
  public RegisterBeamMetrics() {
  }
  
  public String toString() {
    return "Register Beam Metrics";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }
  
  public String getContextPath() {
    return CONTEXT_PATH;
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

    if (isJettyMode() && !request.getRequestURI().startsWith(CONTEXT_PATH))
      return;

    if (log.isDebug()) {
      logDebug("Registration of Apache Beam Metrics");
    }

    // The Object ID
    String carteObjectId = request.getParameter("id"); // the carte object id

    // Data type: "input", "output", "read", "written", "errors", "rejected", ...
    //
    String type = request.getParameter("type");

    // The
    //
    String hostname = request.getParameter("hostname"); // the server on which the mapper or reducer runs

    // The
    String port = request.getParameter("port"); // the name port
    String user = request.getParameter("user"); // the user to connect to the remote server with
    String pass = request.getParameter("pass"); // the password to connect to the remote server with
    String trans = request.getParameter("trans"); // the name of the transformation
    String taskId = request.getParameter("taskId"); // the Id of the task on the node
    String jobId = request.getParameter("jobId"); // the Id of the hadoop job
    boolean update = "Y".equalsIgnoreCase(request.getParameter("update")); // update of information?
    
    PrintWriter out = response.getWriter();
    BufferedReader in = request.getReader();
    
    WebResult webResult = new WebResult(WebResult.STRING_OK, "registeration success", "");
    
    try {
      
      // First read the complete SlaveServerTransStatus object XML in memory from the request
      //
      StringBuilder xml = new StringBuilder(request.getContentLength());
      int c;
      while ((c = in.read()) != -1) {
        xml.append((char) c);
      }
      
      SlaveServerTransStatus transStatus = SlaveServerTransStatus.fromXML(xml.toString());
      
      NodeRegistrationQueue registry = NodeRegistrationQueue.getInstance();
      NodeRegistrationEntry entry = new NodeRegistrationEntry(jobId, taskId, hostname, port, EntryType.valueOf(type), user, pass, trans, carteObjectId, transStatus, update);
      registry.addNodeRegistryEntry(entry);
      
      response.setContentType("text/xml");
      response.setStatus(HttpServletResponse.SC_OK);
      response.setCharacterEncoding(Const.XML_ENCODING);
      out.println(XMLHandler.getXMLHeader());
    } catch(Exception e) {
      webResult.setResult(WebResult.STRING_ERROR);
      webResult.setMessage(Const.getStackTracker(e));
    }
    out.println(webResult.getXML());
  }
}