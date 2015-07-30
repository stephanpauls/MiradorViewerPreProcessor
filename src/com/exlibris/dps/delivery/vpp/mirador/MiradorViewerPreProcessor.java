package com.exlibris.dps.delivery.vpp.mirador;

import java.io.FileWriter;
import java.io.IOException;

import com.exlibris.core.infra.common.cache.SessionUtils;
import com.exlibris.core.infra.common.exceptions.logging.ExLogger;
import com.exlibris.core.infra.svc.api.CodeTablesResourceBundle;
import com.exlibris.core.sdk.formatting.DublinCore;
import com.exlibris.digitool.common.dnx.DNXConstants;

import com.exlibris.digitool.common.dnx.DnxDocument;
import com.exlibris.digitool.common.dnx.DnxRecordKey;
import com.exlibris.digitool.common.dnx.DnxSection;
import com.exlibris.digitool.common.dnx.DnxSectionRecord;
import com.exlibris.dps.sdk.access.AccessException;
import com.exlibris.dps.sdk.delivery.AbstractViewerPreProcessor;
import com.exlibris.dps.sdk.delivery.SmartFilePath;
import com.exlibris.dps.sdk.deposit.IEParser;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import javax.servlet.http.HttpServletRequest;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class MiradorViewerPreProcessor extends AbstractViewerPreProcessor
{
  private ArrayList<String> filesPid = new ArrayList();
  private ArrayList<String> origFilesPid = new ArrayList();
  private ArrayList<String> pids = new ArrayList();
  private String pid = null;
  private String repPid = null;
  private String origPid = null;
  private String origRepPid = null;
  private repType type = null;
  private String ext = "";
  private String licentie = "";
  private String entiteit = "";  
  private String progressBarMessage = null;
  private Db db = null;
  private static final ExLogger logger = ExLogger.getExLogger(MiradorViewerPreProcessor.class);
  private  final String m2 = "http://services.libis.be/m2";
  private  final String iipserv = "http://services.libis.be//iipsrv";
  //private  final String miradorserver = "http://services.libis.be";  
  private Writer output;

  

  public void init(DnxDocument dnx, Map<String, String> viewContext, HttpServletRequest request, String dvs, String ieParentId, String repParentId)
    throws AccessException
  {
    //create oracle connection 
    db = new Db();
      
    logger.debug("in MiradorViewerPP execute");
    super.init(dnx, viewContext, request, dvs, ieParentId, repParentId);
    this.pid = this.origPid = getPid();
    this.pids = db.getOtherPIDsOfEntity(this.pid);
    this.pids.add(0,this.pid);
    
    try
    {
      IEParser ieParser = getAccess().getIE(this.pid,null,null);
      this.repPid = this.origRepPid = db.getDerivativeHighPid(this.pid);
      
      gov.loc.mets.StructMapType[] structMapTypeArray = ieParser.getStructMapsByFileGrpId(this.repPid);
        for (gov.loc.mets.StructMapType structMapType : structMapTypeArray) {
          this.origFilesPid = ieParser.getFilesArray(structMapType);
          if (structMapType.getTYPE().equals(com.exlibris.core.sdk.consts.Enum.StructMapType.LOGICAL.name())) {
            break;
          }
        }      
      
          DnxDocument dnxie = ieParser.getIeDnx();
          DnxSection dnxieS = dnxie.getSectionById(DNXConstants.ACCESSRIGHTSPOLICY.sectionId);
          List<DnxSectionRecord> dnxieSR = dnxieS.getRecordList();
          for (DnxSectionRecord record : dnxieSR) {
              DnxRecordKey key = record.getKeyById(DNXConstants.ACCESSRIGHTSPOLICY.POLICYID.sectionKeyId);
              if (this.licentie.length() != 0) this.licentie += " - ";
              this.licentie += key.getValue();
          }
          DnxSection dnxieS2 = dnxie.getSectionById(DNXConstants.GENERALIECHARACTERISTICS.sectionId);
          List<DnxSectionRecord> dnxieSR2 = dnxieS2.getRecordList();
          for (DnxSectionRecord record : dnxieSR2) {
              DnxRecordKey key = record.getKeyById(DNXConstants.GENERALIECHARACTERISTICS.IEENTITYTYPE.sectionKeyId);
              if (this.entiteit.length() != 0) this.entiteit += " - ";
              this.entiteit += key.getValue();
          }        
      this.ext = db.getFileExtension(this.origFilesPid.get(0));
      if (this.ext.toUpperCase().equals(repType.JP2.name())) {
        this.type = repType.JP2;
      } else if (this.ext.toUpperCase().equals(repType.TIFF.name())) {
        this.type = repType.TIFF;
      } else if (this.ext.toUpperCase().equals(repType.TIF.name())) {
        this.type = repType.TIFF;
      }
      else
      {
        logger.error("Error In Book Reader VPP - The viewer doesn't support the following ext:" + this.ext.toUpperCase(), new String[] { this.origPid });
        throw new Exception();
      }
    }
    catch (Exception e) {
      logger.error("Error In Book Reader VPP - cannot retreive the files to view", e, new String[] { this.origPid });
      throw new AccessException();
    }
    
}

  public void execute() throws Exception
  {
    Map paramMap = getAccess().getViewerDataByDVS(getDvs()).getParameters();

    addHeaderIndexFile();
    Iterator iterator = this.pids.iterator();
    
    while (iterator.hasNext()) {
        this.pid = (String) iterator.next();     
        if (!updateNeeded(this.pid)) {
            if (this.pid.equals(this.origPid)) {
                IEParser ieParser = getAccess().getIE(this.origPid,null,null);
                DublinCore dc = ieParser.getIeDublinCore();
                paramMap.put("ie_title", dc.getTitle());
            }
            addManifestToIndexFile(this.pid);
            continue;
        }
                                                                    
     try
        {

          IEParser ieParser = getAccess().getIE(this.pid,null,null);
          this.repPid = db.getDerivativeHighPid(this.pid);
          gov.loc.mets.StructMapType[] structMapTypeArray = ieParser.getStructMapsByFileGrpId( this.repPid);
          for (gov.loc.mets.StructMapType structMapType : structMapTypeArray) {
            this.filesPid = ieParser.getFilesArray(structMapType);
            if (structMapType.getTYPE().equals(com.exlibris.core.sdk.consts.Enum.StructMapType.LOGICAL.name())) {
              break;
            }
          }
          DublinCore dc = ieParser.getIeDublinCore();


            this.ext = db.getFileExtension(this.filesPid.get(0));
            if ((this.ext != null) && ((this.ext.toUpperCase().equals(repType.TIFF.name())) || (this.ext.toUpperCase().equals(repType.JP2.name())))) {
              prepareImageFiles(dc.getTitle());
              addManifestToIndexFile(this.pid);
            } else {
              logger.warn("Book reader viewer pre processor doesn't support type: " + this.type + ", PID: " + this.pid, new String[0]);
            }

        }
        catch (Exception e) {
          logger.error("Error In Book Reader VPP - cannot retreive the files to view", e, new String[] { this.pid });
          throw new AccessException();
        }
    }
    addFooterIndexFile();
    
    String filePath = getAccess().exportFileStream((String)this.origFilesPid.get((this.origFilesPid.size())-1), MiradorViewerPreProcessor.class.getSimpleName(), this.ieParentId, this.repDirName, this.origRepPid + File.separator + (this.origFilesPid.size()-1));
    String directoryPath = filePath.substring(0, filePath.lastIndexOf(String.valueOf(this.origFilesPid.size() - 1)));
    paramMap.put("ie_dvs", getDvs());
    paramMap.put("entiteit",db.getEntityType());    
    paramMap.put("ie_pid", this.origPid);    
    getAccess().setParametersByDVS(getDvs(), paramMap);
    getAccess().updateProgressBar(getDvs(), "", 100);
    
    getAccess().setFilePathByDVS(getDvs(), new SmartFilePath(directoryPath), this.origRepPid);    
  }
  
  private void prepareImageFiles(String title) throws Exception
  {
      

    String filePath = "";

    JSONObject manifest = new JSONObject();

    manifest.put( "@context", "http://iiif.io/api/presentation/2/context.json");
    manifest.put( "@id", "http://services.libis.be/mirador/manifest/manifest" + this.pid +"manifest.json");
//    manifest.put( "@id", "http://services.libis.be/m2/manifest/" + this.pid +"manifest.json");
    manifest.put( "@type", "sc:Manifest");
    manifest.put( "label", title);
    manifest.put( "attribution", this.licentie);
    manifest.put( "license", this.entiteit);
    
    JSONObject sequence = new JSONObject();
    
    sequence.put("@id",m2+ "data/Pauls/rosetta/sequences/normal");
    sequence.put("@type", "sc:Sequence");
    sequence.put("label", "Current page order");
    sequence.put("viewingDirection", "left-to-right");
    sequence.put("viewingHint", "paged");    
    JSONArray sequences = new JSONArray();
    JSONArray canvases = new JSONArray();
        
    for (int index = 0; index < this.filesPid.size(); index++) {
        
      filePath = getAccess().exportFileStream((String)this.filesPid.get(index), MiradorViewerPreProcessor.class.getSimpleName(), this.ieParentId, this.repDirName, this.repPid + File.separator + index);
      String label = db.getFileLabel(this.filesPid.get(index));
      String fullFileName = filePath.substring(filePath.lastIndexOf("MiradorViewerPreProcessor")+("MiradorViewerPreProcessor").length()+1);
      canvases.put(createManifestFile(label,fullFileName,this.filesPid.get(index),index));
      updateProgressBar(index);
      
    }
    sequence.put("canvases",canvases);
    sequences.put(sequence);
    manifest.put("sequences", sequences);
    
    writeManifest(manifest,this.pid);
     
  }  
  
  
  private void updateProgressBar(int index) throws Exception {
    if (index % 10 == 0) {
      if (this.progressBarMessage == null) {
        Locale locale = new Locale(SessionUtils.getSessionLanguage());
        ResourceBundle resourceBundle = CodeTablesResourceBundle.getDefaultBundle(locale);
        this.progressBarMessage = resourceBundle.getString("delivery.progressBar.bookReaderMessage");
        
      }
      getAccess().updateProgressBar(getDvs(), this.progressBarMessage, Integer.valueOf(index * 100 / this.filesPid.size()).intValue());
    }
  }
  private JSONObject createManifestFile(String label,String fullFilename,String filePid,int index) {

      
      JSONObject service = new JSONObject();
      try {
        service.put("@context","http://iiif.io/api/image/2/context.json");
        service.put("@id", iipserv+"/iipsrv.fcgi?iiif="+fullFilename);
        service.put("profile", "http://iiif.io/api/image/2/level1.json");

        JSONObject resource = new JSONObject();

        resource.put("@id", iipserv+"/iipsrv.fcgi?iiif="+fullFilename);
        resource.put("@type","dctypes:Image");

        resource.put("label", label);
        resource.put("service", service);
        
        JSONObject image = new JSONObject();

        image.put("@id", m2+"/data/Pauls/rosetta/resources/anno-1");
        image.put("@type","oa:Annotation");
        image.put("motivation", "sc:painting");
        image.put("resource", resource);
        image.put("on",  m2+"/data/Pauls/rosetta/canvas/canvas-"+index);

        JSONArray images = new JSONArray();
        images.put(image);
        
        JSONObject canvas = new JSONObject();

        canvas.put("@id", m2+"/data/Pauls/rosetta/canvas/canvas-"+index);
        canvas.put("@type","sc:Canvas");
        canvas.put("label", label);
        db.getFileSize(filePid);
        canvas.put("height", db.getHoogte());
        canvas.put("width", db.getBreedte()); 
        canvas.put("images", images);
        
        return canvas;
            
        
      } catch (JSONException e){
        logger.info(e.getMessage());
        return null;
      }
      
      
  }
  
  public boolean updateNeeded(String pid) {
      
      Integer rosettaDatum = db.getModificationDate(pid);
      Integer manifestDatum = 0;
      boolean updNeeded = false;
      File file=new File("//operational_shared//mirador//"+ pid +"manifest.json");  
      if (file.exists()) {
          SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
          sdf.format(file.lastModified());
          manifestDatum = Integer.valueOf(sdf.format(file.lastModified()));
      }
      
      updNeeded = rosettaDatum > manifestDatum ? true : false;
      return updNeeded;
  }
  
  
  public void writeManifest(JSONObject manifest,String pid) {
        try {  

          // Writing to a file  
          File file=new File("//operational_shared//mirador//"+ pid +"manifest.json");  
          file.createNewFile();  
          FileWriter fileWriter = new FileWriter(file);  

          fileWriter.write(manifest.toString());  
          fileWriter.flush();  
          fileWriter.close();  

        } catch (IOException e) {  
          e.printStackTrace();  
        }  
  }
  
  
private String readFile( String file ) throws IOException {
    BufferedReader reader = new BufferedReader( new FileReader (file));
    String         line = null;
    StringBuilder  stringBuilder = new StringBuilder();
    String         ls = System.getProperty("line.separator");

    while( ( line = reader.readLine() ) != null ) {
        stringBuilder.append( line );
        stringBuilder.append( ls );
    }

    return stringBuilder.toString();
}  
  
    public void addHeaderIndexFile() {
      
   
    try {  
       
        String data = "<script>$(function() {Mirador({\"id\": \"viewer\",\"currentWorkspaceType\": \"singleObject\",\"data\":[";

          File file=new File("//operational_shared//entities//"+db.getEntityType()+"manifest.html");  
          if (!file.exists()) {file.createNewFile(); }
            output = new BufferedWriter(new FileWriter("//operational_shared//entities//"+db.getEntityType()+"manifest.html"));  
            output.append(data);
    } catch (IOException e) {  
      e.printStackTrace();  
    }  
  }
  
  
  public void addManifestToIndexFile(String iePid) {
    
    try {  
        if (!iePid.equals(this.origPid)) { output.append(","); }
        output.append("{ \"manifestUri\": \""+m2+"/manifest/"+iePid+"manifest.json\", \"location\": \"LIBIS\"}");

    } catch (IOException e) {  
      e.printStackTrace();  
    }  
  }

  public void addFooterIndexFile() {
     
    String indexFileWindowObjects = "],\"windowObjects\": [{\"loadedManifest\": \""+m2+"/manifest/"+ this.origPid +"manifest.json\",\"viewType\" : \"BookView\"}]";     
    String indexFileFooter = "}); Mirador.viewer.activeWorkspace.slots[0].addItem(); }); </script>";
    
    try {  
        output.append(indexFileWindowObjects+indexFileFooter);
        output.close();
    } catch (IOException e) {  
      e.printStackTrace();  
    }  
  }
  
  public static enum repType
  {
      JPEG, JPG, PDF, JP2, TIFF, TIF, PNG;
  }
}

