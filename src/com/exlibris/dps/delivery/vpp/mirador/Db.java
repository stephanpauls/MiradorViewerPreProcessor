/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.exlibris.dps.delivery.vpp.mirador;
import com.exlibris.core.infra.common.exceptions.logging.ExLogger;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
/**
 *
 * @author u0077845
 */
public class Db {
    	private Connection conn = null;
    	private Connection conn_shr = null;
    	private Connection conn_per = null;
//        private final String url = "jdbc:oracle:thin:@libis-db-rosettaq.cc.kuleuven.be:1751/ROSETTAQ.kuleuven.be";
        private String url = "jdbc:oracle:thin:@libis-db-rosetta.cc.kuleuven.be:1551/ROSETTAP.kuleuven.be";        
        private static final ExLogger logger = ExLogger.getExLogger(Db.class);
        private String breedte;
        private String hoogte;
        private String entityType;
        private Integer storageid;
        private String internalpath;
        private Properties prop; 

        
	  public Db()
	  {
		  try{
		    Class.forName("oracle.jdbc.driver.OracleDriver");
		  } catch (ClassNotFoundException e) {
			  logger.debug(e.getMessage());
		  }
      try{
        prop.load(new FileInputStream("mirador.properties"));
        } catch (IOException e){
          logger.info("mirador.properties not loaded");
          return;
        }
		  
		  try {
//		    conn = DriverManager.getConnection(url,"V232_REP00","V232_REP00");
                    url = prop.getProperty("database");
		    conn = DriverManager.getConnection(url,prop.getProperty("repuser"),prop.getProperty("reppwd"));
		    conn_shr = DriverManager.getConnection(url,prop.getProperty("shruser"),prop.getProperty("shrpwd"));
		    conn_per = DriverManager.getConnection(url,prop.getProperty("peruser"),prop.getProperty("perpwd"));
		    conn.setAutoCommit(false);
		    conn_per.setAutoCommit(false);
                    conn_shr.setAutoCommit(false);
		  } catch (SQLException e){
			  logger.debug(e.getMessage());
		  }
	  }
	  
	  public void getConnection()
	  {
	  
		  try {
			if (conn == null) { 
//				conn = DriverManager.getConnection(url,"V232_REP00","V232_REP00");
                	    conn = DriverManager.getConnection(url,prop.getProperty("repuser"),prop.getProperty("reppwd"));
				conn.setAutoCommit(false);
			}
			if (conn_shr == null) { 
//				conn = DriverManager.getConnection(url,"V232_REP00","V232_REP00");
                	    conn_shr = DriverManager.getConnection(url,prop.getProperty("shruser"),prop.getProperty("shrpwd"));
				conn_shr.setAutoCommit(false);
			}
			if (conn_per == null) { 
//				conn = DriverManager.getConnection(url,"V232_REP00","V232_REP00");
                	    conn_per = DriverManager.getConnection(url,prop.getProperty("peruser"),prop.getProperty("perpwd"));
				conn_per.setAutoCommit(false);
			}
		  } catch (SQLException e){
			  logger.debug(e.getMessage());
		  }
	  }

          
          
    private void getEntityType(String pid) {
	  
        Statement stmt = null;
        ResultSet rset = null;
                  
        try{
            getConnection();
            stmt = conn.createStatement();
            String query =  "select c.entitytype from hdecontrol c where c.pid = '"+pid+"'";
            rset = stmt.executeQuery(query);
	    while (rset.next()) {
                this.entityType = rset.getString(1);
	    }
	    rset.close();
	    stmt.close();
	  } catch (SQLException e){
        	logger.debug(e.getMessage());
		closeConn();
	  }
	}
          
    
    public HashMap<Integer, String> getStorageIds()  {
        
       Statement stmt = null;
        ResultSet rset = null;
        HashMap<Integer, String> storageMap = new HashMap<Integer, String>();	
                  
        try{
            getConnection();
            stmt = conn_shr.createStatement();
            String query =  "select storage_id,value from storage_parameter where key = 'DIR_ROOT'";
	    rset = stmt.executeQuery(query);
	    while (rset.next()) {
	    	storageMap.put(rset.getInt(1),rset.getString(2));
	    }
	    rset.close();
	    stmt.close();
	  } catch (SQLException e){
        	logger.debug(e.getMessage());
		closeConn();
	  }
	  return storageMap;
	}  
    
    
    
    public ArrayList getOtherPIDsOfEntity(String pid) {
	  
        Statement stmt = null;
        ResultSet rset = null;
        ArrayList<String> pids = new ArrayList();	
                  
        try{
            getConnection();
            stmt = conn.createStatement();
            String query =  "select cc.pid,cc.entitytype from hdecontrol c inner join hdecontrol cc  on cc.entitytype = c.entitytype where c.pid = '"+pid+"'"
                    + " and cc.pid != '"+pid+"' and cc.lifecycle = 'IN_PERMANENT_REPOSITORY'";
	    rset = stmt.executeQuery(query);
	    while (rset.next()) {
	    	pids.add(rset.getString(1));
	    }
	    rset.close();
	    stmt.close();
	  } catch (SQLException e){
        	logger.debug(e.getMessage());
		closeConn();
	  }
          getEntityType(pid);
	  return pids;
	}


    
     public String getDerivativeHighPid(String parentId) {
	  
        Statement stmt = null;
        ResultSet rset = null;
        String pid = null;
                  
        try{
            getConnection();
            stmt = conn.createStatement();
            String query =  "select c.pid from hdecontrol c inner join hdepidmid pm on pm.pid = c.pid inner join hdemetadata m on m.mid = pm.mid "
                    + "where c.parentid = '"+parentId+"' and m.value like '%DERIVATIVE_COPY%HIGH%'";            
            
	    rset = stmt.executeQuery(query);
            
	    while (rset.next()) {
	    	pid = rset.getString(1);
                break;
	    }
	    rset.close();
	    stmt.close();
	  } catch (SQLException e){
        	logger.debug(e.getMessage());
		closeConn();
	  }
	  return pid;
	}

     public String getPreservation(String parentId) {
	  
        Statement stmt = null;
        ResultSet rset = null;
        String pid = null;
                  
        try{
            getConnection();
            stmt = conn.createStatement();
            String query =  "select c.pid from hdecontrol c inner join hdepidmid pm on pm.pid = c.pid inner join hdemetadata m on m.mid = pm.mid "
                    + "where c.parentid = '"+parentId+"' and m.value like '%PRESERVATION%'";            
            
	    rset = stmt.executeQuery(query);
            
	    while (rset.next()) {
	    	pid = rset.getString(1);
                break;
	    }
	    rset.close();
	    stmt.close();
	  } catch (SQLException e){
        	logger.debug(e.getMessage());
		closeConn();
	  }
	  return pid;
	}     
     
     public String getFileLabel(String filePid) {
	  
        Statement stmt = null;
        ResultSet rset = null;
        Statement stmt2 = null;
        ResultSet rset2 = null;
        String label = null;
        String query = null;
        String query2 = null;
        Integer tmpStorageId = null;
        String tmpInternalPath = null;

        this.internalpath = null;
        this.storageid = -1;
        try{
            getConnection();
            stmt = conn.createStatement();
            query =  "select c.label,r.storageid,r.internalpath from hdecontrol c left outer join hdestreamref r on r.pid = c.pid where c.pid = '"+filePid+"'";            

	    rset = stmt.executeQuery(query);
	    while (rset.next()) {
	    	label = rset.getString(1);
                tmpStorageId = rset.getInt(2);
                tmpInternalPath = rset.getString(3);
                break;
	    }
	    rset.close();
	    stmt.close();
            
            try {
                stmt2 = conn_per.createStatement();
                query2 =  "select r.storage_id,r.index_location from permanent_index r where r.stored_entity_id ='"+filePid+"'";            

                rset2 = stmt2.executeQuery(query2);
                while (rset2.next()) {
                   this.storageid = rset2.getInt(1);
                   this.internalpath = rset2.getString(2);
                break;
                }
                if (Integer.valueOf(storageid) < 0) {
                  this.storageid = tmpStorageId;
                  this.internalpath = tmpInternalPath;
                }
            } catch (SQLException e2) {
        	logger.debug(e2.getMessage());
                this.storageid = tmpStorageId;
                this.internalpath = tmpInternalPath;
            }
            rset2.close();
            stmt2.close();

                
	  } catch (SQLException e){
        	logger.debug(e.getMessage());
		closeConn();
	  }
	  return label;
	}

    public String getFileExtension(String filePid) {
	  
        Statement stmt = null;
        ResultSet rset = null;
        String extension = null;
                  
        try{
            getConnection();
            stmt = conn.createStatement();
            String query =  "select c.fileextension from hdestreamref c where c.pid = '"+filePid+"'";            
            
	    rset = stmt.executeQuery(query);
	    while (rset.next()) {
	    	extension = rset.getString(1);
                break;
	    }
	    rset.close();
	    stmt.close();
	  } catch (SQLException e){
        	logger.debug(e.getMessage());
		closeConn();
	  }
	  return extension;
	}     
    
        public void getFileSize(String filePid) {
	  
        Statement stmt = null;
        ResultSet rset = null;
                  
        try{
            getConnection();
            stmt = conn.createStatement();
            String query = "select case when (instr(m.value,'nisoImage.imageLength') = 0 ) then 1000 else to_number(substr(substr(m.value,(instr(m.value,'nisoImage.imageLength')+length('nisoImage.imageLength</key><key id=\"significantPropertiesValue\">'))),0,instr(substr(m.value,(instr(m.value,'nisoImage.imageLength')+length('nisoImage.imageLength</key><key id=\"significantPropertiesValue\">'))),'<')-1)) end as hoogte,"
                    + "case when (instr(m.value,'nisoImage.imageWidth') = 0 ) then 1000 else to_number(substr(substr(m.value,(instr(m.value,'nisoImage.imageWidth')+length('nisoImage.imageWidth</key><key id=\"significantPropertiesValue\">'))),0,instr(substr(m.value,(instr(m.value,'nisoImage.imageWidth')+length('nisoImage.imageWidth</key><key id=\"significantPropertiesValue\">'))),'<')-1)) end as breedte "
                    + "from hdemetadata m inner join hdepidmid pm on pm.mid = m.mid inner join hdecontrol c on c.pid = pm.pid where c.pid= '"+filePid+"'";            
            
	    rset = stmt.executeQuery(query);
	    while (rset.next()) {
	    	hoogte = rset.getString(1);
	    	breedte = rset.getString(2);
                break;
	    }
	    rset.close();
	    stmt.close();
	  } catch (SQLException e){
        	logger.debug(e.getMessage());
		closeConn();
	  }
	}     

    public Integer getModificationDate(String iePid) {
	  
        Statement stmt = null;
        ResultSet rset = null;
        Integer extension = null;
                  
        try{
            getConnection();
            stmt = conn.createStatement();
            String query =  "select to_number(to_char(modificationdate,'yyyyMMdd')) from  hdecontrol where pid =  '"+iePid+"'";            
            
	    rset = stmt.executeQuery(query);
	    while (rset.next()) {
	    	extension = rset.getInt(1);
                break;
	    }
	    rset.close();
	    stmt.close();
	  } catch (SQLException e){
        	logger.debug(e.getMessage());
		closeConn();
	  }
	  return extension;
	}          
  

public void closeConn() {
	try{
		conn.close();
                conn_shr.close();
                conn_per.close();
	} catch (SQLException e) {
		logger.debug(e.getMessage());
	}
}

public Connection getConn() {
	return conn;
}
public Connection getConnShr() {
	return conn_shr;
}

public void setConn(Connection conn) {
	this.conn = conn;
}
public void setConnShr(Connection conn) {
	this.conn_shr = conn;
}

    public String getBreedte() {
        return breedte;
    }

    public String getHoogte() {
        return hoogte;
    }

    public String getEntityType() {
        return entityType;
    }

    public int getStorageid() {
        return storageid;
    }

    public void setStorageid(int storageid) {
        this.storageid = storageid;
    }

    public String getInternalpath() {
        return internalpath;
    }

    public void setInternalpath(String internalpath) {
        this.internalpath = internalpath;
    }


}
