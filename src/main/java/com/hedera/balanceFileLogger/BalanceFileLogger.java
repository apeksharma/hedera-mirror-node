package com.hedera.balanceFileLogger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import com.hedera.databaseUtilities.DatabaseUtilities;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

/**
 * This is a utility file to read back service record file generated by Hedera node
 */
public class BalanceFileLogger {

	private static final Logger log = LogManager.getLogger("recordStream-log");
	private static final Marker MARKER = MarkerManager.getMarker("SERVICE_RECORD");
	static final Marker LOGM_EXCEPTION = MarkerManager.getMarker("EXCEPTION");

    private static Connection connect = null;
	
    enum balance_fields {
        ZERO
        ,ACCOUNT_SHARD
        ,ACCOUNT_REALM
        ,ACCOUNT_NUM
        ,BALANCE_INSERT 
        ,BALANCE_UPDATE        
    }
	static void moveFileToParsedDir(String fileName) {
		File sourceFile = new File(fileName);
		File parsedDir = new File(sourceFile.getParentFile().getParentFile().getPath() + "/parsedRecordFiles/");
		parsedDir.mkdirs();
		File destFile = new File(parsedDir.getPath() + "/" + sourceFile.getName());
		try {
			Files.move(sourceFile.toPath(), destFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
			log.info(MARKER, sourceFile.toPath() + " has been moved to " + destFile.getPath());
		} catch (IOException ex) {
			log.error(MARKER, "Fail to move {} to {} : {}",
					fileName, parsedDir.getName(),
					ex.getStackTrace());
		}
	}
	
	private static File getLatestBalancefile(File balanceFilesPath) throws IOException {
	    
	    File lastFile = null;
        // find all files in path
        // return the greatest file name
	    
	    for (final File nodeFolders : balanceFilesPath.listFiles()) {
	        if (nodeFolders.isDirectory()) {
	            
	            List<String> balancefiles = new ArrayList<String>();

	            for (final File balanceFile : nodeFolders.listFiles()) {
	                balancefiles.add(balanceFile.getName());
	            }
	            if (balancefiles.size() != 0) {
    	            Collections.sort(balancefiles);
    	            File lastFound = new File(nodeFolders.getCanonicalPath() + File.separator + balancefiles.get(balancefiles.size()-1));
    	            System.out.println (nodeFolders.getCanonicalPath());
    	            System.out.println (balancefiles.get(balancefiles.size()-1));
    	            
    	            if (lastFile == null) {
    	                lastFile = lastFound;
    	            } else if (lastFile.getName().compareTo(lastFound.getName()) < 0) {
    	                lastFile = lastFound;
    	            }
	            }
	        }
	    }	    
	    
	    return lastFile;
	    
	}

	public static void main(String[] args) {

	    // balance files are in 
	    File balanceFilesPath = new File("./accountBalances/balance");
	    boolean processLine = false;
	    
        try {
            File balanceFile = getLatestBalancefile(balanceFilesPath);
            if (balanceFile != null) {
                // process the file
                connect = DatabaseUtilities.openDatabase(connect);

                if (connect != null) {
	                PreparedStatement insertBalance;
	                insertBalance = connect.prepareStatement(
	                        "insert into t_account_balances (account_shard, account_realm, account_num, balance) "
	                        + " values (?, ?, ?, ?) "
	                        + " ON CONFLICT (account_shard, account_realm, account_num) "
	                        + " DO UPDATE set balance = ?");
		        
	                BufferedReader br = new BufferedReader(new FileReader(balanceFile));
	
	                String line;
		            while ((line = br.readLine()) != null) {
		                if (processLine) {
		                    try {
		                        String[] balanceLine = line.split(",");
		                        
	                            insertBalance.setLong(balance_fields.ACCOUNT_SHARD.ordinal(), Long.valueOf(balanceLine[0]));
	                            insertBalance.setLong(balance_fields.ACCOUNT_REALM.ordinal(), Long.valueOf(balanceLine[1]));
	                            insertBalance.setLong(balance_fields.ACCOUNT_NUM.ordinal(), Long.valueOf(balanceLine[2]));
	                            insertBalance.setLong(balance_fields.BALANCE_INSERT.ordinal(), Long.valueOf(balanceLine[3]));
	                            insertBalance.setLong(balance_fields.BALANCE_UPDATE.ordinal(), Long.valueOf(balanceLine[3]));
	
	                            insertBalance.execute();
		                        
		                    } catch (SQLException e) {
		                        e.printStackTrace();
		                        log.error(LOGM_EXCEPTION, "Exception {}", e);
		                    }
		                } else if (line.contentEquals("shard,realm,number,balance")) {
		                    // skip all lines until shard,realm,number,balance
	                        processLine = true;
	                    }
		            }
	                insertBalance.close();
	                br.close();
	            }
	        } else {
	            log.info("No balance file to parse found");
	        }
	    } catch (FileNotFoundException e) {
            e.printStackTrace();
            log.error(LOGM_EXCEPTION, "Exception {}", e);
        } catch (IOException e) {
            e.printStackTrace();
            log.error(LOGM_EXCEPTION, "Exception {}", e);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error(LOGM_EXCEPTION, "Exception {}", e);
        }
        log.info(MARKER, "Last Balance processing done");
	}
}