package com.batch.dpr;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javafx.util.Pair;

public class LetterDataExportWithDB {

	private static Connection conn = null;
	private static String dbUrl = null;
	private static String dbUserId = null;
	private static String dbPass = null;
	private static String inputFile = null;
	private static String stBatchDate = (new SimpleDateFormat("yyyyMMdd")).format(new Date());
	private static String dprLtrConfig = null;
	
	public static void main(String[] args) {

		initialize();
		loadDBLoginCredentials();
		System.out.println("Start of processing main()" + "\n");
		String columnName = "";
		String columnData = "";
		String currLine;
		int startPosition = 0;
		List<String> configList = new ArrayList<String>();
		Map<String, String> valueMap;
		List<Pair> valueList = new ArrayList<Pair>();
		try {
			BufferedReader readerConfig = new BufferedReader(new FileReader("C:/Users/adash/Desktop/LetterConfig.txt"));
			BufferedReader readerFile = new BufferedReader(new FileReader("C:/Users/adash/Desktop/DprLetterFile.txt"));
			while ((currLine = readerConfig.readLine()) != null) {
				configList.add(currLine);
			}
			valueMap = new HashMap<>();
			while ((currLine = dprLtrConfig.readLine()) != null) {
				for (String config : configList) {
					String[] mapArray = config.split("\\|");
					String value = currLine.substring(startPosition, startPosition + Integer.valueOf(mapArray[1]))
							.trim();
					Pair<String, String> pair = new Pair<String, String>(mapArray[0], value);
					valueList.add(pair);
					valueMap.put(mapArray[0], value);
					startPosition = startPosition + Integer.valueOf(mapArray[1]);
				}
				startPosition = 0;
			}
			for (Pair pair : valueList) {
				columnName = pair.getKey().toString();
				columnData = pair.getKey().toString();
				System.out.println(columnName + " --> " + columnData);
				dbDataUloader(columnName, columnData);
			}
			readerConfig.close();
			readerFile.close();
		} catch (Exception e) {
			System.out.println("Exception in main()");
			e.printStackTrace();
		}
		System.out.println("\n" + "End of processing main()");

	}

	public static void initialize() {
		FileInputStream myStream = null;
		Properties prop = null;
		try {

			// String DPLDialerLtrPrcsConfig = System.getProperty("LoadDeltaFileProcessConfig"); // Path of Config file.
			dprLtrConfig = "C:/Users/adash/Desktop/LetterConfig.txt"; //local testing
			myStream = new FileInputStream(DPLDialerLtrPrcsConfig);
			prop = new Properties();
			prop.load(myStream);
			inputFile = prop.getProperty("INPUTFILE").concat(".").concat(stBatchDate).concat(".dat");

		} catch (Exception e) {
			//logger.error("Exception occured while fetching the config file properties from LoadDeltaFileProcess.props file."+ e.getMessage());
			System.exit(1);
		}
	}

	private static void loadDBLoginCredentials(String dbConfigFilePath) {
		FileInputStream myStream = null;
		Properties prop = null;
		try {
			//logger.info("Fetching and reading the DB Config properties file....");
			myStream = new FileInputStream(dbConfigFilePath);
			prop = new Properties();
			prop.load(myStream);
			dbUrl = prop.getProperty("DBURL");
			dbUserId = prop.getProperty("DBUserID");
			dbPass = prop.getProperty("DBPassword");
		} catch (Exception e) {
			//logger.error("ProcessRevokePhnData::Exception occured while fetching the DB Config properties file."+ e.getMessage());
			System.exit(1);
		}
	}

	public static boolean dbDataUloader(String columnName, String columnData) {

		PreparedStatement pstmt1 = null;
		PreparedStatement pstmt2 = null;
		ResultSet rs = null;
		String revokedInd = "";
		String fileRevokeInd = "";
		int uploadType = 0;
		try {
			pstmt1 = conn.prepareStatement(querySelctRvkPhnNbr);
			pstmt1.setString(1, filerec.getPhoneNumber());
			rs = pstmt1.executeQuery();

			if (rs.next()) { 
				// Updating Record logger.info("Record Exists for : " + filerec.getPhoneNumber());
				revokedInd = rs.getString(1);
				fileRevokeInd = filerec.getCellConsentInd();
				//logger.info("Revocation Indicator in the File : " + fileRevokeInd + "& Revocation Indicator in the table :" + revokedInd);
				if ((fileRevokeInd.equalsIgnoreCase(Constants.IND_Y) && revokedInd.equalsIgnoreCase(Constants.IND_Y)) 
						|| (fileRevokeInd.equalsIgnoreCase(Constants.IND_Y) && revokedInd.equalsIgnoreCase(Constants.IND_N)) 
							|| (fileRevokeInd.equalsIgnoreCase(Constants.IND_Y) && revokedInd.equalsIgnoreCase(Constants.IND_O))) {
					uploadType = Constants.RECORD_IGNORED;
				}
				if (!revokedInd.equalsIgnoreCase("") && (revokedInd.equalsIgnoreCase(Constants.IND_O)
						|| revokedInd.equalsIgnoreCase(Constants.IND_Y))) {
					if (filerec.getCellConsentInd().equalsIgnoreCase(Constants.IND_N)) {
						pstmt2 = conn.prepareStatement(queryUpdateRvkPhnNbr);
						pstmt2.setString(1, filerec.getCellConsentInd());
						pstmt2.setString(2, filerec.getPhoneNumber());
						int recUpd = pstmt2.executeUpdate();
						//logger.info("Record updated for Phone No : " + filerec.getPhoneNumber());
						uploadType = Constants.RECORD_UPDATED;
					}
				}
			} else // New Record
			{
				//logger.info("Record NOT Exists for : " + filerec.getPhoneNumber());
				pstmt2 = conn.prepareStatement(queryInsertRvkPhnNbr);

				pstmt2.setString(1, filerec.getPhoneNumber());
				pstmt2.setString(2, filerec.getCellConsentInd());

				pstmt2.executeUpdate();
				uploadType = Constants.RECORD_INSERTED;
			}

		} catch (SQLException e) {
			//logger.error("LoadDeltaFileProcessHelper.uploadData():Exception::" + e.getMessage());
			throw e;
		} finally {
			try {
				DbUtil.closeResultSet(rs);
				if (pstmt1 != null)
					pstmt1.close();
				if (pstmt2 != null)
					pstmt2.close();
			} catch (Exception e) {
				//logger.error("LoadDeltaFileProcessHelper.uploadData():Exception::" + e.getMessage());
			}
		}
		return true;
	}

}
