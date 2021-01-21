package com.batch.dpr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javafx.util.Pair;

public class LetterDataExport {

	public static void main(String[] args) {

		System.out.println("Start of processing ReadFile" + "\n");
		String currCnfgLine;
		String currLtrDataLine;
		int count = 0;
		int startPosition = 0;
		List<String> configList = new ArrayList<String>();
		Map<String, String> valueMap;
		List<Pair> valueList = new ArrayList<Pair>();
		try {
			BufferedReader readerConfig = new BufferedReader(new FileReader("C:/Users/adash/Desktop/LetterConfig.txt"));
			BufferedReader readerFile = new BufferedReader(new FileReader("C:/Users/adash/Desktop/DprLetterFile.txt"));
			while ((currCnfgLine = readerConfig.readLine()) != null) {

				if (!currCnfgLine.contains("Variables:Length")) {
					configList.add(currCnfgLine);
				} else if (!currCnfgLine.contains("=================")) {
					configList.add(currCnfgLine);
				} else {
					System.out.println("Lines to exclude is : " + currCnfgLine.toString());
				}

				valueMap = new HashMap<>();
				while ((currLtrDataLine = readerFile.readLine()) != null && !currLtrDataLine.contains("@@HEADER")&& !currLtrDataLine.contains("@@TRAILER")) {
					for (String config : configList) {
						String[] mapArray = config.split("\\|");
						String value = currLtrDataLine.substring(startPosition, startPosition + Integer.valueOf(mapArray[1])).trim();
						Pair<String, String> pair = new Pair<String, String>(mapArray[0], value);
						valueList.add(pair);
						valueMap.put(mapArray[0], value);
						startPosition = startPosition + Integer.valueOf(mapArray[1]);
					}
					startPosition = 0;
				}
			}
			for (Pair pair : valueList) {
				System.out.println(pair.getKey().toString() + " --> " + pair.getValue().toString());
			}
			readerConfig.close();
			readerFile.close();
		} catch (Exception e) {
			System.out.println("Exception in main()");
			e.printStackTrace();
		}
		System.out.println("\n" + "End of processing ReadFile");

	}

}
