package hw3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class FinalAverageDelay {

	// The path where the MapReduce job output is stored
	private static final String PATH = "C:/Users/shawn_000/OneDrive/MapReduce/HW3/Plain/output";
	
	/**
	 * Sequential function to compute final average delay after the MapReduce job done
	 */
	private static void CalculateFinalAverageDelay(String path) {
		int sumFlights = 0;
		double sumDelay = 0;
		
		try {
			File folder = new File(path + "/");
			File[] listOfFiles = folder.listFiles();

			// Load all 'part-r-...' files and calculate sumDelay and sumFlights
			for (File file : listOfFiles) {
				if (file.getName().startsWith("part-r-")) {
					FileReader fr = new FileReader(file);
					BufferedReader br = new BufferedReader(fr);
					String line = br.readLine();
					String[] results = line.split("\t");
					sumDelay += Float.parseFloat(results[0]);
					sumFlights += Integer.parseInt(results[1]);
					br.close();
				}
			}

			// Compute the final average delay
			double avgDelay = sumDelay / sumFlights;

			// Generate a file named "average-delay" under the output path
			File file = new File(path + "/average-delay");
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
			bw.write("" + avgDelay);
			bw.close();
		} catch (IOException e) {
			System.out.println("Invalid path!");
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CalculateFinalAverageDelay(PATH);
	}

}
