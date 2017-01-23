import java.util.Random;

public class PiCalculator {
	/*
	 * The Worker thread. Take a random seed and number of darts as input 
	 * parameters. Calculate an approximate value of Pi and return it to
	 * Master thread.
	 */
	private double workerThread(long randomSeed, int numOfDarts) {
		int countInCircle = 0;

		Random random = new Random(randomSeed);

		for (int i = 0; i < numOfDarts; i++) {
			double x = random.nextDouble();
			double y = random.nextDouble();

			double val = x * x + y * y;
			if (val < 1.0) {
				countInCircle++;
			}
		}

		double pi = (double)countInCircle / (double)numOfDarts * 4;
		return pi;
	}

	/*
	 * The Master thread. Take a random seed, number of darts and number of workers
	 * as input parameters. Initiate a number of worker threads to work and collect
	 * the results from them. Aggregate the results and calculate the error rate and
	 * accuracy.
	 */
	private double masterThread(long randomSeed, int numOfDarts, int numOfWorkers) {
		double sumPi = 0.0;
		double sumDiff = 0.0;

		Random random = new Random(randomSeed);

		for (int i = 0; i < numOfWorkers; i++) {
			// Generate a random seed for each worker. Otherwise they'll get
			// the same results (Pi value) with the same seed.
			int workerRandomSeed = random.nextInt();

			// Initiate the worker threads. Gather the value of Pi.
			double CalculatedPi = workerThread(workerRandomSeed, numOfDarts);
			System.out.println("The calculated Pi value is: " + CalculatedPi);			
			sumPi += CalculatedPi;

			// Calculate the error rate for each worker thread.
			double diffInPercent = (Math.PI - CalculatedPi) / Math.PI * 100;
			System.out.println("The calculated diff in percentage is: " + diffInPercent);	
			sumDiff += diffInPercent;
		}

		double meanPi = sumPi / numOfWorkers;
		System.out.println("\nThe calculated mean Pi value is: " + meanPi);

		// Two ways to calculate the mean difference. Another way can be:
		// double meanDiffInPercent = Math.abs(sumDiff / numOfWorkers);
		// They get the same results.
		double meanDiffInPercent = Math.abs((Math.PI - meanPi) / Math.PI * 100);
		System.out.println("\nMean difference in percentage: " + meanDiffInPercent +"\n");

		return meanDiffInPercent;
	}

	public static void main(String[] args) {		
		long randomSeed = 0;
		int numOfDarts = 0;
		int numOfWorkers = 0;

		if(args.length == 0) {
			System.out.println("Usage: ./CalculatePi <randomSeed> <numOfDarts> <numOfWorkers>");
			System.exit(0);
		} else {
			randomSeed = Long.parseLong(args[0]);
			numOfDarts = Integer.parseInt(args[1]);
			numOfWorkers = Integer.parseInt(args[2]);
			System.out.println("Cmd: ./CalculatePi " + randomSeed + " " + numOfDarts + " " + numOfWorkers + "\n");
		}

		PiCalculator piCal = new PiCalculator();
		// Initiate the master thread.
		double meanDiffInPercent = piCal.masterThread(randomSeed, numOfDarts, numOfWorkers);

		if (meanDiffInPercent < 1.0) {
			System.out.println("Achieve 1% accuracy!");
		} else {
			System.out.println("Fail to achieve 1% accuracy!");
		}
	}
}
