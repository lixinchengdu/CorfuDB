package org.corfudb.samples;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

class MapManipulator extends BaseCorfuAppUtils{

	public MapManipulator(Integer runtimeId, CountDownLatch latch) {
		this.runtimeId = runtimeId;
		this.latch = latch;
	}

	private CorfuRuntime getRuntimeAndConnect(String configurationString) {
		CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
		return corfuRuntime;
	}

	@Override
	void start(String[] args) {

		// Parse the options given, using docopt.
		Map<String, Object> opts =
				new Docopt(MultiRuntime.USAGE)
						.withVersion(GitRepositoryState.getRepositoryState().describe)
						.parse(args);

		String transactionSizeString = (String) opts.get("-s");
		if (transactionSizeString != null) {
			transactionSize = Integer.parseInt(transactionSizeString);
		}

		String transactionNumString = (String) opts.get("-t");
		if (transactionNumString != null) {
			txn = Integer.parseInt(transactionNumString);
		}

		String corfuConfigurationString = (String) opts.get("-c");
		setCorfuRuntime( getRuntimeAndConnect(corfuConfigurationString) );


		String streamString = (String) opts.get("-i");
		if (streamString != null) {
			stream = streamString;
		}

		action();
	}

	@Override
	@SuppressWarnings("checkstyle:printLine") // Sample code
	void action() {
		String key = "A" + runtimeId;

		Map<String, String> map = instantiateCorfuObject(SMRMap.class, stream);

		final int repeatNum = 10;
		final String str = new String(new char[repeatNum]).replace("\0", "a");


		TXBegin();
		if (!map.containsKey(key)) {
			map.put(key, str);
		}

		for (int j = 0; j < transactionSize; ++j) {
			map.put(key, str);
		}
		TXEnd();


		latch.countDown();
		System.out.println("Runtime" + runtimeId + " is ready!\n");
		try {
			latch.await();
		} catch (Exception e) {
			System.out.println(e);
		}

		long start = System.currentTimeMillis();
		//System.out.println("start: " + start + "\n");

		for (int i = 0 ; i < txn; ++i) {
			TXBegin();
			for (int j = 0; j < transactionSize; ++j) {
				//String s = map.get(key);
				map.put(key, str);
			}
			TXEnd();
		}

		long end = System.currentTimeMillis();


		System.out.println("RunTimeId" + runtimeId + " Throughput: " + txn*1000.0/(end-start) + " tx/sec\n");

	}

	private Integer transactionSize = 10;
	private Integer runtimeId = 0;
	private Integer txn = 200;

	private String stream = "A";

	CountDownLatch latch;
}

public class MultiRuntime {

	public static final String USAGE = "Usage: MultiRuntime [-c <conf>] [-n <runtime number>] [-s <transaction size>] "
			+ " [-t <transaction num>] [-i <stream id>]\n"
			+ "Options:\n"
			+ " -c <conf>     Set the configuration host and port  [default: localhost:9999]\n"
			+ " -n <runtime number>		Set the number of working runtime	[default:1]\n"
			+ " -s <transaction size>		Set the number of read/write pairs in one transaction	[default:1000]\n"
			+ " -t <transaction num>	Set the number of transactions in one run	[default:1000]\n"
			+ " -i <stream id>	Set the stream id	[default:A]\n";

	public class Experiment implements Runnable {
		public Experiment(Object runtimeId, Object args, CountDownLatch latch) {
			// store parameter for later user
			this.runtimeId = (Integer) runtimeId;
			this.args = (String[]) args;
			this.latch = latch;
		}

		public void run() {
			BaseCorfuAppUtils app = new MapManipulator(runtimeId, latch);
			app.start(args);
		}

		Integer runtimeId;
		String[] args;

		CountDownLatch latch;
	}

	public static void main(String[] args) {
		Map<String, Object> opts =
				new Docopt(USAGE)
						.withVersion(GitRepositoryState.getRepositoryState().describe)
						.parse(args);
		String corfuConfigurationString = (String) opts.get("-c");

		String runtimeNumberString = (String) opts.get("-n");

		Integer runtimeNumber = 1;

		if (runtimeNumberString != null) {
			runtimeNumber = Integer.parseInt(runtimeNumberString);
		}

		CountDownLatch latch = new CountDownLatch(runtimeNumber);

		for (int i = 0; i < runtimeNumber; ++i) {
			new Thread(new MultiRuntime().new Experiment(i, args, latch)).start();
		}


	}

}
