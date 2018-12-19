package org.corfudb.samples;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.util.Map;

public abstract class MultithreadCorfuAppUtils extends BaseCorfuAppUtils{

	private CorfuRuntime getRuntimeAndConnect(String configurationString) {
		CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
		return corfuRuntime;
	}

	private static final String USAGE = "Usage: HelloCorfu [-c <conf>] [-t <thread number>]\n"
			+ "Options:\n"
			+ " -c <conf>     Set the configuration host and port  [default: localhost:9999]\n"
			+ " -t <thread number>		Set the number of working threads	[default: 1]\n";


	/**
	 * boilerplate activity generator, to be invoked from an application's main().
	 *
	 * @param args are the args passed to main
	 */
	void start(String[] args) {
		// Parse the options given, using docopt.
		Map<String, Object> opts =
				new Docopt(USAGE)
						.withVersion(GitRepositoryState.getRepositoryState().describe)
						.parse(args);
		String corfuConfigurationString = (String) opts.get("-c");

		String threadNumberString = (String) opts.get("-n");
		Integer threadNumber = 1;
		if (threadNumberString != null) {
			threadNumber = Integer.parseInt(threadNumberString);
		}

		/**
		 * Must set up a Corfu runtime before everything.
		 */
		setCorfuRuntime( getRuntimeAndConnect(corfuConfigurationString) );

		/**
		 * Obviously, this application is not doing much yet,
		 * but you can already invoke getRuntimeAndConnect to test if you can connect to a deployed Corfu service.
		 *
		 * Next, invoke a class-specific activity wrapper named 'action()'.
		 */
		//System.out.println(threadNumber + " thread(s) are running.\n");

		for (int i = 0; i < threadNumber; ++i) {
			new Thread(() -> {
				action();
			}
			).start();
		}
	}
}
