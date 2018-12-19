package org.corfudb.samples;

import org.corfudb.runtime.collections.SMRMap;

import java.util.Map;

/**
 * A write-only transaction is a normal transaction that has only object-mutator method invokations,
 * and does not perform any object-accessor invocations.
 *
 * In the default (Optimistic) transaction isolation level, since it has no read-set,
 * a write-only transaction never needs to abort due to another transaction's commit.
 *
 * The main reason to group mutator updates into a write-transaction is commit *atomicity*:
 * Either all of mutator updates are visible to an application or none.
 *
 * This program illustrates the write atomicity concept with a simple transaction example.
 *
 * Created by dalia on 12/30/16.
 */
public class ConcurrentWrite extends MultithreadCorfuAppUtils {
	/**
	 * main() and standard setup methods are deferred to BaseCorfuAppUtils
	 * @return
	 */
	static BaseCorfuAppUtils selfFactory() { return new ConcurrentWrite(); }
	public static void main(String[] args) { selfFactory().start(args); }
	static final Integer txn = 10;
	int threadCount = 0;


	/**
	 * This is where activity is started
	 */
	@Override
	@SuppressWarnings("checkstyle:printLine") // Sample code
	void action() {
		Integer threadId = threadCount;
		++threadCount;

		Map<String, Integer> map = instantiateCorfuObject(SMRMap.class, "A");
		Integer valA = 0;
		String key = "A" + threadId;

		long start = System.currentTimeMillis();
		//System.out.println("start: " + start + "\n");

		for (int i = 0 ; i < txn; ++i) {
			TXBegin();
			valA = map.get(key);
			if (valA == null) {
				valA = 0;
			}
			valA += 1;
			System.out.println(valA);
			map.put(key, valA);

			TXEnd();
		}

		long end = System.currentTimeMillis();
		//System.out.println("end: " + end + "\n");
		System.out.println("Thread" + threadId + " Throughput: " + txn*1000.0/(end-start) + " tx/sec\n");

	}
}
