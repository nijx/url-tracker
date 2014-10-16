/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.examples.ldt;


/**
 * This class contains the times for the various phases of the URL-Tracker
 * application.
 * @author toby
 *
 */
public class TestTiming implements IAppConstants {
	

	// Remember the times (in milliseconds) for each phase
	private long[] startTimes; 
	private long[] endTimes; 
	private long[] elapsedTimes;
	private long   totalTime;
	

	public TestTiming() {
		this.startTimes = new long[AppPhases.LAST.ordinal() + 1];
		this.endTimes = new long[AppPhases.LAST.ordinal() + 1];
		this.elapsedTimes = new long[AppPhases.LAST.ordinal() + 1];
		
		setStart();
	}
	
	public void setStartTime( AppPhases phase ) {
		this.startTimes[phase.ordinal()] = System.currentTimeMillis();
	}
	
	public void setEndTime( AppPhases phase ) {
		this.endTimes[phase.ordinal()] = System.currentTimeMillis();
	}
		
	public void setStart() {
		this.startTimes[AppPhases.START.ordinal()] = System.currentTimeMillis();
		this.endTimes[AppPhases.START.ordinal()] = System.currentTimeMillis();
	}
	
	public void setFinish() {
		this.startTimes[AppPhases.LAST.ordinal()] = System.currentTimeMillis();
		this.endTimes[AppPhases.LAST.ordinal()] = System.currentTimeMillis();
		
		computeElapsedTimes();
	}
	
	private void computeElapsedTimes() {
		int i;
		for ( AppPhases phase : AppPhases.values() ) {
			i = phase.ordinal();
			elapsedTimes[i] = endTimes[i] - startTimes[i];
		}
		totalTime = endTimes[AppPhases.LAST.ordinal()] - startTimes[AppPhases.START.ordinal()];	
	}
	
	/**
	 * Get the start and end times for one phase.
	 * @param phase
	 * @return
	 */
	public long[] getPhaseTime( AppPhases phase ) {
		long[] resultTimes = new long[2];
		
		resultTimes[0] = this.startTimes[phase.ordinal()];
		resultTimes[1] = this.endTimes[phase.ordinal()];
		
		return resultTimes;
	}
	
	/**
	 * Get the phase elapsed time.
	 * @param phase
	 * @return
	 */
	public long getPhaseElapsedTime( AppPhases phase ) {
		int i = phase.ordinal();
		return endTimes[i] - startTimes[i];
	}
	
	/**
	 * Show all of the stats that we've accumulated.
	 */
	public void printStats() {
		int phases = AppPhases.LAST.ordinal() + 1;
		System.out.println("************************************************");
		
		for ( AppPhases phase : AppPhases.values() ) {
			System.out.printf("Phase(%s) Elapsed Time: %d ms \n",
					phase.toString(), elapsedTimes[phase.ordinal()] );
		}
		System.out.printf("TOTAL Time: %d ms,  %f sec\n", 
				totalTime, (double) totalTime / 1000  );	
		System.out.println("************************************************");
	}

} // end class TestTiming
