/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.example.workflow;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;

import org.camunda.bpm.model.xml.impl.util.IoUtil;

import io.zeebe.client.ZeebeClient;
import io.zeebe.client.ZeebeClientBuilder;
import io.zeebe.client.api.ZeebeFuture;
import io.zeebe.client.api.response.WorkflowInstanceResult;

public final class NonBlockingWorkflowInstanceCompleter {
	
   static volatile long instancesCreated = 0;
   static volatile long errors = 0;
   
   static volatile long avgCycleTime = 0;
   static volatile long maxCycleTime = 0;
   static volatile long minCycleTime = 0;
   
	
  @SuppressWarnings("unchecked")
public static void main(final String[] args) throws IOException {
	
	InputStream payload = Thread.currentThread().getContextClassLoader().getResourceAsStream("payload.json");
	String payloadString = IoUtil.getStringFromInputStream(payload);
	  
    final String broker = "127.0.0.1:26500";
    final int numberOfInstances = 100_000;
    final String bpmnProcessId = "demoProcessSingleTask";

    final ZeebeClientBuilder builder =
        ZeebeClient.newClientBuilder().gatewayAddress(broker).usePlaintext();

    try (final ZeebeClient client = builder.build()) {
      System.out.println("Creating " + numberOfInstances + " workflow instances");

      final long startTime = System.currentTimeMillis();


      Timer timer = new Timer();
	timer.scheduleAtFixedRate(new TimerTask() {
		
		long prev = 0;
		long prevErrors = 0;
		
		@Override
		public void run() {
			
			long curr = instancesCreated;
			long currErr = errors;
			
			System.out.println("completed: " + (curr - prev)
					+ "; total: " + instancesCreated
					+ "; cycleTime: (avg: " + avgCycleTime + ", min: " + minCycleTime + ", max: "+maxCycleTime+")"
					+ "; errors: " + (currErr - prevErrors));
			
			prev = curr;
			prevErrors = errors;
			
		}
	}, 0, 1000);
      
      LinkedList<Object[]> futures = new LinkedList<>();
      LinkedList<Long> recentCycleTimes = new LinkedList<>();
      long timestampOfLastAverageCalcultion = 0;
      
      while (instancesCreated < numberOfInstances) {
        // this is non-blocking/async => returns a future
        final ZeebeFuture<WorkflowInstanceResult> req =
            client.newCreateInstanceCommand().bpmnProcessId(bpmnProcessId)
            	.latestVersion()
            	.variables(payloadString)
            	.withResult()
            	.send();

        // could put the future somewhere and eventually wait for its completion
        futures.add(new Object[] {req, System.currentTimeMillis()});
        
        while (futures.size() > 1) {
        	
        	Iterator<Object[]> futuresItr = futures.iterator();
        	while (futuresItr.hasNext()) {
				Object[] future = (Object[]) futuresItr.next();
        		try {			
    				
    				ZeebeFuture<WorkflowInstanceResult> f = ((ZeebeFuture<WorkflowInstanceResult>)future[0]);
    				if (f.isDone()) {
    					futuresItr.remove();
    					f.get();
    					instancesCreated++;
    					
    					long now = System.currentTimeMillis();
    					long took = now - (Long) future[1];
    					
    					recentCycleTimes.add(took);
    					
    				}
    				
    			
    				
    			} catch (InterruptedException | ExecutionException e) {
    				errors++;
    			}
			}
        	
        	long now = System.currentTimeMillis();
        	
        	int sampledCycleTimes = recentCycleTimes.size();
        	if (sampledCycleTimes > 0 && now-timestampOfLastAverageCalcultion > 1000) {
				long totalCycleTime = 0;
				long lastMinCycleTime = Long.MAX_VALUE;
				long lastMaxCycleTime = 0;
				for (Long cycleTime : recentCycleTimes) {
					totalCycleTime += cycleTime;
					lastMinCycleTime = lastMinCycleTime < cycleTime ? lastMinCycleTime : cycleTime;
					lastMaxCycleTime = lastMaxCycleTime > cycleTime ? lastMaxCycleTime : cycleTime;
				}
				avgCycleTime = totalCycleTime / sampledCycleTimes;
				minCycleTime = lastMinCycleTime;
				maxCycleTime = lastMaxCycleTime;
				
				recentCycleTimes.clear();
				timestampOfLastAverageCalcultion = now;
			}
        	
        	Thread.onSpinWait();
        	
		}

      }

      // creating one more instance; joining on this future ensures
      // that all the other create commands were handled
      client.newCreateInstanceCommand().bpmnProcessId(bpmnProcessId).latestVersion().send().join();

      System.out.println("Took: " + (System.currentTimeMillis() - startTime));
      
      timer.cancel();
    }
  }
}
