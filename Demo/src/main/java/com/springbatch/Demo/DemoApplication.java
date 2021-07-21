package com.springbatch.Demo;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@EnableScheduling
@EnableBatchProcessing
public class DemoApplication {

	@Autowired
    JobLauncher jobLauncher;
	
    @Autowired
    Job job;

    @Autowired
    @Qualifier("job1")
    private Job job1;

    @Autowired
    @Qualifier("job2")
    private Job job2;
    
     
	public static void main(String[] args) throws NoSuchJobException {
		SpringApplication.run(DemoApplication.class, args);
		
	}

	@Scheduled(cron = "0 * * ? * *")
	public void run1() throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException, NoSuchJobException{
	    Map<String, JobParameter> confMap = new HashMap<>();
	    confMap.put("time", new JobParameter(System.currentTimeMillis()));
	    JobParameters jobParameters = new JobParameters(confMap);
	System.out.println("Jobparameter for job 1 is " + jobParameters);
	        jobLauncher.run(job1, jobParameters);
	}

	@Scheduled(cron = "0 * * ? * *")
	public void run2() throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException, NoSuchJobExecutionException, NoSuchJobException{
	    Map<String, JobParameter> confMap = new HashMap<>();
	    confMap.put("time", new JobParameter(System.currentTimeMillis()));
	    JobParameters jobParameters = new JobParameters(confMap);
	System.out.println("Jobparameter for job 2 is " + jobParameters);
	        jobLauncher.run(job2, jobParameters);   
	}


}
