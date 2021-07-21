package com.springbatch.Demo.batch.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.springbatch.Demo.model.Student;

@Component
public class StudentProcessor implements ItemProcessor<Student, Student> {

	@Override
	public Student process(Student item) throws Exception {
		int studentId = item.getStudentId();
	    String name = item.getName().toUpperCase();
	    String address = item.getAddress().toUpperCase();

	    Student transformedStudent = new Student(studentId, name, address);
	    System.out.println("Converted " + item + " into " + transformedStudent);
	    return transformedStudent;
	}

}
