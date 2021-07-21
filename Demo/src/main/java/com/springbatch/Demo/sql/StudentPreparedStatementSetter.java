package com.springbatch.Demo.sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.springframework.batch.item.database.ItemPreparedStatementSetter;

import com.springbatch.Demo.model.Student;

public class StudentPreparedStatementSetter implements ItemPreparedStatementSetter<Student>{

	@Override
	public void setValues(Student item, PreparedStatement ps) throws SQLException {
		ps.setString(1, item.getName());
		ps.setString(2, item.getAddress());
		ps.setInt(3, item.getStudentId());
		
	}

}
