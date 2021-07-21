package com.springbatch.Demo.batch.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.springbatch.Demo.model.User;

@Component
public class UserProcessor implements ItemProcessor<User, User>{
	
	
	@Override
	public User process(User user) throws Exception {
		int userId = user.getUserId();
	    String name = user.getName().toUpperCase();
	    String address = user.getAddress().toUpperCase();

	    User transformedUser = new User(userId, name, address);
	    System.out.println("Converted " + user + " into " + transformedUser);
	    return transformedUser;
	}

}
