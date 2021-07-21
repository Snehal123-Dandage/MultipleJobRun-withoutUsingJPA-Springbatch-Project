package com.springbatch.Demo.model;

public class User {

	private int userId;
	private String name;
	private String address;

	public User(int userId, String name, String address) {
		super();
		this.userId = userId;
		this.name = name;
		this.address = address;
	}

	public User() {
		super();
	}

	public int getUserId() {
		return userId;
	}
	public void setUserId(int userId) {
		this.userId = userId;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}

	@Override
	public String toString() {
		return "User [userId=" + userId + ", name=" + name + ", address=" + address + "]";
	}



}
