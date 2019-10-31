package com.controller;

public class Users {
    private int id;
    private String name;
    private int age;
    private String email;

    public Users(int id, String name, int age){
        this.id=id;
        this.name=name;
        this.age=age;
    }

    public void setEmail(String email){
        this.email=email;
    }
}
