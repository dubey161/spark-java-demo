package com.example;

public class IntegerWIthDoubleNumber implements java.io.Serializable{
    public double inputData;
    public double inputData2;

    IntegerWIthDoubleNumber(double inputData) {
        this.inputData = inputData;
        this.inputData2 = inputData*2;
    }
}
