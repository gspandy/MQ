package com.ztesoft.zsmart.zmq.common.filter.impl;

public abstract class Op {
    private String symbol;

    protected Op(String symbol) {
        this.symbol = symbol;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

}
