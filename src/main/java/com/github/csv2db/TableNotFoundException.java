package com.github.csv2db;

public class TableNotFoundException extends RuntimeException {

	private static final long serialVersionUID = 4652536311022491663L;

	private final String tableName;

	public TableNotFoundException(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public String getMessage() {
		return "Table with name [" + tableName + "] not found";
	}
}
