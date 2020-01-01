package com.github.csv2db;

public class CsvImportException extends RuntimeException {

	private static final long serialVersionUID = -2002801885116031444L;

	public CsvImportException(Throwable cause) {
		super(cause);
	}

	public CsvImportException(String message) {
		super(message);
	}
}
