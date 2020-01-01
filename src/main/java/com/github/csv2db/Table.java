package com.github.csv2db;

import java.util.List;

public class Table {

	public String name;

	public List<Column> columns;

	public Table(String name, List<Column> columns) {
		this.name = name;
		this.columns = columns;
	}

	public static class Column {

		public String name;

		public String isNullable;

		public String dataType;

		public String maxLength;

		public Column(String name, String isNullable, String dataType, String maxLength) {
			this.name = name;
			this.isNullable = isNullable;
			this.dataType = dataType;
			this.maxLength = maxLength;
		}
	}
}
