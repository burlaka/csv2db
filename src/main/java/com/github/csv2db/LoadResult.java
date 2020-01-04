package com.github.csv2db;

import java.util.List;
import java.util.Objects;

public class LoadResult {

    private String fileName;
    private int totalRecords;
    private int insertedRecords;
    private List<SkippedRecord> skippedRecords;

    public LoadResult(String fileName, int totalRecords, int insertedRecords, List<SkippedRecord> skippedRecords) {
        this.fileName = fileName;
        this.totalRecords = totalRecords;
        this.insertedRecords = insertedRecords;
        this.skippedRecords = skippedRecords;
    }

    public String getFileName() {
        return fileName;
    }

    public int getTotalRecords() {
        return totalRecords;
    }

    public int getRecordsInserted() {
        return insertedRecords;
    }

    public List<SkippedRecord> getSkippedRecords() {
        return skippedRecords;
    }

    public static class SkippedRecord {

        private int recordIndex;

        private Exception exception;

        public SkippedRecord(int recordIndex, Exception exception) {
            this.recordIndex = recordIndex;
            this.exception = exception;
        }

        public int getRecordIndex() {
            return recordIndex;
        }

        public Exception getException() {
            return exception;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SkippedRecord that = (SkippedRecord) o;
            return recordIndex == that.recordIndex &&
                Objects.equals(exception, that.exception);
        }

        @Override
        public int hashCode() {
            return Objects.hash(recordIndex, exception);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LoadResult that = (LoadResult) o;
        return totalRecords == that.totalRecords &&
            insertedRecords == that.insertedRecords &&
            Objects.equals(fileName, that.fileName) &&
            Objects.equals(skippedRecords, that.skippedRecords);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, totalRecords, insertedRecords, skippedRecords);
    }

	@Override
	public String toString() {
		return "LoadResult [fileName=" + fileName + ", totalRecords=" + totalRecords + ", insertedRecords="
				+ insertedRecords + ", skippedRecords=" + skippedRecords + "]";
	}
}
