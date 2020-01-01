package com.github.csv2db;

import java.util.List;
import java.util.Objects;

public class LoadResult {

    private String fileName;
    private int totalRecords;
    private int recordsInserted;
    private List<SkippedRecord> skippedRecords;

    public LoadResult(String fileName, int totalRecords, int recordsInserted, List<SkippedRecord> skippedRecords) {
        this.fileName = fileName;
        this.totalRecords = totalRecords;
        this.recordsInserted = recordsInserted;
        this.skippedRecords = skippedRecords;
    }

    public String getFileName() {
        return fileName;
    }

    public int getTotalRecords() {
        return totalRecords;
    }

    public int getRecordsInserted() {
        return recordsInserted;
    }

    public List<SkippedRecord> getSkippedRecords() {
        return skippedRecords;
    }

    public static class SkippedRecord {

        private int recordIndex;
        private String exceptionMessage;

        public SkippedRecord(int recordIndex, String exceptionMessage) {
            this.recordIndex = recordIndex;
            this.exceptionMessage = exceptionMessage;
        }

        public int getRecordIndex() {
            return recordIndex;
        }

        public String getExceptionMessage() {
            return exceptionMessage;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SkippedRecord that = (SkippedRecord) o;
            return recordIndex == that.recordIndex &&
                Objects.equals(exceptionMessage, that.exceptionMessage);
        }

        @Override
        public int hashCode() {

            return Objects.hash(recordIndex, exceptionMessage);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LoadResult that = (LoadResult) o;
        return totalRecords == that.totalRecords &&
            recordsInserted == that.recordsInserted &&
            Objects.equals(fileName, that.fileName) &&
            Objects.equals(skippedRecords, that.skippedRecords);
    }

    @Override
    public int hashCode() {

        return Objects.hash(fileName, totalRecords, recordsInserted, skippedRecords);
    }
}
