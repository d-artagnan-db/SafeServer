package pt.uminho.haslab.smcoprocessors.secureFilters;

import org.apache.hadoop.hbase.Cell;

import java.util.List;

public interface SecureFilter {

	void reset();

	boolean filterRow(List<Cell> row);

}
