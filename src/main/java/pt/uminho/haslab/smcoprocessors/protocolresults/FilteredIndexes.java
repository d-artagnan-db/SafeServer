package pt.uminho.haslab.smcoprocessors.protocolresults;

import java.util.List;

public class FilteredIndexes {

	/**
	 * List of indexes that passed the MPC protocol. In case of the equal
	 * protocol the list only contains one element. For the greater than
	 * protocol it ranges from 0..n.
	 * 
	 */
	private final List<byte[]> indexes;

	public FilteredIndexes(List<byte[]> indexes) {
		this.indexes = indexes;
	}

	public List<byte[]> getIndexes() {
		return indexes;
	}

	public boolean isEmpty() {
		return indexes.isEmpty();
	}

}
