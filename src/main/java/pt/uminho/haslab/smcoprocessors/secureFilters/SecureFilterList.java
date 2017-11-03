package pt.uminho.haslab.smcoprocessors.secureFilters;

import org.apache.hadoop.hbase.Cell;
import pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition;

import java.util.List;

import static pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition.Condition.And;
import static pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition.Condition.Or;

public class SecureFilterList implements SecureFilter {

	private SearchCondition.Condition cond;
	private List<SecureFilter> filters;

	public SecureFilterList(SearchCondition.Condition cond,
			List<SecureFilter> filters) {
		this.cond = cond;
		this.filters = filters;

	}

	public boolean filterRow(List<Cell> row) {
		boolean toFilter = true;
		switch (cond) {
			case Or :
				toFilter = false;
				break;
		}

		for (SecureFilter filter : filters) {

			switch (cond) {
				case And :
					toFilter = filter.filterRow(row);
					break;
				case Or :
					toFilter |= filter.filterRow(row);
					break;
			}

			if (cond  == And && !toFilter) {
				return false;
			}else if (cond == Or && toFilter){
				return true;
			}

		}
		return toFilter;
	}

	public void reset() {
		for (SecureFilter filter : filters) {
			filter.reset();
		}

	}

	public List<SecureFilter> getFilterList() {
		return this.filters;
	}

	public SearchCondition.Condition getCond() {
		return this.cond;
	}

}
