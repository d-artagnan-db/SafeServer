package pt.uminho.haslab.smcoprocessors;

import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.junit.Test;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.Family;
import pt.uminho.haslab.safemapper.Qualifier;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smcoprocessors.secretSearch.ComposedSearchValue;
import pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition;
import pt.uminho.haslab.smcoprocessors.secretSearch.SearchValue;
import pt.uminho.haslab.smcoprocessors.secretSearch.UnarySearchValue;
import pt.uminho.haslab.smcoprocessors.secureFilters.*;
import pt.uminho.haslab.smcoprocessors.secureRegionScanner.HandleSafeFilter;

import java.math.BigInteger;
import java.util.List;

import static org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.EQUAL;
import static org.junit.Assert.assertEquals;
import static pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition.Condition;
import static pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition.Condition.And;
import static pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition.Condition.Or;

public class HandleSafeFilterTest {

	private final byte[] column = "teste".getBytes();
	private final byte[] val = BigInteger.ONE.toByteArray();

	@Test
	public void testSCVF() {

		CompareOp[] ops = {EQUAL, CompareOp.GREATER,
				CompareOp.GREATER_OR_EQUAL, CompareOp.LESS,
				CompareOp.LESS_OR_EQUAL};

		for (CompareOp op : ops) {
			testeSCVF(op);
		}
	}

	@Test
	public void testSCVFWhileMatch() {
		CompareOp op = EQUAL;

		SingleColumnValueFilter filter = new SingleColumnValueFilter(column,
				column, op, val);
		WhileMatchFilter wmf = new WhileMatchFilter(filter);
		HandleSafeFilter sfh = new HandleSafeFilter(getTableSchema(), wmf, null);
		sfh.processFilter();

		assertEquals(true, sfh.isStopOnMatch());

		SecureFilter scond = sfh.getSecureFilter();
		assertEquals(true, scond instanceof SearchConditionFilter);
		SearchConditionFilter scf = (SearchConditionFilter) scond;

		validate(op, scf.getCondition());

	}

	@Test
	public void testFilterListAllProtected() {

		FilterList fList = generateFilterList();
		HandleSafeFilter sfh = new HandleSafeFilter(getTableSchema(), fList,
				null);
		sfh.processFilter();
		SecureFilter scond = sfh.getSecureFilter();

		assertEquals(true, scond instanceof SecureFilterList);
		validateListProtected(fList, (SecureFilterList) scond);

	}

	private FilterList generateFilterList() {
		FilterList fList = new FilterList();
		SingleColumnValueFilter fOne = new SingleColumnValueFilter(column,
				column, CompareOp.EQUAL, val);
		SingleColumnValueFilter fTwo = new SingleColumnValueFilter(column,
				column, CompareOp.GREATER, val);
		SingleColumnValueFilter fThree = new SingleColumnValueFilter(column,
				column, CompareOp.GREATER_OR_EQUAL, val);
		SingleColumnValueFilter fFour = new SingleColumnValueFilter(column,
				column, CompareOp.LESS, val);
		SingleColumnValueFilter fFive = new SingleColumnValueFilter(column,
				column, CompareOp.LESS_OR_EQUAL, val);
		fList.addFilter(fOne);
		fList.addFilter(fTwo);
		fList.addFilter(fThree);
		fList.addFilter(fFour);
		fList.addFilter(fFive);
		return fList;
	}

	private void validateListProtected(FilterList fList, SecureFilterList list) {

		List<Filter> originalFilters = fList.getFilters();
		List<SecureFilter> parsedFilters = list.getFilterList();

		assertEquals(originalFilters.size(), parsedFilters.size());

		if (fList.getOperator() == FilterList.Operator.MUST_PASS_ALL) {
			assertEquals(And, list.getCond());
		} else {
			assertEquals(Or, list.getCond());
		}

		for (SecureFilter f : parsedFilters) {
			assertEquals(true, f instanceof SearchConditionFilter);
		}
	}

	@Test
	public void testFilterListAllUnProtected() {

		FilterList fList = generateUnprotectedFilterList();
		HandleSafeFilter sfh = new HandleSafeFilter(getTableSchema(), fList,
				null);
		sfh.processFilter();
		SecureFilter scond = sfh.getSecureFilter();

		assertEquals(true, scond instanceof SecureFilterList);
		validateListUnprotected(fList, (SecureFilterList) scond);

	}

	private FilterList generateUnprotectedFilterList() {
		byte[] ucolumn = "ucol".getBytes();
		FilterList fList = new FilterList();
		SingleColumnValueFilter fOne = new SingleColumnValueFilter(ucolumn,
				ucolumn, CompareOp.EQUAL, val);
		SingleColumnValueFilter fTwo = new SingleColumnValueFilter(ucolumn,
				ucolumn, CompareOp.GREATER, val);
		SingleColumnValueFilter fThree = new SingleColumnValueFilter(ucolumn,
				ucolumn, CompareOp.GREATER_OR_EQUAL, val);
		SingleColumnValueFilter fFour = new SingleColumnValueFilter(ucolumn,
				ucolumn, CompareOp.LESS, val);
		SingleColumnValueFilter fFive = new SingleColumnValueFilter(ucolumn,
				ucolumn, CompareOp.LESS_OR_EQUAL, val);
		fList.addFilter(fOne);
		fList.addFilter(fTwo);
		fList.addFilter(fThree);
		fList.addFilter(fFour);
		fList.addFilter(fFive);
		return fList;
	}

	private void validateListUnprotected(FilterList fList, SecureFilterList list) {

		List<Filter> originalFilters = fList.getFilters();
		List<SecureFilter> parsedFilters = list.getFilterList();

		assertEquals(originalFilters.size(), parsedFilters.size());

		if (fList.getOperator() == FilterList.Operator.MUST_PASS_ALL) {
			assertEquals(And, list.getCond());
		} else {
			assertEquals(Or, list.getCond());
		}

		for (SecureFilter f : parsedFilters) {
			assertEquals(true, f instanceof SecureSingleColumnValueFilter);
		}
	}

	@Test
	public void testUnprotectedSCVF() {

		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				"teste2".getBytes(), "teste2".getBytes(), EQUAL, val);
		HandleSafeFilter sfh = new HandleSafeFilter(getTableSchema(), filter,
				null);
		sfh.processFilter();
		SecureFilter sf = sfh.getSecureFilter();
		assertEquals(true, sf instanceof SecureSingleColumnValueFilter);

	}

	@Test
	public void testeRowFilter() {

		RowFilter rFilter = new RowFilter(EQUAL, null);

		HandleSafeFilter sfh = new HandleSafeFilter(getTableSchema(), rFilter,
				null);
		sfh.processFilter();

		SecureFilter sf = sfh.getSecureFilter();
		assertEquals(true, sf instanceof SecureIdentifierFilter);

	}

	private void testeSCVF(CompareOp cOp) {

		SingleColumnValueFilter filter = new SingleColumnValueFilter(column,
				column, cOp, val);
		HandleSafeFilter sfh = new HandleSafeFilter(getTableSchema(), filter,
				null);
		sfh.processFilter();
		SecureFilter scond = sfh.getSecureFilter();
		assertEquals(true, scond instanceof SearchConditionFilter);
		SearchConditionFilter scf = (SearchConditionFilter) scond;
		validate(cOp, scf.getCondition());

	}

	private void validate(CompareOp op, SearchCondition cond) {

		switch (op) {
			case EQUAL :
				assertEquals(true, cond instanceof SearchValue);
				assertEquals(Condition.Equal, cond.getCondition());
				break;
			case GREATER_OR_EQUAL :
				assertEquals(true, cond instanceof SearchValue);
				assertEquals(Condition.GreaterOrEqualThan, cond.getCondition());
				break;
			case GREATER :
				assertEquals(true, cond instanceof ComposedSearchValue);
				assertEquals(And, cond.getCondition());

				ComposedSearchValue csv = (ComposedSearchValue) cond;
				assertEquals(true, csv.getVal1() instanceof UnarySearchValue);
				assertEquals(Condition.Not, csv.getVal1().getCondition());

				UnarySearchValue usv = (UnarySearchValue) csv.getVal1();
				assertEquals(true,
						usv.getSearchCondition() instanceof SearchValue);
				assertEquals(Condition.Equal, usv.getSearchCondition()
						.getCondition());

				assertEquals(Condition.GreaterOrEqualThan, csv.getVal2()
						.getCondition());
				break;
			case LESS :
				assertEquals(true, cond instanceof UnarySearchValue);
				UnarySearchValue less_usv = (UnarySearchValue) cond;
				assertEquals(true,
						less_usv.getSearchCondition() instanceof SearchValue);
				assertEquals(Condition.Not, less_usv.getCondition());
				assertEquals(Condition.GreaterOrEqualThan, less_usv
						.getSearchCondition().getCondition());
				break;
			case LESS_OR_EQUAL :
				assertEquals(true, cond instanceof ComposedSearchValue);
				assertEquals(Condition.Xor, cond.getCondition());

				ComposedSearchValue loe_csv = (ComposedSearchValue) cond;
				assertEquals(Condition.Equal, loe_csv.getVal1().getCondition());
				assertEquals(true,
						loe_csv.getVal2() instanceof UnarySearchValue);

				UnarySearchValue loe_usv = (UnarySearchValue) loe_csv.getVal2();
				assertEquals(Condition.Not, loe_usv.getCondition());
				assertEquals(Condition.GreaterOrEqualThan, loe_usv
						.getSearchCondition().getCondition());
				break;

		}
	}

	private TableSchema getTableSchema() {
		TableSchema schema = new TableSchema();
		Family fam = new Family();
		fam.setFamilyName(new String(column));
		Qualifier qual = new Qualifier();
		qual.setQualifierName(new String(column));
		qual.setCryptoType(DatabaseSchema.CryptoType.SMPC);
		fam.addQualifier(qual);
		schema.addFamily(fam);
		return schema;
	}
}
