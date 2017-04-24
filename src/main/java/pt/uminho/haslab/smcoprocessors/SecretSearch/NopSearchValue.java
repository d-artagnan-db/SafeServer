package pt.uminho.haslab.smcoprocessors.SecretSearch;

import java.util.ArrayList;
import java.util.List;
import pt.uminho.haslab.smcoprocessors.SharemindPlayer;

public class NopSearchValue extends AbstractSearchValue {

	public NopSearchValue(Condition condition, int targetPlayer) {
		super(condition, targetPlayer);
	}

	@Override
	public List<Boolean> evaluateCondition(List<byte[]> value,
			List<byte[]> rowID, SharemindPlayer p) {
		List<Boolean> vals = new ArrayList<Boolean>();

		for (byte[] value1 : value) {
			vals.add(Boolean.TRUE);
		}

		return vals;
	}

	public Condition getCondition() {
		return condition;
	}

}
