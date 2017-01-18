package pt.uminho.haslab.smcoprocessors.SecretSearch;

import pt.uminho.haslab.smcoprocessors.SharemindPlayer;

public class NopSearchValue extends AbstractSearchValue {

	public NopSearchValue(Condition condition, int targetPlayer) {
		super(condition, targetPlayer);
	}

	@Override
	public boolean evaluateCondition(byte[] value, byte[] rowID,
			SharemindPlayer p) {
		return true;
	}

	public Condition getCondition() {
		return condition;
	}

}
