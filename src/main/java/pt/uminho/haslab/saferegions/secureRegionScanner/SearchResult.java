package pt.uminho.haslab.saferegions.secureRegionScanner;

import pt.uminho.haslab.smhbase.interfaces.Secret;

public class SearchResult {

	private final Column col;

	private final Secret secret;

	public SearchResult(Column col, Secret secret) {
		this.col = col;
		this.secret = secret;
	}

	public Column getCol() {
		return col;
	}

	public Secret getSecret() {
		return secret;
	}

}
