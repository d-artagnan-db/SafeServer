package pt.uminho.haslab.saferegions.secureRegionScanner;

import java.util.Arrays;

public class Column {

	private final byte[] cf;
	private final byte[] cq;

	public Column(byte[] cf, byte[] cq) {
		this.cf = cf;
		this.cq = cq;
	}

	public byte[] getCf() {
		return cf;
	}

	public byte[] getCq() {
		return cq;
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 43 * hash + Arrays.hashCode(this.cf);
		hash = 43 * hash + Arrays.hashCode(this.cq);
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final Column other = (Column) obj;
		if (!Arrays.equals(this.cf, other.cf)) {
			return false;
		}
		return Arrays.equals(this.cq, other.cq);
	}

}
