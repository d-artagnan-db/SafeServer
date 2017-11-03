package pt.uminho.haslab.smcoprocessors;

public class OperationAttributesIdentifiers {


	public static String RequestIdentifier = "requestID";
	public static String TargetPlayer = "targetPlayer";



	public static String SecretFamily = "SecretFamily";
	public static String SecretQualifier = "SecretQualifier";
	public static String ProtectedColumn = "protectedColumn";
	public static String ScanStartVal = "ScanStartVal";
	public static String ScanStopVal = "ScanStopVal";
	public static String ScanForEqualVal = "ScanForEqualVal";

	public enum ScanType {
		Normal, ProtectedIdentifierGet, ProtectedIdentifierScan, ProtectedColumnScan
	}

}
