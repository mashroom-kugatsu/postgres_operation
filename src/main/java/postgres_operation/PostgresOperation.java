package postgres_operation;

public class PostgresOperation {

	public static void main(String[] args) {
		CopyOperation copyOperation= new CopyOperation();
		copyOperation.CopyOperationPsgr("access_log", "csv", "access_log");
		copyOperation.CopyOperationPsgr("customers", "csv", "customers");
		
		CreateTableOperation createTableOperation = new CreateTableOperation();
		createTableOperation.CreateTableOperationPsgr("access_log_summary");
	}

}
