package postgres_operation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

public class CreateTableOperation {
	public void CreateTableOperationPsgr(String createTableName) {
		
		Logger logger = LoggerFactory.getLogger(CreateTableOperation.class);
		ch.qos.logback.classic.Logger log = (ch.qos.logback.classic.Logger) logger;
		log.setLevel(Level.TRACE);

		try {
			// PostgreSQLのドライバを指定
			Class.forName("org.postgresql.Driver");

			// PostgreSQLデータベースに接続 (DB名,ID,パスワードを指定)
			String url = "jdbc:postgresql://localhost/javasample";
			String user = "postgres";
			String password = "postgres";
			Connection conn = DriverManager.getConnection(url, user, password);

			// ステートメントの作成
			Statement stmt = conn.createStatement();

			// 既存テーブルの削除
			String sqlDropTable = "drop table if exists " + createTableName;
			stmt.executeUpdate(sqlDropTable);
			
			//ログ出力
			logger.trace(createTableName + "テーブルの既存レコード削除");
					
			//テーブルの作成
			String sqlCreateTable =
					"create table " +  createTableName + " as(\r\n" + 
					"select\r\n" + 
					"    access_log.*\r\n" + 
					"	,customers.customer_name\r\n" + 
					"	,customers.customer_birthday\r\n" + 
					"	,customers.customer_gender\r\n" + 
					"	,customers.customer_location\r\n" + 
					"from access_log\r\n" + 
					"left join customers\r\n" + 
					"on access_log.customer_id = customers.customer_id\r\n" + 
					")";
			stmt.executeUpdate(sqlCreateTable);
			
			//ログ出力
			logger.trace(createTableName + "テーブルの作成");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("--------------------");
			System.out.println(createTableName + "テーブル作成　終わり");
			System.out.println("--------------------");
		}
		//ログ出力
		logger.trace(createTableName + "テーブルの更新完了");
	}
}
