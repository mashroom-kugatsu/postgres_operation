package postgres_operation;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

public class CopyOperation {

	public void CopyOperationPsgr(String fileName, String extension, String tableName) {

		Logger logger = LoggerFactory.getLogger(CopyOperation.class);
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

			// テーブルの既存レコードを削除
			String sqlDeleteRecord = "delete from " + tableName;
			stmt.executeUpdate(sqlDeleteRecord);

			// ログ出力
			logger.trace(tableName + "テーブルの既存レコード削除");

			CopyManager copyManager = new CopyManager((BaseConnection) conn);

			String fileDir = "C:\\Users\\S.Matsukawa\\Desktop\\csvサンプル\\";
			String filePath = fileDir + fileName + "." + extension;

			Reader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF8"));

			String sql = "copy " + tableName + " FROM STDIN (DELIMITER ',', FORMAT csv, HEADER true)";
			copyManager.copyIn(sql, reader);

			logger.trace(tableName + "テーブルに" + fileName + "." + extension + "のコピー");

			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("--------------------");
			System.out.println(fileName + "コピー　終わり");
			System.out.println("--------------------");
		}
		//ログ出力
		logger.trace(tableName + "テーブルの更新完了");
	}

}
