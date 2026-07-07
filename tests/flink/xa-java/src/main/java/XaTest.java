import com.mysql.cj.jdbc.MysqlXADataSource;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

// Drives thinDB's XA support with the REAL MySQL Connector/J XAResource — the
// exact path Flink's exactly-once JDBC sink uses.
//
//   (no arg)  full happy path: start/insert/end/prepare/recover/commit
//   prepare   start/insert/end/prepare, then leave the branch PREPARED (crash sim)
//   recover   recover() prepared xids and commit them (post-restart recovery)
public class XaTest {
    static final class MyXid implements Xid {
        final int fmt;
        final byte[] g, b;
        MyXid(int fmt, byte[] g, byte[] b) { this.fmt = fmt; this.g = g; this.b = b; }
        public int getFormatId() { return fmt; }
        public byte[] getGlobalTransactionId() { return g; }
        public byte[] getBranchQualifier() { return b; }
    }

    public static void main(String[] args) throws Exception {
        String phase = args.length > 0 ? args[0] : "full";
        MysqlXADataSource ds = new MysqlXADataSource();
        ds.setUrl("jdbc:mysql://host.docker.internal:13306/main");
        ds.setUser("root");
        ds.setPassword("");
        XAConnection xaConn = ds.getXAConnection();
        XAResource xa = xaConn.getXAResource();
        Connection conn = xaConn.getConnection();
        Xid xid = new MyXid(1, "gtrid-abc".getBytes(), "bq-1".getBytes());

        if (phase.equals("prepare") || phase.equals("full")) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS jx");
                s.execute("CREATE TABLE jx (id INT, v INT, PRIMARY KEY(id))");
            }
            xa.start(xid, XAResource.TMNOFLAGS);
            try (Statement s = conn.createStatement()) {
                s.executeUpdate("INSERT INTO jx (id, v) VALUES (11, 111), (12, 222)");
            }
            xa.end(xid, XAResource.TMSUCCESS);
            System.out.println("PREPARE rc=" + xa.prepare(xid));
            System.out.println("COUNT after prepare = " + count(conn) + " (expect 0)");
        }

        if (phase.equals("recover") || phase.equals("full")) {
            Xid[] recovered = xa.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
            System.out.println("RECOVER returned " + recovered.length + " xid(s)");
            for (Xid rx : recovered) {
                System.out.println("  commit fmt=" + rx.getFormatId()
                        + " gtrid=" + new String(rx.getGlobalTransactionId())
                        + " bqual=" + new String(rx.getBranchQualifier()));
                xa.commit(rx, false);
            }
            System.out.println("COUNT after recover+commit = " + count(conn) + " (expect 2)");
        }
        xaConn.close();
    }

    static int count(Connection conn) throws Exception {
        try (Statement s = conn.createStatement(); ResultSet r = s.executeQuery("SELECT COUNT(*) FROM jx")) {
            r.next();
            return r.getInt(1);
        }
    }
}
