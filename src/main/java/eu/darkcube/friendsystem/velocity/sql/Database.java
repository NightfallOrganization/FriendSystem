package eu.darkcube.friendsystem.velocity.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import org.mariadb.jdbc.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Database {
    private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);

    // --------------------------------------------------
    // Konfiguration
    // --------------------------------------------------
    private static final String CONNECT_URL_FORMAT = "jdbc:mariadb://%s:%d/%s?serverTimezone=UTC&allowMultiQueries=true";

    private static final String TABLE_FRIENDS = "friendsystem_friends";
    private static final String TABLE_REQUESTS = "friendsystem_requests";

    // --------------------------------------------------
    // CREATE-Statements
    // --------------------------------------------------
    private static final String CREATE_TABLE_FRIENDS =
            "CREATE TABLE IF NOT EXISTS `" + TABLE_FRIENDS + "` (" +
                    "  `person1` VARCHAR(36) NOT NULL," +
                    "  `person2` VARCHAR(36) NOT NULL," +
                    "  `sorted`  VARCHAR(73) AS (" +
                    "    CONCAT(LEAST(`person1`, `person2`), '-', GREATEST(`person1`, `person2`))" +
                    "  ) STORED INVISIBLE UNIQUE" +
                    ")";

    private static final String CREATE_TABLE_REQUESTS =
            "CREATE TABLE IF NOT EXISTS `" + TABLE_REQUESTS + "` (" +
                    "  `requester`  VARCHAR(36) NOT NULL," +
                    "  `requested`  VARCHAR(36) NOT NULL," +
                    "  `sorted`     VARCHAR(73) AS (" +
                    "    CONCAT(LEAST(`requester`, `requested`), '-', GREATEST(`requester`, `requested`))" +
                    "  ) STORED INVISIBLE UNIQUE" +
                    ")";

    // --------------------------------------------------
    // Multi-Statement: sendFriendrequest
    //
    // Ablauf (A = ?1, B = ?2):
    //   1) Prüfe, ob sie schon Freunde sind (Eintrag in friends-Tabelle).
    //   2) Prüfe, ob es eine identische Request (A->B) bereits gibt.
    //   3) Prüfe, ob es eine entgegengesetzte Request (B->A) bereits gibt.
    //   4) Falls schon befreundet ODER A->B existiert => "no action"
    //   5) Falls B->A existiert => Lösche B->A, lege Freundschaft an => "accepted"
    //   6) Sonst neue Request A->B => "new request"
    //
    // Hier werden 14 Platzhalter belegt.
    // --------------------------------------------------
    private static final String SQL_SEND_FRIENDREQUEST =
            "SET @alreadyFriends = ("
                    + "  SELECT 1 FROM " + TABLE_FRIENDS
                    + "   WHERE sorted = CONCAT(LEAST(? , ?), '-', GREATEST(? , ?)) "
                    + "   LIMIT 1"
                    + ");"
                    + "SET @hasForward = ("
                    + "  SELECT 1 FROM " + TABLE_REQUESTS
                    + "   WHERE requester = ? AND requested = ? "
                    + "   LIMIT 1"
                    + ");"
                    + "SET @hasReverse = ("
                    + "  SELECT 1 FROM " + TABLE_REQUESTS
                    + "   WHERE requester = ? AND requested = ? "
                    + "   LIMIT 1"
                    + ");"
                    + "IF (@alreadyFriends IS NOT NULL OR @hasForward IS NOT NULL) THEN "
                    + "    SELECT 'no action' AS result;"
                    + "ELSEIF (@hasReverse IS NOT NULL) THEN "
                    + "    DELETE FROM " + TABLE_REQUESTS + " WHERE requester = ? AND requested = ?;"
                    + "    INSERT INTO " + TABLE_FRIENDS + " (person1, person2) VALUES (?, ?);"
                    + "    SELECT 'accepted' AS result;"
                    + "ELSE "
                    + "    INSERT INTO " + TABLE_REQUESTS + " (requester, requested) VALUES (?, ?);"
                    + "    SELECT 'new request' AS result;"
                    + "END IF;";

    // --------------------------------------------------
    // Multi-Statement: acceptFriendrequest
    //
    // Ablauf (A = ?1, B = ?2):
    //   1) Prüfe, ob es überhaupt eine offene Request gibt (egal ob A->B oder B->A).
    //   2) Wenn nicht => "no request"
    //   3) Wenn doch => löschen + Freunde-Eintrag => "accepted"
    //
    //   Wir löschen reihenfolgenabhängig in BEIDEN Varianten:
    //   - A->B
    //   - B->A
    // --------------------------------------------------
    private static final String SQL_ACCEPT_FRIENDREQUEST =
            "SET @hasRequestAny = ("
                    + "  SELECT 1 FROM " + TABLE_REQUESTS
                    + "   WHERE sorted = CONCAT(LEAST(? , ?), '-', GREATEST(? , ?)) "
                    + "   LIMIT 1"
                    + ");"
                    + "IF (@hasRequestAny IS NULL) THEN "
                    + "    SELECT 'no request' AS result;"
                    + "ELSE "
                    + "    DELETE FROM " + TABLE_REQUESTS + " "
                    + "     WHERE (requester = ? AND requested = ?) "
                    + "        OR (requester = ? AND requested = ?);"
                    + "    INSERT INTO " + TABLE_FRIENDS + " (person1, person2) VALUES (?, ?);"
                    + "    SELECT 'accepted' AS result;"
                    + "END IF;";

    // --------------------------------------------------
    // Einfache Statements:
    // --------------------------------------------------
    private static final String SQL_ADD_FRIEND =
            "INSERT INTO " + TABLE_FRIENDS + " (person1, person2) VALUES (?, ?)";

    private static final String SQL_REMOVE_FRIEND =
            "DELETE FROM " + TABLE_FRIENDS
                    + " WHERE sorted = CONCAT(LEAST(? , ?), '-', GREATEST(? , ?))";

    private static final String SQL_ADD_REQUEST =
            "INSERT INTO " + TABLE_REQUESTS + " (requester, requested) VALUES (?, ?)";

    // Reihenfolgenabhängig: requester=? AND requested=?
    private static final String SQL_REMOVE_REQUEST_EXACT =
            "DELETE FROM " + TABLE_REQUESTS + " WHERE requester=? AND requested=?";

    // --------------------------------------------------
    // Select-Statements:
    // --------------------------------------------------
    private static final String SQL_GET_FRIENDS =
            "SELECT person1, person2 FROM " + TABLE_FRIENDS
                    + " WHERE person1 = ? OR person2 = ?";

    private static final String SQL_GET_REQUESTS =
            "SELECT requester, requested FROM " + TABLE_REQUESTS
                    + " WHERE requester = ? OR requested = ?";

    // --------------------------------------------------
    // Datenquellen-Handling
    // --------------------------------------------------

    private volatile HikariDataSource hikariDataSource;
    private final DatabaseConfig config;

    public Database(DatabaseConfig config) {
        this.config = config;
    }


    public void load() throws HikariPool.PoolInitializationException {
        var hikariConfig = new HikariConfig();
        var endpoint = config.endpoint();
        hikariConfig.setJdbcUrl(CONNECT_URL_FORMAT.formatted(
                endpoint.address().host(),
                endpoint.address().port(),
                endpoint.database()));
        hikariConfig.setDriverClassName(Driver.class.getName());
        hikariConfig.setUsername(config.username());
        hikariConfig.setPassword(config.password());

        // Hikari-Einstellungen
        hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        hikariConfig.addDataSourceProperty("useServerPrepStmts", "true");
        hikariConfig.addDataSourceProperty("useLocalSessionState", "true");
        hikariConfig.addDataSourceProperty("rewriteBatchedStatements", "true");
        hikariConfig.addDataSourceProperty("cacheResultSetMetadata", "true");
        hikariConfig.addDataSourceProperty("cacheServerConfiguration", "true");
        hikariConfig.addDataSourceProperty("elideSetAutoCommits", "true");
        hikariConfig.addDataSourceProperty("maintainTimeStats", "false");

        hikariConfig.setMinimumIdle(1);
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setConnectionTimeout(10_000);
        hikariConfig.setValidationTimeout(10_000);

        this.hikariDataSource = new HikariDataSource(hikariConfig);

        create();
    }

    public static void main() throws SQLException {
        var database = new Database(DatabaseConfig.DEFAULT);
        database.load();

//        database.removeFriend(UUID.fromString("d9884eb8-c7f6-4929-b54e-bae32e974032"),UUID.fromString("b87e62fb-55ef-4c39-aa80-af41cdcaffba"));
    }

    private Connection hikariConnection() throws SQLException {
        return hikariDataSource.getConnection();
    }

    public void create() {
        try (Connection connection = hikariConnection();
             Statement statement = connection.createStatement()) {

            statement.execute(CREATE_TABLE_FRIENDS);
            statement.execute(CREATE_TABLE_REQUESTS);

        } catch (SQLException e) {
            LOGGER.error("Failed to create default tables", e);
        }
    }

    // --------------------------------------------------
    // Gewünschte Methoden
    // --------------------------------------------------

    /**
     * sendFriendrequest(A, B):
     * - Alles in einer Multi-Statement-Query (SQL_SEND_FRIENDREQUEST)
     * - p1 = A, p2 = B
     */
    public void sendFriendrequest(UUID requester, UUID requested) {
        try (Connection connection = hikariConnection();
             PreparedStatement ps = connection.prepareStatement(SQL_SEND_FRIENDREQUEST)) {

            // 14 Platzhalter in der Reihenfolge:
            // 1,2,3,4 => alreadyFriends
            ps.setString(1, requester.toString());
            ps.setString(2, requested.toString());
            ps.setString(3, requester.toString());
            ps.setString(4, requested.toString());

            // 5,6 => hasForward (A->B)
            ps.setString(5, requester.toString());
            ps.setString(6, requested.toString());

            // 7,8 => hasReverse (B->A)
            ps.setString(7, requested.toString());
            ps.setString(8, requester.toString());

            // 9,10 => DELETE B->A
            ps.setString(9, requested.toString());
            ps.setString(10, requester.toString());

            // 11,12 => INSERT friend (person1, person2)
            ps.setString(11, requester.toString());
            ps.setString(12, requested.toString());

            // 13,14 => INSERT request (requester, requested)
            ps.setString(13, requester.toString());
            ps.setString(14, requested.toString());

            boolean result = ps.execute();
            // Wenn du das SELECT '...' AS result auslesen willst,
            // müsstest du in einer Schleife next() auf die ResultSets gehen.
            // Hier verwerfen wir das Ergebnis einfach oder protokollieren es optional.

        } catch (SQLException e) {
            LOGGER.error("Failed to send friend request (SQL)", e);
        }
    }

    /**
     * acceptFriendrequest(A, B):
     * - Prüft, ob es A->B oder B->A als offenen Request gibt.
     * - Falls nein => no request
     * - Falls ja => delete + insert friend
     */
    public void acceptFriendrequest(UUID personA, UUID personB) {
        try (Connection connection = hikariConnection();
             PreparedStatement ps = connection.prepareStatement(SQL_ACCEPT_FRIENDREQUEST)) {

            // 1,2,3,4 => hasRequestAny
            ps.setString(1, personA.toString());
            ps.setString(2, personB.toString());
            ps.setString(3, personA.toString());
            ps.setString(4, personB.toString());

            // 5,6 => DELETE A->B
            ps.setString(5, personA.toString());
            ps.setString(6, personB.toString());

            // 7,8 => DELETE B->A
            ps.setString(7, personB.toString());
            ps.setString(8, personA.toString());

            // 9,10 => INSERT friend
            ps.setString(9, personA.toString());
            ps.setString(10, personB.toString());

            ps.execute();

        } catch (SQLException e) {
            LOGGER.error("Failed to accept friend request (SQL)", e);
        }
    }

    public void addFriend(UUID personA, UUID personB) {
        try (Connection connection = hikariConnection();
             PreparedStatement ps = connection.prepareStatement(SQL_ADD_FRIEND)) {

            ps.setString(1, personA.toString());
            ps.setString(2, personB.toString());
            ps.executeUpdate();

        } catch (SQLException e) {
            LOGGER.error("Failed to add friend", e);
        }
    }

    public void removeFriend(UUID personA, UUID personB) {
        try (Connection connection = hikariConnection();
             PreparedStatement ps = connection.prepareStatement(SQL_REMOVE_FRIEND)) {

            ps.setString(1, personA.toString());
            ps.setString(2, personB.toString());
            ps.setString(3, personA.toString());
            ps.setString(4, personB.toString());
            ps.executeUpdate();

        } catch (SQLException e) {
            LOGGER.error("Failed to remove friend", e);
        }
    }

    public void addFriendrequest(UUID requester, UUID requested) {
        try (Connection connection = hikariConnection();
             PreparedStatement ps = connection.prepareStatement(SQL_ADD_REQUEST)) {

            ps.setString(1, requester.toString());
            ps.setString(2, requested.toString());
            ps.executeUpdate();

        } catch (SQLException e) {
            LOGGER.error("Failed to add friend request (direct)", e);
        }
    }

    /**
     * removeFriendrequest (reihenfolgenabhängig),
     * d. h. requester=? und requested=?
     */
    public void removeFriendrequest(UUID requester, UUID requested) {
        try (Connection connection = hikariConnection();
             PreparedStatement ps = connection.prepareStatement(SQL_REMOVE_REQUEST_EXACT)) {

            ps.setString(1, requester.toString());
            ps.setString(2, requested.toString());
            ps.executeUpdate();

        } catch (SQLException e) {
            LOGGER.error("Failed to remove friend request (exact)", e);
        }
    }

    /**
     * Alle Freunde eines Spielers
     */
    public List<UUID> getFriendlist(UUID player) {
        var friends = new ArrayList<UUID>();
        try (Connection connection = hikariConnection();
             PreparedStatement ps = connection.prepareStatement(SQL_GET_FRIENDS)) {

            ps.setString(1, player.toString());
            ps.setString(2, player.toString());

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    UUID p1 = UUID.fromString(rs.getString("person1"));
                    UUID p2 = UUID.fromString(rs.getString("person2"));
                    friends.add(p1.equals(player) ? p2 : p1);
                }
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to get friend list", e);
        }
        return friends;
    }

    /**
     * Alle offenen Requests, bei denen player als requester oder requested vorkommt
     */
    public List<String> getFriendrequests(UUID player) {
        List<String> requests = new ArrayList<>();
        try (Connection connection = hikariConnection();
             PreparedStatement ps = connection.prepareStatement(SQL_GET_REQUESTS)) {

            ps.setString(1, player.toString());
            ps.setString(2, player.toString());

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String reqer = rs.getString("requester");
                    String reqed = rs.getString("requested");
                    requests.add(reqer + " -> " + reqed);
                }
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to get friend requests", e);
        }
        return requests;
    }
}
