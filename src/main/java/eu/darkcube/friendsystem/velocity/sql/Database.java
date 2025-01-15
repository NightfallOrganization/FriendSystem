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
    private static final String CREATE_TABLE_FRIENDS = """
            CREATE TABLE IF NOT EXISTS `%1$s`
            (
                `person1` VARCHAR(36) NOT NULL,
                `person2` VARCHAR(36) NOT NULL,
                `sorted`  VARCHAR(73) AS
                    (
                        CONCAT(LEAST(`person1`, `person2`), '-', GREATEST(`person1`, `person2`))
                    ) STORED INVISIBLE UNIQUE
            )
            """.formatted(TABLE_FRIENDS);

    private static final String CREATE_TABLE_REQUESTS = """
            CREATE TABLE IF NOT EXISTS `%1$s`
            (
                `requester`  VARCHAR(36) NOT NULL,
                `requested`  VARCHAR(36) NOT NULL,
                `sorted`     VARCHAR(73) AS
                    (
                        CONCAT(LEAST(`requester`, `requested`), '-', GREATEST(`requester`, `requested`))
                    ) STORED INVISIBLE UNIQUE
            )
            """.formatted(TABLE_REQUESTS);

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

    /**
     * 4 arguments: p1, p2, p1, p2
     */
    private static final String SQL_IS_FRIENDS = """
            SELECT 1 FROM `%1$s` WHERE `sorted` = CONCAT(LEAST(?, ?), '-', GREATEST(?, ?)) LIMIT 1
            """.formatted(TABLE_FRIENDS);

    /**
     * 2 arguments: requester, requested
     */
    private static final String SQL_HAS_REQUEST = """
            SELECT 1 FROM `%1$s` WHERE `requester` = ? AND `requested` = ? LIMIT 1
            """.formatted(TABLE_REQUESTS);

    /**
     * 2 arguments: p1, p2
     */
    private static final String SQL_ADD_FRIEND = """
            INSERT INTO `%1$s` (person1, person2) VALUES (?, ?)
            """.formatted(TABLE_FRIENDS);

    /**
     * 2 arguments: requester, requested
     */
    private static final String SQL_ADD_REQUEST = """
            INSERT INTO `%1$s` (requester, requested) VALUES (?, ?)
            """.formatted(TABLE_REQUESTS);

    /**
     * 2 arguments: requester,requested
     */
    private static final String SQL_REMOVE_REQUEST = """
            DELETE FROM `%1$s` WHERE requester=? AND requested=?
            """.formatted(TABLE_REQUESTS);

    /**
     * 4 arguments: p1, p2, p1, p2
     */
    private static final String SQL_REMOVE_FRIEND = """
            DELETE FROM `%1$s` WHERE sorted = CONCAT(LEAST(? , ?), '-', GREATEST(? , ?))
            """.formatted(TABLE_FRIENDS);

    // --------------------------------------------------
    // Select-Statements:
    // --------------------------------------------------
    /**
     * 2 arguments: p, p
     */
    private static final String SQL_GET_FRIENDS = """
            SELECT `person1`, `person2` FROM `%1$s` WHERE `person1` = ? OR `person2` = ?
            """.formatted(TABLE_FRIENDS);

    /**
     * 2 arguments: p, p
     */
    private static final String SQL_GET_REQUESTS = """
            SELECT `requester`, `requested` FROM `%1$s` WHERE `requester` = ? OR `requested` = ?
            """.formatted(TABLE_REQUESTS);

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

//        var u1 = UUID.fromString("d0a3478e-c5ad-4d41-858f-eb34273c3932");
//        var u2 = UUID.fromString("984132b7-9c57-476d-a19c-1b1a33bf12d7");
        var u1 = UUID.randomUUID();
        var u2 = UUID.randomUUID();

        System.out.println(database.sendFriendrequest(u1, u2));
        System.out.println(database.sendFriendrequest(u1, u2));
        System.out.println(database.sendFriendrequest(u2, u1));
        System.out.println(database.sendFriendrequest(u2, u1));

        var u3 = UUID.fromString("a6bb6627-0a83-4337-bb25-cf60d4b6c811");
        var u4 = UUID.fromString("e59fcafd-bf24-48f6-a78d-07bf104d595f");

        System.out.println(database.sendFriendrequest(u3, u4));
        System.out.println(database.sendFriendrequest(u4, u3));

//        database.removeFriend(UUID.fromString("d9884eb8-c7f6-4929-b54e-bae32e974032"),UUID.fromString("b87e62fb-55ef-4c39-aa80-af41cdcaffba"));
    }

    private Connection hikariTransaction() throws SQLException {
        var connection = hikariConnection();
        connection.setAutoCommit(false);
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        return connection;
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
    public RequestResult sendFriendrequest(UUID requester, UUID requested) {
        var requesterString = requester.toString();
        var requestedString = requested.toString();
        Connection connection = null;
        try {
            connection = hikariTransaction();

            boolean alreadyFriends;
            try (var psIsFriends = connection.prepareStatement(SQL_IS_FRIENDS)) {
                psIsFriends.setString(1, requesterString);
                psIsFriends.setString(2, requestedString);
                psIsFriends.setString(3, requesterString);
                psIsFriends.setString(4, requestedString);
                alreadyFriends = psIsFriends.executeQuery().next();
            }
            if (alreadyFriends) {
                connection.rollback();
                return RequestResult.ALREADY_FRIENDS;
            }

            boolean hasRequest;
            boolean hasReverse;
            try (var psHasRequest = connection.prepareStatement(SQL_HAS_REQUEST)) {
                psHasRequest.setString(1, requesterString);
                psHasRequest.setString(2, requestedString);

                hasRequest = psHasRequest.executeQuery().next();
                if (hasRequest) {
                    connection.rollback();
                    return RequestResult.ALREADY_SENT;
                }

                psHasRequest.setString(1, requestedString);
                psHasRequest.setString(2, requesterString);
                hasReverse = psHasRequest.executeQuery().next();
            }

            if (hasReverse) {
                try (var psRemoveRequest = connection.prepareStatement(SQL_REMOVE_REQUEST); var psAddFriend = connection.prepareStatement(SQL_ADD_FRIEND)) {
                    psRemoveRequest.setString(1, requestedString);
                    psRemoveRequest.setString(2, requesterString);
                    if (psRemoveRequest.executeUpdate() != 1)
                        throw new SQLException("Failed to remove friend request from " + requestedString + " to " + requesterString);

                    psAddFriend.setString(1, requesterString);
                    psAddFriend.setString(2, requestedString);
                    if (psAddFriend.executeUpdate() != 1)
                        throw new SQLException("Failed to add friendship between " + requesterString + " and " + requestedString);

                    connection.commit();
                    return RequestResult.ACCEPTED_OUTSTANDING_REQUEST;
                }
            }

            try (var psAddRequest = connection.prepareStatement(SQL_ADD_REQUEST)) {
                psAddRequest.setString(1, requesterString);
                psAddRequest.setString(2, requestedString);
                if (psAddRequest.executeUpdate() != 1)
                    throw new SQLException("Failed to send request from " + requesterString + " to " + requestedString);

                connection.commit();
                return RequestResult.SENT_REQUEST;
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to send friend request (SQL)", e);
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    LOGGER.error("Failed to roll back", ex);
                }
            }
            return RequestResult.FAILED;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOGGER.error("Failed to close connection", e);
                }
            }
        }
    }

    public enum RequestResult {
        ALREADY_SENT, ALREADY_FRIENDS, SENT_REQUEST, ACCEPTED_OUTSTANDING_REQUEST, FAILED
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
             PreparedStatement ps = connection.prepareStatement(SQL_REMOVE_REQUEST)) {

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
