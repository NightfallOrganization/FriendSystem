package eu.darkcube.friendsystem.velocity.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import org.mariadb.jdbc.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.*;

public class Database {
    private static final String CONNECT_URL_FORMAT = "jdbc:mariadb://%s:%d/%s?serverTimezone=UTC";
    private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);

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
        System.out.println(CREATE_TABLE_FRIENDS);
        System.out.println(CREATE_TABLE_REQUESTS);
        var database = new Database(DatabaseConfig.DEFAULT);
        database.load();
    }

    private static final String TABLE_FRIENDS = "friendsystem_friends";
    private static final String TABLE_REQUESTS = "friendsystem_requests";

    private static final String CREATE_TABLE_FRIENDS =
            "CREATE TABLE IF NOT EXISTS `" + TABLE_FRIENDS + "` (" +
                    "  `person1` VARCHAR(36) NOT NULL," +
                    "  `person2` VARCHAR(36) NOT NULL," +
                    "  `sorted` VARCHAR(73) AS (" +
                    "    CONCAT(LEAST(`person1`, `person2`), '-', GREATEST(`person1`, `person2`))" +
                    "  ) STORED INVISIBLE UNIQUE" +
                    ")";

    private static final String CREATE_TABLE_REQUESTS =
            "CREATE TABLE IF NOT EXISTS `" + TABLE_REQUESTS + "` (" +
                    "  `requester` VARCHAR(36) NOT NULL," +
                    "  `requested` VARCHAR(36) NOT NULL," +
                    "  `sorted` VARCHAR(73) AS (" +
                    "    CONCAT(LEAST(`requester`, `requested`), '-', GREATEST(`requester`, `requested`))" +
                    "  ) STORED INVISIBLE UNIQUE" +
                    ")";

    // Anfragen
    private static final String CREATE_ROW_REQUESTS =
            "INSERT INTO `" + TABLE_REQUESTS + "` (`requester`, `requested`) VALUES (?, ?)";
    private static final String DELETE_ROW_REQUESTS_SORTED =
            "DELETE FROM `" + TABLE_REQUESTS + "` " + "WHERE sorted = CONCAT(LEAST(?, ?), '-', GREATEST(?, ?))";

    // Freunde
    private static final String CREATE_FRIEND =
            "INSERT INTO `" + TABLE_FRIENDS + "` (`person1`, `person2`) VALUES (?, ?)";
    private static final String DELETE_FRIEND =
            "DELETE FROM `" + TABLE_FRIENDS + "` " + "WHERE sorted = CONCAT(LEAST(?, ?), '-', GREATEST(?, ?))";

    // Selects
    private static final String SELECT_FRIENDS =
            "SELECT person1, person2 FROM `" + TABLE_FRIENDS + "` " + "WHERE person1 = ? OR person2 = ?";

    // Existenz (symmetrisch / any direction)
    private static final String SELECT_REQUEST_EXISTS =
            "SELECT 1 FROM `" + TABLE_REQUESTS + "` " + "WHERE sorted = CONCAT(LEAST(?, ?), '-', GREATEST(?, ?))";
    private static final String SELECT_FRIENDS_EXISTS =
            "SELECT 1 FROM `" + TABLE_FRIENDS + "` " + "WHERE sorted = CONCAT(LEAST(?, ?), '-', GREATEST(?, ?))";

    // Existenz (exakt / this direction)
    private static final String SELECT_REQUEST_EXISTS_EXACT =
            "SELECT 1 FROM `" + TABLE_REQUESTS + "` " + "WHERE requester = ? AND requested = ?";

    public void create() {
        try (Connection connection = hikariConnection();
             Statement statement = connection.createStatement()) {

            statement.execute(CREATE_TABLE_FRIENDS);
            statement.execute(CREATE_TABLE_REQUESTS);

        } catch (SQLException e) {
            LOGGER.error("Failed to create default tables", e);
        }
    }

    private Connection hikariConnection() throws SQLException {
        return hikariDataSource.getConnection();
    }

    /*
     * =============================================================================
     *          Öffentliche Methoden (öffnen/schließen eigene Connection)
     * =============================================================================
     */

    public void addFriendRequest(UUID requester, UUID requested) {
        try (Connection connection = hikariConnection()) {
            addFriendRequestInternal(connection, requester, requested);
        } catch (SQLException e) {
            LOGGER.error("Failed to add friend request", e);
        }
    }

    public void removeFriendRequest(UUID requester, UUID requested) {
        try (Connection connection = hikariConnection()) {
            removeFriendRequestInternal(connection, requester, requested);
        } catch (SQLException e) {
            LOGGER.error("Failed to remove friend request", e);
        }
    }

    public void addFriend(UUID personA, UUID personB) {
        try (Connection connection = hikariConnection()) {
            addFriendInternal(connection, personA, personB);
        } catch (SQLException e) {
            LOGGER.error("Failed to add friend", e);
        }
    }

    public void removeFriend(UUID personA, UUID personB) {
        try (Connection connection = hikariConnection()) {
            removeFriendInternal(connection, personA, personB);
        } catch (SQLException e) {
            LOGGER.error("Failed to remove friend", e);
        }
    }

    /**
     * sendRequest:
     *
     * - Prüft, ob die beiden Spieler schon Freunde sind.
     *      -> Falls ja, Abbruch.
     *
     * - Prüft, ob eine Anfrage **exakt** (A→B) schon existiert.
     *      -> Falls ja, erneut A→B? Dann machen wir nichts (Verhinderung Doppel-Request).
     *
     * - Prüft, ob eine Anfrage **exakt** (B→A) existiert.
     *      -> Falls ja, d.h. B hatte A angefragt, und jetzt kommt A→B. Dann akzeptieren wir.
     *
     * - Falls keine existiert, legen wir eine neue A→B an.
     *
     * Alles in einer Transaktion.
     */
    public void sendRequest(UUID requester, UUID requested) {
        try (Connection connection = hikariConnection()) {
            connection.setAutoCommit(false);

            if (doesFriendshipExistInternal(connection, requester, requested)) {
                LOGGER.info("sendRequest: {} und {} sind bereits Freunde.", requester, requested);
                connection.rollback();
                return;
            }

            // Prüfen, ob es A→B schon gibt (dann nichts tun).
            if (doesFriendRequestExistExact(connection, requester, requested)) {
                LOGGER.info("sendRequest: Eine offene Anfrage von {} an {} existiert bereits. Keine Aktion.", requester, requested);
                connection.rollback();
                return;
            }

            // Prüfen, ob es B→A schon gibt (dann akzeptieren).
            if (doesFriendRequestExistExact(connection, requested, requester)) {
                LOGGER.info("sendRequest: Entgegengesetzte Anfrage {}→{} existierte bereits. Akzeptiere jetzt automatisch!", requested, requester);
                acceptRequestInternal(connection, requested, requester);
                connection.commit();
                return;
            }

            // Neue Anfrage A→B erstellen
            addFriendRequestInternal(connection, requester, requested);
            LOGGER.info("sendRequest: Neue Request von {} an {} wurde erstellt.", requester, requested);

            connection.commit();
        } catch (SQLException e) {
            LOGGER.error("Failed to send friend request", e);
        }
    }

    /**
     * acceptRequest:
     * - Prüft, ob eine Request (egal in welcher Richtung) existiert.
     * - Falls nicht vorhanden, Abbruch.
     * - Falls vorhanden, wird gelöscht und eine Freundschaft angelegt.
     *
     * Bleibt symmetrisch, d.h. man kann z. B. acceptRequest(A, B) aufrufen,
     * solange irgendeine offene Anfrage existiert (A→B oder B→A).
     */
    public void acceptRequest(UUID requester, UUID requested) {
        try (Connection connection = hikariConnection()) {
            connection.setAutoCommit(false);

            if (!doesFriendRequestExistAnyDirection(connection, requester, requested)) {
                LOGGER.info("acceptRequest: Es existiert keine Anfrage zwischen {} und {}.", requester, requested);
                connection.rollback();
                return;
            }

            removeFriendRequestInternal(connection, requester, requested);
            addFriendInternal(connection, requester, requested);
            LOGGER.info("acceptRequest: Anfrage zwischen {} und {} akzeptiert (unabhängig von Richtung).", requester, requested);

            connection.commit();
        } catch (SQLException e) {
            LOGGER.error("Failed to accept friend request", e);
        }
    }

    public List<UUID> getPlayerFriendlist(UUID player) {
        List<UUID> friends = new ArrayList<>();
        try (Connection connection = hikariConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_FRIENDS)) {

            statement.setString(1, player.toString());
            statement.setString(2, player.toString());

            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    UUID person1 = UUID.fromString(rs.getString("person1"));
                    UUID person2 = UUID.fromString(rs.getString("person2"));
                    // Füge den "anderen" Spieler in die Liste
                    if (person1.equals(player)) {
                        friends.add(person2);
                    } else {
                        friends.add(person1);
                    }
                }
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to get friend list", e);
        }
        return friends;
    }

    /*
     * =============================================================================
     *          Interne Methoden (verwenden bestehende Connection)
     * =============================================================================
     */

    private void addFriendRequestInternal(Connection connection, UUID requester, UUID requested) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(CREATE_ROW_REQUESTS)) {
            statement.setString(1, requester.toString());
            statement.setString(2, requested.toString());
            statement.executeUpdate();
        }
    }

    /**
     * Entfernt eine Friend-Request über die Spalte 'sorted',
     * sodass es egal ist, wer 'requester' und wer 'requested' war.
     */
    private void removeFriendRequestInternal(Connection connection, UUID p1, UUID p2) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(DELETE_ROW_REQUESTS_SORTED)) {
            statement.setString(1, p1.toString());
            statement.setString(2, p2.toString());
            statement.setString(3, p1.toString());
            statement.setString(4, p2.toString());
            statement.executeUpdate();
        }
    }

    private void addFriendInternal(Connection connection, UUID personA, UUID personB) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(CREATE_FRIEND)) {
            statement.setString(1, personA.toString());
            statement.setString(2, personB.toString());
            statement.executeUpdate();
        }
    }

    private void removeFriendInternal(Connection connection, UUID personA, UUID personB) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(DELETE_FRIEND)) {
            statement.setString(1, personA.toString());
            statement.setString(2, personB.toString());
            statement.setString(3, personA.toString());
            statement.setString(4, personB.toString());
            statement.executeUpdate();
        }
    }

    /**
     * Prüft, ob eine Friend-Request (in beliebiger Richtung) existiert (A↔B).
     */
    private boolean doesFriendRequestExistAnyDirection(Connection connection, UUID p1, UUID p2) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_REQUEST_EXISTS)) {
            statement.setString(1, p1.toString());
            statement.setString(2, p2.toString());
            statement.setString(3, p1.toString());
            statement.setString(4, p2.toString());
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        }
    }

    /**
     * Prüft, ob genau A→B bereits in der Tabelle steht (exakt diese Richtung).
     */
    private boolean doesFriendRequestExistExact(Connection connection, UUID from, UUID to) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_REQUEST_EXISTS_EXACT)) {
            statement.setString(1, from.toString());
            statement.setString(2, to.toString());
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        }
    }

    /**
     * Prüft, ob eine Freundschaft (A↔B) existiert (symmetrisch).
     */
    private boolean doesFriendshipExistInternal(Connection connection, UUID p1, UUID p2) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_FRIENDS_EXISTS)) {
            statement.setString(1, p1.toString());
            statement.setString(2, p2.toString());
            statement.setString(3, p1.toString());
            statement.setString(4, p2.toString());
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        }
    }

    /**
     * Interne Methode, um eine vorhandene Anfrage (egal wer wen angefragt hat)
     * zu akzeptieren: Request löschen + Freundschaft hinzufügen.
     */
    private void acceptRequestInternal(Connection connection, UUID requester, UUID requested) throws SQLException {
        removeFriendRequestInternal(connection, requester, requested);
        addFriendInternal(connection, requester, requested);
    }
}
