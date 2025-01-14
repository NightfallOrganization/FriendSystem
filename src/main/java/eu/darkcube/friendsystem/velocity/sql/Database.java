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
        hikariConfig.setJdbcUrl(CONNECT_URL_FORMAT.formatted(endpoint.address().host(), endpoint.address().port(), endpoint.database()));
        hikariConfig.setDriverClassName(Driver.class.getName());
        hikariConfig.setUsername(config.username());
        hikariConfig.setPassword(config.password());

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

        database.sendRequest(
                UUID.fromString("a6e88c41-68e5-4970-98e6-02c3671d9c1b"),
                UUID.fromString("8dad1d4a-2129-4c49-b758-4138aecdf9fe"));

//        database.sendRequest(
//                UUID.fromString("8dad1d4a-2129-4c49-b758-4138aecdf9fe"),
//                UUID.fromString("a6e88c41-68e5-4970-98e6-02c3671d9c1b"));


//        List<UUID> list = database.getPlayerFriendlist(UUID.fromString("a6e88c41-68e5-4970-98e6-02c3671d9c1b"));
//        System.out.println(list);

//        database.removeFriend(UUID.fromString("a6e88c41-68e5-4970-98e6-02c3671d9c1b"), UUID.fromString("8dad1d4a-2129-4c49-b758-4138aecdf9fe"));
//        database.addFriend(UUID.fromString("a6e88c41-68e5-4970-98e6-02c3671d9c1b"), UUID.randomUUID());

//        database.hikariConnection().createStatement().execute(CREATE_ROW_REQUESTS);
//        database.hikariConnection().createStatement().execute(DELETE_ROW_REQUESTS);
    }

    private static final String TABLE_FRIENDS = "friendsystem_friends";
    private static final String TABLE_REQUESTS = "friendsystem_requests";

    private static final String CREATE_TABLE_FRIENDS = "CREATE TABLE IF NOT EXISTS `" + TABLE_FRIENDS + "` (`person1` VARCHAR(36) NOT NULL, `person2` VARCHAR(36) NOT NULL, `sorted` VARCHAR(73) AS (CONCAT(LEAST(`person1`, `person2`), '-', GREATEST(`person1`, `person2`))) STORED INVISIBLE UNIQUE)";

    private static final String CREATE_TABLE_REQUESTS = "CREATE TABLE IF NOT EXISTS `" + TABLE_REQUESTS + "` (`requester` VARCHAR(36) NOT NULL, `requested` VARCHAR(36) NOT NULL, `sorted` VARCHAR(73) AS (CONCAT(LEAST(`requester`, `requested`), '-', GREATEST(`requester`, `requested`))) STORED INVISIBLE UNIQUE)";

    private static final String CREATE_ROW_REQUESTS = "INSERT INTO `" + TABLE_REQUESTS + "` (`requester`, `requested`) VALUES (?, ?)";
    private static final String DELETE_ROW_REQUESTS = "DELETE FROM `" + TABLE_REQUESTS + "` WHERE `requester` = ? AND `requested` = ?";

    private static final String CREATE_FRIEND = "INSERT INTO `" + TABLE_FRIENDS + "` (`person1`, `person2`) VALUES (?, ?)";

    private static final String DELETE_FRIEND = "DELETE FROM `" + TABLE_FRIENDS + "` " + "WHERE sorted = CONCAT(LEAST(?, ?), '-', GREATEST(?, ?))";

    // Liste aller Freunde eines Spielers
    private static final String SELECT_FRIENDS = "SELECT person1, person2 FROM `" + TABLE_FRIENDS + "` WHERE person1 = ? OR person2 = ?";

    // Abfragen für Existenzprüfung (Request/Freundschaft):
    private static final String SELECT_REQUEST_EXISTS = "SELECT 1 FROM `" + TABLE_REQUESTS + "` " + "WHERE sorted = CONCAT(LEAST(?, ?), '-', GREATEST(?, ?))";
    private static final String SELECT_FRIENDS_EXISTS = "SELECT 1 FROM `" + TABLE_FRIENDS + "` " + "WHERE sorted = CONCAT(LEAST(?, ?), '-', GREATEST(?, ?))";

    public void create() {
        try (Connection connection = hikariConnection()) {
            try (Statement statement = connection.createStatement()) { // Schließt danach das Statement
                statement.execute(CREATE_TABLE_FRIENDS);
                statement.execute(CREATE_TABLE_REQUESTS);
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to create default tables", e);
        }
    }

    private Connection hikariConnection() throws SQLException {
        return hikariDataSource.getConnection();
    }

    public void addFriendRequest(UUID requester, UUID requested) {
        try (Connection connection = hikariConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(CREATE_ROW_REQUESTS)) {
                statement.setString(1, requester.toString());
                statement.setString(2, requested.toString());
                statement.execute();
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to add friend request", e);
        }
    }

    public void removeFriendRequest(UUID requester, UUID requested) {
        try (Connection connection = hikariConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(DELETE_ROW_REQUESTS)) {
                statement.setString(1, requester.toString());
                statement.setString(2, requested.toString());
                statement.execute();
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to remove friend request", e);
        }
    }

    public void sendRequest(UUID requester, UUID requested) {
        if (doesFriendshipExist(requester, requested)) {
            LOGGER.info("sendRequest: {} und {} sind bereits Freunde.", requester, requested);
            return;
        }

        if (doesFriendRequestExist(requester, requested)) {
            LOGGER.info("sendRequest: Request zwischen {} und {} existiert bereits. Akzeptiere automatisch.", requester, requested);
            acceptRequest(requester, requested);
            return;
        }

        addFriendRequest(requester, requested);
        LOGGER.info("sendRequest: Neue Request von {} an {} wurde erstellt.", requester, requested);
    }

    public void acceptRequest(UUID requester, UUID requested) {
        if (!doesFriendRequestExist(requester, requested)) {
            LOGGER.info("acceptRequest: Es existiert keine Anfrage zwischen {} und {}.", requester, requested);
            return;
        }
        removeFriendRequest(requester, requested);
        addFriend(requester, requested);
        LOGGER.info("acceptRequest: Anfrage zwischen {} und {} akzeptiert und als Freundschaft angelegt.", requester, requested);
    }

    public void addFriend(UUID personA, UUID personB) {
        try (Connection connection = hikariConnection();
             PreparedStatement statement = connection.prepareStatement(CREATE_FRIEND)) {

            statement.setString(1, personA.toString());
            statement.setString(2, personB.toString());
            statement.executeUpdate();

        } catch (SQLException e) {
            LOGGER.error("Failed to add friend", e);
        }
    }

    public void removeFriend(UUID personA, UUID personB) {
        try (Connection connection = hikariConnection();
             PreparedStatement statement = connection.prepareStatement(DELETE_FRIEND)) {

            statement.setString(1, personA.toString());
            statement.setString(2, personB.toString());
            statement.setString(3, personA.toString());
            statement.setString(4, personB.toString());
            statement.executeUpdate();

        } catch (SQLException e) {
            LOGGER.error("Failed to remove friend", e);
        }
    }

    public List<UUID> getPlayerFriendlist(UUID player) {
        List<UUID> friends = new ArrayList<>();
        try (Connection connection = hikariConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_FRIENDS)) {

            // player kann in person1 oder person2 stehen
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

    private boolean doesFriendRequestExist(UUID p1, UUID p2) {
        try (Connection connection = hikariConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_REQUEST_EXISTS)) {

            statement.setString(1, p1.toString());
            statement.setString(2, p2.toString());
            statement.setString(3, p1.toString());
            statement.setString(4, p2.toString());

            try (ResultSet rs = statement.executeQuery()) {
                // Wenn wir etwas zurückbekommen, existiert ein Datensatz
                return rs.next();
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to check if friend request exists", e);
        }
        return false;
    }

    private boolean doesFriendshipExist(UUID p1, UUID p2) {
        try (Connection connection = hikariConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_FRIENDS_EXISTS)) {

            statement.setString(1, p1.toString());
            statement.setString(2, p2.toString());
            statement.setString(3, p1.toString());
            statement.setString(4, p2.toString());

            try (ResultSet rs = statement.executeQuery()) {
                // Wenn wir etwas zurückbekommen, existiert ein Datensatz
                return rs.next();
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to check if friendship exists", e);
        }
        return false;
    }

}
