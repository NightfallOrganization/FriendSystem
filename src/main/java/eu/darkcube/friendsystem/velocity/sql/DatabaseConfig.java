package eu.darkcube.friendsystem.velocity.sql;

public record DatabaseConfig(String username, String password, ConnectionEndpoint endpoint) {
    public static final DatabaseConfig DEFAULT = new DatabaseConfig("root", "", new ConnectionEndpoint("test", new HostAndPort("127.0.0.1", 3306)));

    public record ConnectionEndpoint(String database, HostAndPort address) {
    }

    public record HostAndPort(String host, int port) {
    }
}
