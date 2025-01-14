package eu.darkcube.friendsystem.velocity;

import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.plugin.Plugin;
import com.velocitypowered.api.proxy.ProxyServer;
import org.slf4j.Logger;

import javax.inject.Inject;

@Plugin(id = "friendsystem", name = "FriendSystem", version = "0.1.0-SNAPSHOT")
public class FriendSystemVelocity {

    private final ProxyServer server;
    private final Logger logger;

    @Inject
    public FriendSystemVelocity(ProxyServer server, Logger logger) {
        this.server = server;
        this.logger = logger;

        this.server.getEventManager().register(this, this);
    }

    @Subscribe
    public void onProxyInitialization(ProxyInitializeEvent event) {

    }
}
