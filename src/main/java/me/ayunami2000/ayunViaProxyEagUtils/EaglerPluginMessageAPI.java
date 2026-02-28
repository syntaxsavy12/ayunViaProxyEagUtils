package me.ayunami2000.ayunViaProxyEagUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import net.raphimc.netminecraft.packet.PacketTypes;
import net.raphimc.vialegacy.protocol.release.r1_6_4tor1_7_2_5.types.Types1_6_4;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class EaglerPluginMessageAPI {

	public interface PluginMessageListener {
		void onMessage(UUID playerUUID, String username, String channel, byte[] data);
	}

	public interface ServerPluginMessageListener {
		void onServerMessage(UUID playerUUID, String username, String channel, byte[] data);
	}

	public interface PlayerEventListener {
		void onJoin(UUID playerUUID, String username);
		void onQuit(UUID playerUUID, String username);
	}

	private static final Map<UUID, PlayerEntry> players = new ConcurrentHashMap<>();
	private static final Map<String, List<PluginMessageListener>> channelListeners = new ConcurrentHashMap<>();
	private static final Map<String, List<ServerPluginMessageListener>> serverChannelListeners = new ConcurrentHashMap<>();
	private static final List<PlayerEventListener> playerEventListeners = new ArrayList<>();

	static class PlayerEntry {
		final String username;
		final ChannelHandlerContext ctx;
		final boolean legacy;
		final int pluginMessageId;

		PlayerEntry(String username, ChannelHandlerContext ctx, boolean legacy, int pluginMessageId) {
			this.username = username;
			this.ctx = ctx;
			this.legacy = legacy;
			this.pluginMessageId = pluginMessageId;
		}
	}

	public static void registerChannel(String channel, PluginMessageListener listener) {
		channelListeners.computeIfAbsent(channel, k -> new ArrayList<>()).add(listener);
	}

	public static void unregisterChannel(String channel, PluginMessageListener listener) {
		List<PluginMessageListener> listeners = channelListeners.get(channel);
		if (listeners != null) {
			listeners.remove(listener);
			if (listeners.isEmpty()) {
				channelListeners.remove(channel);
			}
		}
	}

	public static void unregisterAllChannels(PluginMessageListener listener) {
		for (Map.Entry<String, List<PluginMessageListener>> entry : channelListeners.entrySet()) {
			entry.getValue().remove(listener);
			if (entry.getValue().isEmpty()) {
				channelListeners.remove(entry.getKey());
			}
		}
	}

	public static void registerServerChannel(String channel, ServerPluginMessageListener listener) {
		serverChannelListeners.computeIfAbsent(channel, k -> new ArrayList<>()).add(listener);
	}

	public static void unregisterServerChannel(String channel, ServerPluginMessageListener listener) {
		List<ServerPluginMessageListener> listeners = serverChannelListeners.get(channel);
		if (listeners != null) {
			listeners.remove(listener);
			if (listeners.isEmpty()) {
				serverChannelListeners.remove(channel);
			}
		}
	}

	public static void unregisterAllServerChannels(ServerPluginMessageListener listener) {
		for (Map.Entry<String, List<ServerPluginMessageListener>> entry : serverChannelListeners.entrySet()) {
			entry.getValue().remove(listener);
			if (entry.getValue().isEmpty()) {
				serverChannelListeners.remove(entry.getKey());
			}
		}
	}

	public static void registerPlayerListener(PlayerEventListener listener) {
		synchronized (playerEventListeners) {
			playerEventListeners.add(listener);
		}
	}

	public static void unregisterPlayerListener(PlayerEventListener listener) {
		synchronized (playerEventListeners) {
			playerEventListeners.remove(listener);
		}
	}

	public static boolean sendPluginMessage(UUID playerUUID, String channel, byte[] data) {
		PlayerEntry entry = players.get(playerUUID);
		if (entry == null || !entry.ctx.channel().isActive()) {
			return false;
		}
		ByteBuf bb = entry.ctx.alloc().buffer();
		if (entry.legacy) {
			bb.writeByte(250);
			try {
				Types1_6_4.STRING.write(bb, channel);
			} catch (Exception e) {
				bb.release();
				return false;
			}
			bb.writeShort(data.length);
			bb.writeBytes(data);
		} else {
			PacketTypes.writeVarInt(bb, entry.pluginMessageId);
			PacketTypes.writeString(bb, channel);
			bb.writeBytes(data);
		}
		entry.ctx.writeAndFlush(new BinaryWebSocketFrame(bb));
		return true;
	}

	public static boolean isPlayerOnline(UUID playerUUID) {
		PlayerEntry entry = players.get(playerUUID);
		return entry != null && entry.ctx.channel().isActive();
	}

	public static String getPlayerName(UUID playerUUID) {
		PlayerEntry entry = players.get(playerUUID);
		return entry != null ? entry.username : null;
	}

	public static Map<UUID, String> getOnlinePlayers() {
		Map<UUID, String> result = new ConcurrentHashMap<>();
		for (Map.Entry<UUID, PlayerEntry> entry : players.entrySet()) {
			if (entry.getValue().ctx.channel().isActive()) {
				result.put(entry.getKey(), entry.getValue().username);
			}
		}
		return result;
	}

	public static Channel getPlayerChannel(UUID playerUUID) {
		PlayerEntry entry = players.get(playerUUID);
		return entry != null ? entry.ctx.channel() : null;
	}

	public static UUID getPlayerUUID(String username) {
		return UUID.nameUUIDFromBytes(("OfflinePlayer:" + username).getBytes(StandardCharsets.UTF_8));
	}

	static void handlePlayerJoin(UUID uuid, String username, ChannelHandlerContext ctx, boolean legacy, int pluginMessageId) {
		players.put(uuid, new PlayerEntry(username, ctx, legacy, pluginMessageId));
		synchronized (playerEventListeners) {
			for (int i = 0; i < playerEventListeners.size(); i++) {
				try {
					playerEventListeners.get(i).onJoin(uuid, username);
				} catch (Exception ignored) {
				}
			}
		}
	}

	static void handlePlayerQuit(UUID uuid) {
		PlayerEntry entry = players.remove(uuid);
		if (entry != null) {
			synchronized (playerEventListeners) {
				for (int i = 0; i < playerEventListeners.size(); i++) {
					try {
						playerEventListeners.get(i).onQuit(uuid, entry.username);
					} catch (Exception ignored) {
					}
				}
			}
		}
	}

	static void handleIncomingMessage(UUID playerUUID, String username, String channel, byte[] data) {
		List<PluginMessageListener> listeners = channelListeners.get(channel);
		if (listeners == null || listeners.isEmpty()) return;
		for (int i = 0; i < listeners.size(); i++) {
			try {
				listeners.get(i).onMessage(playerUUID, username, channel, data);
			} catch (Exception ignored) {
			}
		}
	}

	static void handleOutgoingMessage(UUID playerUUID, String username, String channel, byte[] data) {
		List<ServerPluginMessageListener> listeners = serverChannelListeners.get(channel);
		if (listeners == null || listeners.isEmpty()) return;
		for (int i = 0; i < listeners.size(); i++) {
			try {
				listeners.get(i).onServerMessage(playerUUID, username, channel, data);
			} catch (Exception ignored) {
			}
		}
	}

	static boolean hasListeners(String channel) {
		List<PluginMessageListener> listeners = channelListeners.get(channel);
		return listeners != null && !listeners.isEmpty();
	}

	static boolean hasServerListeners(String channel) {
		List<ServerPluginMessageListener> listeners = serverChannelListeners.get(channel);
		return listeners != null && !listeners.isEmpty();
	}
}
