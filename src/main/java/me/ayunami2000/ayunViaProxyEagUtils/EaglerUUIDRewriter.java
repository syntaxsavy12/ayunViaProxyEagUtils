package me.ayunami2000.ayunViaProxyEagUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import net.raphimc.netminecraft.constants.MCPackets;
import net.raphimc.netminecraft.packet.PacketTypes;

import com.viaversion.viaversion.libs.gson.JsonElement;
import com.viaversion.viaversion.libs.gson.JsonObject;
import com.viaversion.viaversion.libs.gson.JsonParser;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class EaglerUUIDRewriter extends ChannelOutboundHandlerAdapter {

	private final Map<UUID, UUID> onlineToOffline = new ConcurrentHashMap<>();
	private final Map<UUID, UUID> offlineToOnline = new ConcurrentHashMap<>();
	private final Map<UUID, String> skinUrls = new ConcurrentHashMap<>();
	private int playerInfoId = -1;
	private int spawnPlayerId = -1;
	private boolean initialized;

	public String getSkinUrl(UUID offlineUUID) {
		return skinUrls.get(offlineUUID);
	}

	public UUID getOnlineUUID(UUID offlineUUID) {
		return offlineToOnline.get(offlineUUID);
	}

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		if (msg instanceof BinaryWebSocketFrame) {
			if (!initialized) {
				initialized = true;
				try {
					EaglercraftHandler handler = (EaglercraftHandler) ctx.pipeline().get("eaglercraft-handler");
					if (handler != null && handler.version != null) {
						int ver = handler.version.getVersion();
						playerInfoId = MCPackets.S2C_PLAYER_INFO.getId(ver);
						spawnPlayerId = MCPackets.S2C_SPAWN_PLAYER.getId(ver);
					}
				} catch (Exception ignored) {
				}
			}

			if (playerInfoId != -1) {
				ByteBuf buf = ((BinaryWebSocketFrame) msg).content();
				if (buf.readableBytes() >= 3) {
					int readerIndex = buf.readerIndex();
					try {
						int packetId = PacketTypes.readVarInt(buf);
						if (packetId == playerInfoId) {
							rewritePlayerInfo(buf);
						} else if (packetId == spawnPlayerId) {
							rewriteSpawnPlayer(buf);
						}
					} catch (Exception ignored) {
					}
					buf.readerIndex(readerIndex);
				}
			}
		}

		super.write(ctx, msg, promise);
	}

	private void rewritePlayerInfo(ByteBuf buf) {
		int action = PacketTypes.readVarInt(buf);
		int numPlayers = PacketTypes.readVarInt(buf);

		for (int i = 0; i < numPlayers; i++) {
			int uuidPos = buf.readerIndex();
			long msb = buf.readLong();
			long lsb = buf.readLong();
			UUID onlineUUID = new UUID(msb, lsb);

			if (action == 0) {
				String name = PacketTypes.readString(buf, 16);
				UUID offlineUUID = UUID.nameUUIDFromBytes(("OfflinePlayer:" + name).getBytes(StandardCharsets.UTF_8));

				onlineToOffline.put(onlineUUID, offlineUUID);
				offlineToOnline.put(offlineUUID, onlineUUID);
				buf.setLong(uuidPos, offlineUUID.getMostSignificantBits());
				buf.setLong(uuidPos + 8, offlineUUID.getLeastSignificantBits());

				int numProperties = PacketTypes.readVarInt(buf);
				for (int j = 0; j < numProperties; j++) {
					String propName = PacketTypes.readString(buf, 32767);
					String propValue = PacketTypes.readString(buf, 32767);
					if (buf.readBoolean()) {
						PacketTypes.readString(buf, 32767);
					}
					if (propName.equals("textures")) {
						try {
							byte[] decoded = Base64.getDecoder().decode(propValue);
							JsonObject json = JsonParser.parseString(new String(decoded, StandardCharsets.UTF_8)).getAsJsonObject();
							JsonObject textures = json.getAsJsonObject("textures");
							if (textures != null) {
								JsonObject skin = textures.getAsJsonObject("SKIN");
								if (skin != null) {
									JsonElement urlEl = skin.get("url");
									if (urlEl != null) {
										skinUrls.put(offlineUUID, urlEl.getAsString());
									}
								}
							}
						} catch (Exception ignored) {
						}
					}
				}

				PacketTypes.readVarInt(buf);
				PacketTypes.readVarInt(buf);
				if (buf.readBoolean()) {
					PacketTypes.readString(buf, 32767);
				}
			} else {
				UUID offlineUUID = onlineToOffline.get(onlineUUID);
				if (offlineUUID != null) {
					buf.setLong(uuidPos, offlineUUID.getMostSignificantBits());
					buf.setLong(uuidPos + 8, offlineUUID.getLeastSignificantBits());
				}

				if (action == 1) {
					PacketTypes.readVarInt(buf);
				} else if (action == 2) {
					PacketTypes.readVarInt(buf);
				} else if (action == 3) {
					if (buf.readBoolean()) {
						PacketTypes.readString(buf, 32767);
					}
				}
			}
		}
	}

	private void rewriteSpawnPlayer(ByteBuf buf) {
		PacketTypes.readVarInt(buf);
		int uuidPos = buf.readerIndex();
		long msb = buf.readLong();
		long lsb = buf.readLong();
		UUID onlineUUID = new UUID(msb, lsb);

		UUID offlineUUID = onlineToOffline.get(onlineUUID);
		if (offlineUUID != null) {
			buf.setLong(uuidPos, offlineUUID.getMostSignificantBits());
			buf.setLong(uuidPos + 8, offlineUUID.getLeastSignificantBits());
		}
	}
}
