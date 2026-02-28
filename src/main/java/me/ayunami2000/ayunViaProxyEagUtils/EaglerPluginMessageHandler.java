package me.ayunami2000.ayunViaProxyEagUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import net.raphimc.netminecraft.constants.MCPackets;
import net.raphimc.netminecraft.packet.PacketTypes;
import net.raphimc.vialegacy.protocol.release.r1_6_4tor1_7_2_5.types.Types1_6_4;
import net.raphimc.viaproxy.proxy.util.ExceptionUtil;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class EaglerPluginMessageHandler extends ChannelDuplexHandler {

	private String user;
	private UUID uuid;
	private final boolean legacy;
	private int pluginMessageId = -1;
	private int s2cPluginMessageId = -1;
	private boolean registered;

	public EaglerPluginMessageHandler(boolean legacy) {
		this.legacy = legacy;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		ExceptionUtil.handleNettyException(ctx, cause, null, true);
	}

	@Override
	public void channelRead(final ChannelHandlerContext ctx, final Object obj) throws Exception {
		if (((EaglercraftHandler) ctx.pipeline().get("eaglercraft-handler")).state != EaglercraftHandler.State.LOGIN_COMPLETE) {
			super.channelRead(ctx, obj);
			return;
		}

		if (this.user == null) {
			this.user = ((EaglercraftHandler) ctx.pipeline().get("eaglercraft-handler")).username;
			this.uuid = UUID.nameUUIDFromBytes(("OfflinePlayer:" + this.user).getBytes(StandardCharsets.UTF_8));
		}

		if (!registered) {
			registered = true;
			EaglercraftHandler eagHandler = (EaglercraftHandler) ctx.pipeline().get("eaglercraft-handler");
			if (!legacy) {
				this.pluginMessageId = eagHandler.pluginMessageId;
			}
			this.s2cPluginMessageId = legacy ? -1 : MCPackets.S2C_CUSTOM_PAYLOAD.getId(eagHandler.version.getVersion());
			EaglerPluginMessageAPI.handlePlayerJoin(uuid, user, ctx, legacy, s2cPluginMessageId);
		}

		if (obj instanceof BinaryWebSocketFrame) {
			final ByteBuf bb = ((BinaryWebSocketFrame) obj).content();
			bb.markReaderIndex();
			try {
				if (legacy) {
					if (bb.readableBytes() >= 3 && bb.readByte() == -6) {
						String tag = Types1_6_4.STRING.read(bb);
						if (EaglerPluginMessageAPI.hasListeners(tag)) {
							byte[] msg = new byte[bb.readShort()];
							bb.readBytes(msg);
							EaglerPluginMessageAPI.handleIncomingMessage(uuid, user, tag, msg);
						}
					}
				} else {
					if (this.pluginMessageId <= 0) {
						this.pluginMessageId = ((EaglercraftHandler) ctx.pipeline().get("eaglercraft-handler")).pluginMessageId;
					}
					int pktId = PacketTypes.readVarInt(bb);
					if (pktId == this.pluginMessageId) {
						String channel = PacketTypes.readString(bb, 32767);
						if (EaglerPluginMessageAPI.hasListeners(channel)) {
							byte[] data = new byte[bb.readableBytes()];
							bb.readBytes(data);
							EaglerPluginMessageAPI.handleIncomingMessage(uuid, user, channel, data);
						}
					}
				}
			} catch (Exception ignored) {
			}
			bb.resetReaderIndex();
		}
		super.channelRead(ctx, obj);
	}

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		if (uuid != null && msg instanceof BinaryWebSocketFrame) {
			ByteBuf bb = ((BinaryWebSocketFrame) msg).content();
			bb.markReaderIndex();
			try {
				if (legacy) {
					if (bb.readableBytes() >= 3 && bb.readByte() == -6) {
						String tag = Types1_6_4.STRING.read(bb);
						if (EaglerPluginMessageAPI.hasServerListeners(tag)) {
							byte[] data = new byte[bb.readShort()];
							bb.readBytes(data);
							EaglerPluginMessageAPI.handleOutgoingMessage(uuid, user, tag, data);
						}
					}
				} else if (s2cPluginMessageId >= 0) {
					int pktId = PacketTypes.readVarInt(bb);
					if (pktId == s2cPluginMessageId) {
						String channel = PacketTypes.readString(bb, 32767);
						if (EaglerPluginMessageAPI.hasServerListeners(channel)) {
							byte[] data = new byte[bb.readableBytes()];
							bb.readBytes(data);
							EaglerPluginMessageAPI.handleOutgoingMessage(uuid, user, channel, data);
						}
					}
				}
			} catch (Exception ignored) {
			}
			bb.resetReaderIndex();
		}
		super.write(ctx, msg, promise);
	}

	@Override
	public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
		super.channelInactive(ctx);
		if (uuid != null) {
			EaglerPluginMessageAPI.handlePlayerQuit(uuid);
		}
	}
}
