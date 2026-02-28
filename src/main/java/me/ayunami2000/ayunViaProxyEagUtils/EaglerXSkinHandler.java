package me.ayunami2000.ayunViaProxyEagUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.AttributeKey;
import net.raphimc.netminecraft.packet.PacketTypes;
import net.raphimc.viaproxy.proxy.util.ExceptionUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class EaglerXSkinHandler extends ChannelInboundHandlerAdapter {
    public static final AttributeKey<ConcurrentHashMap<UUID, Integer>> v5RequestIdMapKey = AttributeKey.newInstance("eag-v5-skin-request-ids");
    private final ConcurrentHashMap<String, byte[]> profileData;
    public static SkinService skinService;
    private String user;
    private int pluginMessageId;

    public EaglerXSkinHandler() {
        this.profileData = new ConcurrentHashMap<>();
        this.pluginMessageId = -1;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ExceptionUtil.handleNettyException(ctx, cause, null, true);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object obj) throws Exception {
        final EaglercraftHandler.State state = ((EaglercraftHandler) ctx.pipeline().get("eaglercraft-handler")).state;
        if (state == EaglercraftHandler.State.LOGIN && obj instanceof BinaryWebSocketFrame) {
            final ByteBuf bb = ((BinaryWebSocketFrame) obj).content();
            if (bb.readUnsignedByte() == 7) {
                if (this.profileData.size() > 12) {
                    ctx.close();
                    bb.release();
                    return;
                }
                final EaglercraftHandler handler = (EaglercraftHandler) ctx.pipeline().get("eaglercraft-handler");
                if (handler.eaglercraftVersion >= 4) {
                    int count = bb.readUnsignedByte();
                    for (int k = 0; k < count; k++) {
                        int strlen = bb.readUnsignedByte();
                        final String dataType = bb.readCharSequence(strlen, StandardCharsets.US_ASCII).toString();
                        strlen = bb.readUnsignedShort();
                        final byte[] readData = new byte[strlen];
                        bb.readBytes(readData);
                        if (!this.profileData.containsKey(dataType)) {
                            this.profileData.put(dataType, readData);
                        }
                    }
                } else {
                    int strlen = bb.readUnsignedByte();
                    final String dataType = bb.readCharSequence(strlen, StandardCharsets.US_ASCII).toString();
                    strlen = bb.readUnsignedShort();
                    final byte[] readData = new byte[strlen];
                    bb.readBytes(readData);
                    if (bb.isReadable()) {
                        ctx.close();
                        bb.release();
                        return;
                    }
                    if (this.profileData.containsKey(dataType)) {
                        ctx.close();
                        bb.release();
                        return;
                    }
                    this.profileData.put(dataType, readData);
                }
            }
            bb.resetReaderIndex();
        }
        if (state != EaglercraftHandler.State.LOGIN_COMPLETE) {
            super.channelRead(ctx, obj);
            return;
        }
        if (this.user == null) {
            this.user = ((EaglercraftHandler) ctx.pipeline().get("eaglercraft-handler")).username;
            if (FunnyConfig.eaglerSkins) {
                final UUID clientUUID = UUID.nameUUIDFromBytes(("OfflinePlayer:" + this.user).getBytes(StandardCharsets.UTF_8));
                byte[] skinData = this.profileData.get("skin_v2");
                if (skinData == null) {
                    skinData = this.profileData.get("skin_v1");
                }
                if (skinData != null) {
                    try {
                        SkinPackets.registerEaglerPlayer(clientUUID, skinData, EaglerXSkinHandler.skinService);
                    } catch (Throwable ex) {
                        SkinPackets.registerEaglerPlayerFallback(clientUUID, EaglerXSkinHandler.skinService);
                    }
                } else {
                    SkinPackets.registerEaglerPlayerFallback(clientUUID, EaglerXSkinHandler.skinService);
                }
            }
        }
        if (this.pluginMessageId <= 0) {
            this.pluginMessageId = ((EaglercraftHandler) ctx.pipeline().get("eaglercraft-handler")).pluginMessageId;
        }
        if (obj instanceof BinaryWebSocketFrame) {
            final ByteBuf bb = ((BinaryWebSocketFrame) obj).content();
            if (bb.readableBytes() >= 2 && bb.getByte(bb.readerIndex()) == (byte) 0xEE) {
                EaglercraftHandler eagHandler = (EaglercraftHandler) ctx.pipeline().get("eaglercraft-handler");
                if (eagHandler.eaglercraftVersion >= 5) {
                    try {
                        handleV5EaglerFrame(bb, ctx);
                    } catch (Exception ignored) {
                    }
                    bb.release();
                    return;
                }
            }
            try {
                int packetId = PacketTypes.readVarInt(bb);
                if (packetId == this.pluginMessageId) {
                    String channel = PacketTypes.readString(bb, 32767);
                    if (channel.equals("EAG|Skins-1.8")) {
                        final byte[] data = new byte[bb.readableBytes()];
                        bb.readBytes(data);
                        SkinPackets.processPacket(data, ctx, EaglerXSkinHandler.skinService);
                        bb.release();
                        return;
                    } else if (channel.equals("EAG|1.8") && bb.isReadable()) {
                        int subPacketId = bb.getUnsignedByte(bb.readerIndex());
                        if (subPacketId == 0x01 || subPacketId == 0x04) {
                            byte[] data = new byte[bb.readableBytes()];
                            bb.readBytes(data);
                            if (data[0] == 0x01) data[0] = 0x03;
                            else if (data[0] == 0x04) data[0] = 0x06;
                            SkinPackets.processPacket(data, ctx, EaglerXSkinHandler.skinService);
                            bb.release();
                            return;
                        } else if (subPacketId == 0xFF) {
                            bb.readByte();
                            int batchCount = PacketTypes.readVarInt(bb);
                            for (int i = 0; i < batchCount; i++) {
                                int len = PacketTypes.readVarInt(bb);
                                int startIdx = bb.readerIndex();
                                int innerPktId = bb.getUnsignedByte(bb.readerIndex());
                                if (innerPktId == 0x01 || innerPktId == 0x04) {
                                    byte[] innerData = new byte[len];
                                    bb.readBytes(innerData);
                                    if (innerData[0] == 0x01) innerData[0] = 0x03;
                                    else if (innerData[0] == 0x04) innerData[0] = 0x06;
                                    SkinPackets.processPacket(innerData, ctx, EaglerXSkinHandler.skinService);
                                } else {
                                    bb.readerIndex(startIdx + len);
                                }
                            }
                            bb.release();
                            return;
                        }
                    }
                }
            } catch (Exception ignored) {
            }
            bb.resetReaderIndex();
        }
        super.channelRead(ctx, obj);
    }

    private void handleV5EaglerFrame(ByteBuf bb, ChannelHandlerContext ctx) throws Exception {
        bb.readByte();
        if (!bb.isReadable()) return;
        if (bb.getByte(bb.readerIndex()) == (byte) 0xFF) {
            bb.readByte();
            int count = PacketTypes.readVarInt(bb);
            for (int i = 0; i < count; i++) {
                int len = PacketTypes.readVarInt(bb);
                byte[] pktData = new byte[len];
                bb.readBytes(pktData);
                int pktId = pktData[0] & 0xFF;
                if (pktId == 0x01 || pktId == 0x03 || pktId == 0x04) {
                    processV5SkinPacket(pktData, ctx);
                }
            }
        } else {
            byte[] pktData = new byte[bb.readableBytes()];
            bb.readBytes(pktData);
            int pktId = pktData[0] & 0xFF;
            if (pktId == 0x01 || pktId == 0x03 || pktId == 0x04) {
                processV5SkinPacket(pktData, ctx);
            }
        }
    }

    private void processV5SkinPacket(byte[] data, ChannelHandlerContext ctx) throws IOException {
        ByteBuf buf = ctx.alloc().buffer(data.length);
        buf.writeBytes(data);
        try {
            int subPacketId = buf.readUnsignedByte();
            int requestId = PacketTypes.readVarInt(buf);
            ConcurrentHashMap<UUID, Integer> requestIdMap = ctx.channel().attr(v5RequestIdMapKey).get();
            if (requestIdMap == null) {
                requestIdMap = new ConcurrentHashMap<>();
                ctx.channel().attr(v5RequestIdMapKey).set(requestIdMap);
            }
            if (subPacketId == 0x01 || subPacketId == 0x03) {
                byte[] v3data = new byte[17];
                v3data[0] = 0x03;
                buf.readBytes(v3data, 1, 16);
                UUID uuid = SkinPackets.bytesToUUID(v3data, 1);
                requestIdMap.put(uuid, requestId);
                SkinPackets.processPacket(v3data, ctx, EaglerXSkinHandler.skinService);
            } else if (subPacketId == 0x04) {
                int urlLen = buf.readUnsignedShort();
                byte[] urlBytes = new byte[urlLen];
                buf.readBytes(urlBytes);
                String urlStr = SkinService.sanitizeTextureURL(new String(urlBytes, StandardCharsets.US_ASCII));
                if (urlStr == null) return;
                UUID genUUID = SkinPackets.createEaglerURLSkinUUID(urlStr);
                requestIdMap.put(genUUID, requestId);
                byte[] v3data = new byte[19 + urlLen];
                v3data[0] = 0x06;
                SkinPackets.UUIDToBytes(genUUID, v3data, 1);
                v3data[17] = (byte) (urlLen >> 8);
                v3data[18] = (byte) (urlLen & 0xFF);
                System.arraycopy(urlBytes, 0, v3data, 19, urlLen);
                SkinPackets.processPacket(v3data, ctx, EaglerXSkinHandler.skinService);
            }
        } finally {
            buf.release();
        }
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        if (this.user != null) {
            EaglerXSkinHandler.skinService.unregisterPlayer(UUID.nameUUIDFromBytes(("OfflinePlayer:" + this.user).getBytes(StandardCharsets.UTF_8)));
        }
    }
}
