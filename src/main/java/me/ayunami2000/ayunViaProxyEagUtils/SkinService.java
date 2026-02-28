package me.ayunami2000.ayunViaProxyEagUtils;

import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import net.raphimc.netminecraft.constants.MCPackets;
import net.raphimc.netminecraft.packet.PacketTypes;

import javax.imageio.ImageIO;
import java.awt.image.DataBufferByte;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SkinService {
    private final ConcurrentHashMap<UUID, CachedSkin> skinCache;

    public SkinService() {
        this.skinCache = new ConcurrentHashMap<>();
    }

    private static void sendData(final ChannelHandlerContext ctx, final byte[] data) {
        EaglercraftHandler handler = (EaglercraftHandler) ctx.pipeline().get("eaglercraft-handler");
        int eagVer = handler.eaglercraftVersion;
        final ByteBuf bb = ctx.alloc().buffer();
        if (eagVer >= 5) {
            bb.writeByte(0xEE);
            writeV5SkinResponse(bb, data, ctx);
        } else if (eagVer >= 4) {
            PacketTypes.writeVarInt(bb, MCPackets.S2C_CUSTOM_PAYLOAD.getId(handler.version.getVersion()));
            PacketTypes.writeString(bb, "EAG|1.8");
            writeV4SkinResponse(bb, data);
        } else {
            PacketTypes.writeVarInt(bb, MCPackets.S2C_CUSTOM_PAYLOAD.getId(handler.version.getVersion()));
            PacketTypes.writeString(bb, "EAG|Skins-1.8");
            bb.writeBytes(data);
        }
        ctx.writeAndFlush(new BinaryWebSocketFrame(bb));
    }

    private static void writeV4SkinResponse(ByteBuf bb, byte[] v3data) {
        int type = v3data[0] & 0xFF;
        if (type == 4) {
            bb.writeByte(0x02);
            bb.writeBytes(v3data, 1, v3data.length - 1);
        } else if (type == 5) {
            bb.writeByte(0x03);
            bb.writeBytes(v3data, 1, 17);
            bb.writeBytes(SkinPackets.convertV3ToV4Skin(v3data, 18));
        }
    }

    private static void writeV5SkinResponse(ByteBuf bb, byte[] v3data, ChannelHandlerContext ctx) {
        UUID responseUUID = SkinPackets.bytesToUUID(v3data, 1);
        int requestId = 0;
        java.util.concurrent.ConcurrentHashMap<UUID, Integer> requestIdMap = ctx.channel().attr(EaglerXSkinHandler.v5RequestIdMapKey).get();
        if (requestIdMap != null) {
            Integer rid = requestIdMap.remove(responseUUID);
            if (rid != null) requestId = rid;
        }
        int type = v3data[0] & 0xFF;
        if (type == 4) {
            bb.writeByte(0x01);
            PacketTypes.writeVarInt(bb, requestId);
            int presetId = (v3data[17] & 0xFF) << 24 | (v3data[18] & 0xFF) << 16 | (v3data[19] & 0xFF) << 8 | (v3data[20] & 0xFF);
            PacketTypes.writeVarInt(bb, presetId);
        } else if (type == 5) {
            bb.writeByte(0x02);
            PacketTypes.writeVarInt(bb, requestId);
            bb.writeByte(v3data[17]);
            bb.writeBytes(SkinPackets.convertV3ToV4Skin(v3data, 18));
        }
    }

    public void processGetOtherSkin(final UUID searchUUID, final ChannelHandlerContext sender) {
        final CachedSkin cached = this.skinCache.get(searchUUID);
        if (cached != null) {
            sendData(sender, cached.packet);
        } else if (EaglerSkinHandler.skinCollection.containsKey(searchUUID)) {
            final byte[] src = EaglerSkinHandler.skinCollection.get(searchUUID);
            byte[] res = new byte[src.length - 1];
            System.arraycopy(src, 1, res, 0, res.length);
            if (res.length <= 16) {
                int presetId = res[0] & 0xFF;
                InputStream stream = Main.class.getResourceAsStream("/n" + presetId + ".png");
                if (stream != null) {
                    try {
                        res = ((DataBufferByte) ImageIO.read(stream).getRaster().getDataBuffer()).getData();
                        for (int i = 0; i < res.length; i += 4) {
                            final byte tmp = res[i];
                            res[i] = res[i + 1];
                            res[i + 1] = res[i + 2];
                            res[i + 2] = res[i + 3];
                            res[i + 3] = tmp;
                        }
                    } catch (IOException ignored) {}
                }
            }
            if (res.length == 8192) {
                final int[] tmp1 = new int[2048];
                final int[] tmp2 = new int[4096];
                for (int i = 0; i < tmp1.length; ++i) {
                    tmp1[i] = Ints.fromBytes(res[i * 4 + 3], res[i * 4], res[i * 4 + 1], res[i * 4 + 2]);
                }
                SkinConverter.convert64x32to64x64(tmp1, tmp2);
                res = new byte[16384];
                for (int i = 0; i < tmp2.length; ++i) {
                    System.arraycopy(Ints.toByteArray(tmp2[i]), 0, res, i * 4, 4);
                }
            } else if (res.length == 16384) {
                for (int j = 0; j < res.length; j += 4) {
                    final byte tmp3 = res[j + 3];
                    res[j + 3] = res[j + 2];
                    res[j + 2] = res[j + 1];
                    res[j + 1] = res[j];
                    res[j] = tmp3;
                }
            } else {
                sendData(sender, SkinPackets.makePresetResponse(searchUUID));
                return;
            }
            sendData(sender, SkinPackets.makeCustomResponse(searchUUID, 0, res));
        } else {
            String skinUrl = null;
            EaglerUUIDRewriter rewriter = (EaglerUUIDRewriter) sender.pipeline().get("ayun-eag-uuid-rewriter");
            if (rewriter != null) {
                skinUrl = rewriter.getSkinUrl(searchUUID);
                if (skinUrl == null) {
                    UUID onlineUUID = rewriter.getOnlineUUID(searchUUID);
                    if (onlineUUID != null) {
                        skinUrl = "https://crafatar.com/skins/" + onlineUUID.toString();
                    }
                }
            }
            if (skinUrl == null) {
                skinUrl = "https://crafatar.com/skins/" + searchUUID.toString();
            }
            processGetOtherSkin(searchUUID, skinUrl, sender);
        }
    }

    public byte[] fetchSkinPacket(final UUID searchUUID, final String skinURL) {
        // no rate-limit or size limit. it is assumed that this feature is used privately anyway.
        final CachedSkin cached = this.skinCache.get(searchUUID);
        if (cached != null) {
            return cached.packet;
        } else {
            try {
                byte[] res = ((DataBufferByte) ImageIO.read(new URL(skinURL)).getRaster().getDataBuffer()).getData();
                if (res.length == 8192) {
                    final int[] tmp1 = new int[2048];
                    final int[] tmp2 = new int[4096];
                    for (int i = 0; i < tmp1.length; ++i) {
                        tmp1[i] = Ints.fromBytes(res[i * 4 + 3], res[i * 4], res[i * 4 + 1], res[i * 4 + 2]);
                    }
                    SkinConverter.convert64x32to64x64(tmp1, tmp2);
                    res = new byte[16384];
                    for (int i = 0; i < tmp2.length; ++i) {
                        System.arraycopy(Ints.toByteArray(tmp2[i]), 0, res, i * 4, 4);
                    }
                    for (int j = 0; j < res.length; j += 4) {
                        final byte tmp3 = res[j];
                        res[j] = res[j + 1];
                        res[j + 1] = res[j + 2];
                        res[j + 2] = res[j + 3];
                        res[j + 3] = tmp3;
                    }
                }
                byte[] pkt = SkinPackets.makeCustomResponse(searchUUID, 0, res);
                registerEaglercraftPlayer(searchUUID, pkt);
                return pkt;
            } catch (IOException ignored) {
                return null;
            }
        }
    }

    public void processGetOtherSkin(final UUID searchUUID, final String skinURL, final ChannelHandlerContext sender) {
        final byte[] skin = fetchSkinPacket(searchUUID, skinURL);
        if (skin != null) {
            sendData(sender, skin);
        } else {
            sendData(sender, SkinPackets.makePresetResponse(searchUUID));
        }
    }

    public void registerEaglercraftPlayer(final UUID clientUUID, final byte[] generatedPacket) throws IOException {
        this.skinCache.put(clientUUID, new CachedSkin(clientUUID, generatedPacket));
        EaglerSkinHandler.skinCollection.put(clientUUID, newToOldSkin(generatedPacket));
    }

    public static byte[] newToOldSkin(final byte[] packet) throws IOException {
        final byte type = packet[0];
        byte[] res;
        switch (type) {
            case 1:
            case 4: {
                res = new byte[16385];
                res[0] = 1;
                final int o = type == 1 ? 16 : 0;
                final int presetId = packet[17 - o] << 24 | packet[18 - o] << 16 | packet[19 - o] << 8 | packet[20 - o];
                final InputStream stream = Main.class.getResourceAsStream("/" + presetId + ".png");
                if (stream == null) {
                    throw new IOException("Invalid skin preset: " + presetId);
                }
                System.arraycopy(((DataBufferByte) ImageIO.read(stream).getRaster().getDataBuffer()).getData(), 0, res, 1, 16384);
                for (int i = 1; i < 16385; i += 4) {
                    final byte tmp = res[i];
                    res[i] = res[i + 1];
                    res[i + 1] = res[i + 2];
                    res[i + 2] = res[i + 3];
                    res[i + 3] = tmp;
                }
                break;
            }
            case 2:
            case 5: {
                res = new byte[16385];
                res[0] = 1;
                final int o = type == 2 ? 16 : 0;
                final int dataOffset = 18 - o;
                final int dataLen = packet.length - dataOffset;
                if (dataLen == 12288) {
                    byte[] converted = SkinPackets.convertV4ToV3Skin(packet, dataOffset);
                    System.arraycopy(converted, 0, res, 1, 16384);
                } else {
                    System.arraycopy(packet, dataOffset, res, 1, 16384);
                }
                for (int i = 1; i < 16385; i += 4) {
                    final byte tmp = res[i];
                    res[i] = res[i + 1];
                    res[i + 1] = res[i + 2];
                    res[i + 2] = res[i + 3];
                    res[i + 3] = tmp;
                }
                break;
            }
            default: {
                throw new IOException("Invalid skin packet type: " + type);
            }
        }
        return res;
    }

    public void unregisterPlayer(final UUID clientUUID) {
        this.skinCache.remove(clientUUID);
        EaglerSkinHandler.skinCollection.remove(clientUUID);
    }

    private static class CachedSkin {
        protected final UUID uuid;
        protected final byte[] packet;

        protected CachedSkin(final UUID uuid, final byte[] packet) {
            this.uuid = uuid;
            this.packet = packet;
        }
    }

    public static String sanitizeTextureURL(String url) {
        try {
            URI uri = URI.create(url);
            StringBuilder builder = new StringBuilder();
            String scheme = uri.getScheme();
            if(scheme == null) {
                return null;
            }
            String host = uri.getHost();
            if(host == null) {
                return null;
            }
            scheme = scheme.toLowerCase();
            builder.append(scheme).append("://");
            builder.append(host);
            int port = uri.getPort();
            if(port != -1) {
                switch(scheme) {
                    case "http":
                        if(port == 80) {
                            port = -1;
                        }
                        break;
                    case "https":
                        if(port == 443) {
                            port = -1;
                        }
                        break;
                    default:
                        return null;
                }
                if(port != -1) {
                    builder.append(":").append(port);
                }
            }
            String path = uri.getRawPath();
            if(path != null) {
                if(path.contains("//")) {
                    path = String.join("/", path.split("[\\/]+"));
                }
                int len = path.length();
                if(len > 1 && path.charAt(len - 1) == '/') {
                    path = path.substring(0, len - 1);
                }
                builder.append(path);
            }
            return builder.toString();
        }catch(Throwable t) {
            return null;
        }
    }
}
