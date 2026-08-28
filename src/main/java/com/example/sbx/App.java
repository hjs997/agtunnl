package com.example.sbx;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public class App {

    private static final Map<String, String> DOT_ENV = loadDotEnv();

    private static final String UUID_STR = env("UUID", "e7ac7f64-a99a-4e0c-b3b7-014ed3100009");
    private static final int PORT = envInt("PORT", 12345);
    private static final String WS_PATH = env("WS_PATH", "/ws?ed=2560");

    public static void main(String[] args) throws Exception {
        Logger.getLogger("io.netty").setLevel(Level.OFF);
        parseUuid(UUID_STR);

        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new HttpServerCodec());
                            pipeline.addLast(new HttpObjectAggregator(65536));
                            pipeline.addLast(new HttpFallbackHandler(WS_PATH));
                            pipeline.addLast(new WebSocketServerProtocolHandler(WS_PATH, null, true, 1048576));
                            pipeline.addLast(new VlessFrameHandler(UUID_STR));
                        }
                    });
            bootstrap.bind(PORT).sync().channel().closeFuture().sync();
        } finally {
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }
    }

    private static final class HttpFallbackHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        private static final String DECOY_HTML =
                "<!DOCTYPE html><html><head><title>404 Not Found</title></head>"
                        + "<body><h1>Not Found</h1>"
                        + "<p>The requested URL was not found on this server.</p></body></html>";

        private final String path;

        HttpFallbackHandler(String path) {
            this.path = path;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
            String uri = request.uri();
            int queryIndex = uri.indexOf('?');
            String pathOnly = queryIndex >= 0 ? uri.substring(0, queryIndex) : uri;
            boolean vlessUpgrade = HttpMethod.GET.equals(request.method())
                    && path.equals(pathOnly)
                    && request.headers().containsValue(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE, true)
                    && request.headers().contains(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET, true);
            if (vlessUpgrade) {
                if (queryIndex >= 0) {
                    request.setUri(pathOnly);
                }
                ctx.fireChannelRead(request.retain());
                return;
            }

            byte[] body = DECOY_HTML.getBytes(StandardCharsets.UTF_8);
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, Unpooled.wrappedBuffer(body));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=utf-8");
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body.length);
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }

    private static final class VlessFrameHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

        private final byte[] expectedUuid;
        private final ByteBuf headerBuf = Unpooled.buffer(320);
        private final Deque<ByteBuf> pending = new ArrayDeque<>();

        private Channel targetChannel;
        private boolean headerParsed;
        private boolean connecting;

        VlessFrameHandler(String uuid) {
            this.expectedUuid = parseUuid(uuid);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
            if (!(frame instanceof BinaryWebSocketFrame)) {
                return;
            }
            ByteBuf content = frame.content();
            if (!headerParsed) {
                headerBuf.writeBytes(content);
                tryParseHeader(ctx);
            } else if (connecting) {
                pending.addLast(content.retain());
            } else if (targetChannel != null && targetChannel.isActive()) {
                targetChannel.writeAndFlush(content.retain());
            }
        }

        private void tryParseHeader(ChannelHandlerContext ctx) {
            ByteBuf buf = headerBuf;
            if (buf.readableBytes() < 19) {
                return;
        }
        buf.markReaderIndex();
        int version = buf.readUnsignedByte();
        if (version != 0) {
            ctx.close();
            return;
        }
        byte[] uuid = new byte[16];
            buf.readBytes(uuid);
            int addonsLen = buf.readUnsignedShort();
            if (buf.readableBytes() < addonsLen + 4) {
                buf.resetReaderIndex();
                return;
            }
            buf.skipBytes(addonsLen);
            int command = buf.readUnsignedByte();
            int port = buf.readUnsignedShort();
            int addrType = buf.readUnsignedByte();

            String host;
            switch (addrType) {
                case 1: // IPv4
                    if (buf.readableBytes() < 4) {
                        buf.resetReaderIndex();
                        return;
                    }
                    host = inet4(buf.readInt());
                    break;
                case 2: { // domain
                    if (buf.readableBytes() < 1) {
                        buf.resetReaderIndex();
                        return;
                    }
                    int len = buf.readUnsignedByte();
                    if (buf.readableBytes() < len) {
                        buf.resetReaderIndex();
                        return;
                    }
                    byte[] name = new byte[len];
                    buf.readBytes(name);
                    host = new String(name, StandardCharsets.US_ASCII);
                    break;
                }
                case 3: // IPv6
                    if (buf.readableBytes() < 16) {
                        buf.resetReaderIndex();
                        return;
                    }
                    byte[] ip6 = new byte[16];
                    buf.readBytes(ip6);
                    host = inet6(ip6);
                    break;
                default:
                    ctx.close();
                    return;
            }

            if (!MessageDigest.isEqual(uuid, expectedUuid)) {
                ctx.close();
                return;
            }
            if (command != 1 || port <= 0 || port > 65535 || host.isEmpty()) {
                ctx.close();
                return;
            }

            headerParsed = true;
            connecting = true;
            ByteBuf initial = buf.readRetainedSlice(buf.readableBytes());
            headerBuf.release();
            connectTo(ctx.channel(), host, port, initial);
        }

        private void connectTo(Channel wsChannel, String host, int port, ByteBuf initial) {
            Bootstrap bootstrap = new Bootstrap()
                    .group(wsChannel.eventLoop())
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new TargetRelayHandler(wsChannel));
            ChannelFuture future = bootstrap.connect(InetSocketAddress.createUnresolved(host, port));
            future.addListener((ChannelFutureListener) f -> {
                if (f.isSuccess() && wsChannel.isActive()) {
                    targetChannel = f.channel();
                    targetChannel.writeAndFlush(initial);
                    while (!pending.isEmpty()) {
                        targetChannel.writeAndFlush(pending.poll());
                    }
                    connecting = false;
                } else {
                    initial.release();
                    while (!pending.isEmpty()) {
                        pending.poll().release();
                    }
                    wsChannel.close();
                }
            });
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            if (targetChannel != null) {
                targetChannel.config().setAutoRead(ctx.channel().isWritable());
            }
            super.channelWritabilityChanged(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (targetChannel != null) {
                targetChannel.close();
            }
            super.channelInactive(ctx);
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
            if (headerBuf.refCnt() > 0) {
                headerBuf.release();
            }
            while (!pending.isEmpty()) {
                pending.poll().release();
            }
            super.handlerRemoved(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (targetChannel != null) {
                targetChannel.close();
            }
            ctx.close();
        }
    }

    private static final class TargetRelayHandler extends SimpleChannelInboundHandler<ByteBuf> {

        private final Channel wsChannel;

        TargetRelayHandler(Channel wsChannel) {
            this.wsChannel = wsChannel;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
            if (wsChannel.isActive()) {
                wsChannel.writeAndFlush(new BinaryWebSocketFrame(msg.retain()));
            } else {
                ctx.close();
            }
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            if (wsChannel.isActive()) {
                wsChannel.config().setAutoRead(ctx.channel().isWritable());
            }
            super.channelWritabilityChanged(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (wsChannel.isActive()) {
                wsChannel.close();
            }
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
            if (wsChannel.isActive()) {
                wsChannel.close();
            }
        }
    }

    private static String inet4(int ip) {
        return (ip >>> 24 & 0xff) + "." + (ip >>> 16 & 0xff) + "." + (ip >>> 8 & 0xff) + "." + (ip & 0xff);
    }

    private static String inet6(byte[] bytes) {
        try {
            return InetAddress.getByAddress(bytes).getHostAddress();
        } catch (Exception e) {
            return "";
        }
    }

    private static byte[] parseUuid(String uuid) {
        String hex = uuid.replace("-", "");
        if (hex.length() != 32) {
            throw new IllegalArgumentException("UUID must be a valid UUID");
        }
        byte[] out = new byte[16];
        for (int i = 0; i < 16; i++) {
            out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    private static String env(String name, String fallback) {
        String value = DOT_ENV.get(name);
        if (value == null) value = System.getenv(name);
        return value == null || value.isEmpty() ? fallback : value;
    }

    private static int envInt(String name, int fallback) {
        try {
            return Integer.parseInt(env(name, String.valueOf(fallback)));
        } catch (Exception e) {
            return fallback;
        }
    }

    private static Map<String, String> loadDotEnv() {
        Map<String, String> values = new LinkedHashMap<>();
        Path envPath = Path.of(".env").toAbsolutePath().normalize();
        if (!Files.exists(envPath)) return values;
        try {
            for (String line : Files.readAllLines(envPath, StandardCharsets.UTF_8)) {
                parseDotEnvLine(line).ifPresent(entry -> values.put(entry.getKey(), entry.getValue()));
            }
        } catch (IOException e) {
            // silent
        }
        return values;
    }

    private static Optional<Map.Entry<String, String>> parseDotEnvLine(String line) {
        String trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("#")) return Optional.empty();
        if (trimmed.startsWith("export ")) trimmed = trimmed.substring("export ".length()).trim();
        int equals = trimmed.indexOf('=');
        if (equals <= 0) return Optional.empty();
        String key = trimmed.substring(0, equals).trim();
        if (key.isEmpty()) return Optional.empty();
        String value = trimmed.substring(equals + 1).trim();
        return Optional.of(Map.entry(key, parseDotEnvValue(value)));
    }

    private static String parseDotEnvValue(String value) {
        if (value.length() >= 2) {
            char quote = value.charAt(0);
            if ((quote == '"' || quote == '\'') && value.charAt(value.length() - 1) == quote) {
                value = value.substring(1, value.length() - 1);
                return quote == '"' ? unescapeDotEnvValue(value) : value;
            }
        }
        return stripInlineComment(value).trim();
    }

    private static String stripInlineComment(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) == '#' && (i == 0 || Character.isWhitespace(value.charAt(i - 1)))) {
                return value.substring(0, i);
            }
        }
        return value;
    }

    private static String unescapeDotEnvValue(String value) {
        StringBuilder out = new StringBuilder();
        boolean escaped = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (escaped) {
                switch (c) {
                    case 'n': out.append('\n'); break;
                    case 'r': out.append('\r'); break;
                    case 't': out.append('\t'); break;
                    default: out.append(c);
                }
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }
}
