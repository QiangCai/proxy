package org.apache.carbondata;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class Proxy {

  private static String localHost;
  private static int localPort;
  private static String remoteHost;
  private static int remotePort;

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("Usage: " + Proxy.class.getSimpleName()
          + "<local host> <local port> <remote host> <remote port>");
      return;
    }

    localHost = args[0];
    localPort = Integer.parseInt(args[1]);
    remoteHost = args[2];
    remotePort = Integer.parseInt(args[3]);
    System.err.println(
        "Proxying " + localHost + ":" + localPort + " to " + remoteHost + ":" + remotePort
            + " ...");

    EventLoopGroup bossGroup = new NioEventLoopGroup();
    EventLoopGroup workerGroup = new NioEventLoopGroup(2);
    try {
      ServerBootstrap bootstrap = new ServerBootstrap();
      bootstrap.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_BACKLOG, 100)
          .option(ChannelOption.TCP_NODELAY, true)
          .handler(new LoggingHandler(LogLevel.INFO))
          .childHandler(new ChannelInitializer<Channel>() {

            @Override
            protected void initChannel(Channel ch) throws Exception {
              ch.pipeline().addLast("httpCodec", new HttpServerCodec());
              ch.pipeline().addLast("httpObject", new HttpObjectAggregator(65536));
              ch.pipeline().addLast("serverHandle", new HttpProxyServerHandle());
            }
          });
      ChannelFuture f = bootstrap.bind(localHost, localPort).sync();
      f.channel().closeFuture().sync();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }

  }

  static class HttpProxyServerHandle extends ChannelInboundHandlerAdapter {

    private ChannelFuture cf;

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg)
        throws Exception {
      if (msg instanceof FullHttpRequest) {
        FullHttpRequest request = (FullHttpRequest) msg;
        if ("CONNECT".equalsIgnoreCase(request.method().name())) {
          HttpResponse response =
              new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
          ctx.writeAndFlush(response);
          ctx.pipeline().remove("httpCodec");
          ctx.pipeline().remove("httpObject");
          return;
        }

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(ctx.channel().eventLoop())
            .channel(ctx.channel().getClass())
            .handler(new HttpProxyInitializer(ctx.channel()));

        ChannelFuture cf = bootstrap.connect(remoteHost, remotePort);
        cf.addListener(new ChannelFutureListener() {
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
              future.channel().writeAndFlush(msg);
            } else {
              ctx.channel().close();
            }
          }
        });
      } else {
        if (cf == null) {
          Bootstrap bootstrap = new Bootstrap();
          bootstrap.group(ctx.channel().eventLoop())
              .channel(ctx.channel().getClass())
              .handler(new ChannelInitializer() {

                @Override protected void initChannel(Channel ch) throws Exception {
                  ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                    @Override public void channelRead(ChannelHandlerContext ctx0, Object msg)
                        throws Exception {
                      ctx.channel().writeAndFlush(msg);
                    }
                  });
                }
              });
          cf = bootstrap.connect(remoteHost, remotePort);
          cf.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) throws Exception {
              if (future.isSuccess()) {
                future.channel().writeAndFlush(msg);
              } else {
                ctx.channel().close();
              }
            }
          });
        } else {
          cf.channel().writeAndFlush(msg);
        }
      }
    }

  }

  static class HttpProxyInitializer extends ChannelInitializer {

    private Channel clientChannel;

    public HttpProxyInitializer(Channel clientChannel) {
      this.clientChannel = clientChannel;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
      ch.pipeline().addLast(new HttpClientCodec());
      ch.pipeline().addLast(new HttpObjectAggregator(6553600));
      ch.pipeline().addLast(new HttpProxyClientHandle(clientChannel));
    }
  }

  static class HttpProxyClientHandle extends ChannelInboundHandlerAdapter {

    private Channel clientChannel;

    public HttpProxyClientHandle(Channel clientChannel) {
      this.clientChannel = clientChannel;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      clientChannel.writeAndFlush(msg);
    }
  }

}
