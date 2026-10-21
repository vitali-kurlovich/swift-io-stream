import NIOCore
import NIOHTTP1
import NIOPosix
import NIOWebSocket

final class WebSocketSendQueue: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = WebSocketFrame
    typealias OutboundOut = WebSocketFrame

    private var context: ChannelHandlerContext?
    private var pending: [WebSocketFrame] = []

    func handlerAdded(context: ChannelHandlerContext) {
        self.context = context
    }

    func send(_ frame: WebSocketFrame) {
        guard let context else {
            return
        }

        if context.channel.isWritable {
            context.writeAndFlush(
                wrapOutboundOut(frame),
                promise: nil
            )
        } else {
            pending.append(frame)
        }
    }

    func channelWritabilityChanged(
        context: ChannelHandlerContext
    ) {
        while context.channel.isWritable,
              !pending.isEmpty
        {
            context.write(
                wrapOutboundOut(pending.removeFirst()),
                promise: nil
            )
        }

        context.flush()
        context.fireChannelWritabilityChanged()
    }
}
