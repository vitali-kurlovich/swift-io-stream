//
//  Created by Kurlovich Vitali on 10/21/26.
//

import NIOCore
import NIOHTTP1
import NIOPosix
import NIOWebSocket

enum WebSocketTestServerError: Error {
    case channelError
}

enum ChannelError: Error {
    case channelNoExists
    case inactive
    case outputClosed
}

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
              !pending.isEmpty {
            context.write(
                wrapOutboundOut(pending.removeFirst()),
                promise: nil
            )
        }

        context.flush()
        context.fireChannelWritabilityChanged()
    }
}


private
extension WebSocketTestServer {
    func makeTextFrame(
        _ text: String,
        allocator: ByteBufferAllocator = ByteBufferAllocator()
    ) -> WebSocketFrame {
        var buffer = allocator.buffer(capacity: text.utf8.count)
        buffer.writeString(text)

        return WebSocketFrame(
            fin: true,
            opcode: .text,
            data: buffer
        )
    }
}

final class WritabilityHandler: ChannelInboundHandler, Sendable {
    typealias InboundIn = WebSocketFrame

    func channelWritabilityChanged(
        context: ChannelHandlerContext
    ) {
        print("Writable:", context.channel.isWritable)

        context.fireChannelWritabilityChanged()
    }
}


extension WebSocketTestServer {
    func send(text: String) throws  {
        guard let channel = serverChannel else {
            throw ChannelError.channelNoExists
        }
                
        var buffer = channel.allocator.buffer(capacity: text.utf8.count)
        buffer.writeString(text)

        let frame = WebSocketFrame(
            fin: true,
            opcode: .text,
            data: buffer
        )
       
        channel.eventLoop.execute { [sendQueue] in
            sendQueue.send(
                frame
            )
        }

        
        
    }
}

actor WebSocketTestServer: Sendable {
    private let group: MultiThreadedEventLoopGroup
    private var serverChannel: Channel?

    let sendQueue = WebSocketSendQueue()
    
    init(group: MultiThreadedEventLoopGroup) {
        self.group = group
    }
    

    func start() async throws -> Int {
        
        let handlers: [any ChannelHandler & Sendable] = [
            EchoWebSocketHandler(),
            sendQueue,
            WritabilityHandler()
        ]
        
        let upgrader = NIOWebSocketServerUpgrader(
            maxFrameSize: 1 << 14,

            shouldUpgrade: { channel, requestHead in
                guard requestHead.uri == "/socket" else {
                    return channel.eventLoop.makeSucceededFuture(nil)
                }

                var headers = HTTPHeaders()
                headers.add(name: "x-test-server", value: "true")

                return channel.eventLoop.makeSucceededFuture(headers)
            },

            
            
            upgradePipelineHandler: { channel, _ in
                channel.pipeline.addHandlers(handlers)
                
            }
        )

        let bootstrap = ServerBootstrap(group: group)
            .serverChannelOption(
                ChannelOptions.backlog,
                value: 256
            )
            .serverChannelOption(
                ChannelOptions.socketOption(.so_reuseaddr),
                value: 1
            )
            .childChannelInitializer { channel in
                channel.pipeline.configureHTTPServerPipeline(
                    withServerUpgrade: (
                        upgraders: [upgrader],
                        completionHandler: { _ in
                            // The WebSocket upgrader now owns the pipeline.
                        }
                    )
                )
            }
            .childChannelOption(
                ChannelOptions.socketOption(.so_reuseaddr),
                value: 1
            )

        let channel = try await bootstrap.bind(
            host: "127.0.0.1",
            port: 0
        ).get()

        

        serverChannel = channel
        
        guard let port = channel.localAddress?.port else {
            throw TestServerError.missingPort
        }

        return port
    }

    func stop() async throws {
        guard let serverChannel else {
            return
        }

        try await serverChannel.close().get()
        self.serverChannel = nil
    }
}

enum TestServerError: Error {
    case missingPort
}
