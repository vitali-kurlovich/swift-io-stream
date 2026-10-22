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

extension WebSocketTestServer {
    func send(text: String) throws {
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

    func close(code: WebSocketErrorCode) throws {
        guard let channel = serverChannel else {
            throw ChannelError.channelNoExists
        }

        var buffer = channel.allocator.buffer(capacity: 2)
        buffer.write(webSocketErrorCode: code)

        let frame = WebSocketFrame(
            fin: true,
            opcode: .connectionClose,
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
    let socketHandler: ReadSocketHandler

    init(
        group: MultiThreadedEventLoopGroup,
        recieveHandler: @escaping @Sendable (WebSocketOpcode, ByteBuffer) -> Void = {
            _,
            _ in
        }
    ) {
        self.group = group
        socketHandler = ReadSocketHandler(recieveHandler: recieveHandler)
    }

    func start() async throws -> Int {
        let handlers: [any ChannelHandler & Sendable] = [
            socketHandler,
            sendQueue,
        ]

        let upgrader = NIOWebSocketServerUpgrader(
            maxFrameSize: 1 << 14,

            shouldUpgrade: { channel, request in
                guard request.uri == "/socket" else {
                    return channel.eventLoop.makeSucceededFuture(nil)
                }

                return channel.eventLoop.makeSucceededFuture(
                    HTTPHeaders()
                )
            },

            upgradePipelineHandler: { channel, _ in
                channel.pipeline.addHandlers(
                    handlers
                )
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
                        completionHandler: { _ in }
                    )
                )

            }.childChannelOption(
                ChannelOptions.socketOption(.so_reuseaddr),
                value: 1
            )
            .childChannelOption(
                ChannelOptions.socketOption(.tcp_nodelay),
                value: 1
            )

        serverChannel = try await bootstrap.bind(host: "127.0.0.1", port: 0).get()

        // serverChannel = channel

        guard let port = serverChannel?.localAddress?.port else {
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
