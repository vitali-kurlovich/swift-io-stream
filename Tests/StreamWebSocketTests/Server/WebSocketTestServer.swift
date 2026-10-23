//
//  Created by Kurlovich Vitali on 10/21/26.
//

import Foundation
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

    func send(data: Data) throws {
        guard let channel = serverChannel else {
            throw ChannelError.channelNoExists
        }

        var buffer = channel.allocator.buffer(capacity: data.count)
        buffer.writeBytes(data)

        let frame = WebSocketFrame(
            fin: true,
            opcode: .binary,
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

enum WebSocketTestServerMode: Sendable {
    case `default`
    case failure
    case policyViolation
}

actor WebSocketTestServer: Sendable {
    private let group: MultiThreadedEventLoopGroup
    private var serverChannel: Channel?

    let sendQueue = WebSocketSendQueue()
    let socketHandler: ReadSocketHandler

    let mode: WebSocketTestServerMode

    init(
        group: MultiThreadedEventLoopGroup,
        mode: WebSocketTestServerMode = .default,
        recieveHandler: @escaping @Sendable (WebSocketOpcode, ByteBuffer) -> Void = {
            _,
            _ in
        }
    ) {
        self.group = group
        self.mode = mode
        socketHandler = ReadSocketHandler(recieveHandler: recieveHandler)
    }

    func start() async throws -> Int {
        let handlers: [any ChannelHandler & Sendable]

        switch mode {
        case .default, .failure:
            handlers = [
                socketHandler,
                sendQueue,
            ]
        case .policyViolation:
            handlers = [
                PolicyViolationHandler(),
            ]
        }

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

        struct ConnectionRejected: Error {}

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
                switch self.mode {
                case .failure:
                    return channel.eventLoop.makeFailedFuture(ConnectionRejected())

                case .default, .policyViolation:
                    return channel.pipeline.configureHTTPServerPipeline(
                        withServerUpgrade: (
                            upgraders: [upgrader],
                            completionHandler: { _ in }
                        )
                    )
                }

            }.childChannelOption(
                ChannelOptions.socketOption(.so_reuseaddr),
                value: 1
            )
            .childChannelOption(
                ChannelOptions.socketOption(.tcp_nodelay),
                value: 1
            )

        serverChannel = try await bootstrap.bind(host: "127.0.0.1", port: 0).get()

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

    /// Starts the close handshake: sends a close frame with `code` and an optional
    /// reason, then stops sending. The connection fully closes when the client replies.
    private func close(context: ChannelHandlerContext, code: WebSocketErrorCode, reason: String? = nil) {
        var data = context.channel.allocator.buffer(capacity: 2)
        data.write(webSocketErrorCode: code)
        if let reason {
            // A close frame payload is at most 125 bytes: 2 for the code + 123 for the reason.
            data.writeString(String(decoding: reason.utf8.prefix(123), as: UTF8.self))
        }

        let frame = WebSocketFrame(fin: true, opcode: .connectionClose, data: data)

        context.eventLoop.execute { [sendQueue] in
            sendQueue.send(
                frame
            )
        }

        // wrapOut

//        context.writeAndFlush(wrapOutboundOut(frame)).whenComplete { _ in
//            context.close(mode: .output, promise: nil)
//        }
    }
}

enum TestServerError: Error {
    case missingPort
}
