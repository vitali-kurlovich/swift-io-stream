
import Foundation
import NIOCore
import NIOFoundationCompat
import NIOPosix
import StreamWebSocket
import Testing

extension WebSocketIntegrationTests {
    var urlComponents: URLComponents {
        var components = URLComponents()
        components.scheme = "ws"
        components.host = "127.0.0.1"

        components.path = "/socket"
        return components
    }

    func socketURL(with port: Int) -> URL {
        var urlComponents = urlComponents
        urlComponents.port = port
        return urlComponents.url!
    }
}

final class WebSocketIntegrationTests {
    @Test
    func serverStartsAndAcceptsConnections() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let server = WebSocketTestServer(group: group)

        defer {
            try? await group.shutdownGracefully()
        }

        let port = try await server.start()

        defer {
            Task {
                try? await server.stop()
            }
        }

        #expect(port > 0)
    }

    @Test
    func serverLifecycleIsClean() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let server = WebSocketTestServer(group: group)

        do {
            let port = try await server.start()
            #expect(port > 0)

            try await server.stop()
            try await group.shutdownGracefully()
        } catch {
            try? await server.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }
}

/// States
extension WebSocketIntegrationTests {
    @Test(.timeLimit(.minutes(1)))
    func clientWebSocketStates() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let server = WebSocketTestServer(group: group)

        defer {
            do {
                try await server.stop()
                try await group.shutdownGracefully()
            } catch {
                Issue.record("Unexpected error: \(error)")
            }
        }

        let port = try await server.start()
        #expect(port > 0)
        let url = socketURL(with: port)

        let websocket = WebSocket(configuration: .init(url: url))

        let connectTask = Task {
            var states: [WebSocketState] = []
            for await state in await websocket.states.prefix(3) {
                states.append(state)
            }

            return states
        }

        await websocket.connect()

        #expect(
            await connectTask.value == [.disconnected, .connecting, .connected]
        )

        let disconnectTask = Task {
            var states: [WebSocketState] = []
            for await state in await websocket.states.prefix(1) {
                states.append(state)
            }

            return states
        }

        await websocket.disconnect()

        #expect(await disconnectTask.value == [.disconnected])
    }

    @Test(.timeLimit(.minutes(1)))
    func clientWebSocketStatesServerDisconnect() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let server = WebSocketTestServer(group: group)

        let port = try await server.start()
        let url = socketURL(with: port)

        let websocket = WebSocket(configuration: .init(url: url))

        let disconnectTask = Task {
            var states: [WebSocketState] = []
            for await state in await websocket.states.prefix(4) {
                states.append(state)
            }

            return states
        }

        await websocket.connect()

        try await Task.sleep(for: .seconds(5))

        do {
            try await server.stop()
            try await group.shutdownGracefully()
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        #expect(await disconnectTask.value == [.disconnected, .connecting, .connected, .disconnected])
    }
}

/// Messages
extension WebSocketIntegrationTests {
    @Test(.timeLimit(.minutes(1)))
    func clientWebSocketMessages() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let server = WebSocketTestServer(group: group)

        defer {
            do {
                try await server.stop()
                try await group.shutdownGracefully()
            } catch {
                Issue.record("Unexpected error: \(error)")
            }
        }

        let port = try await server.start()
        #expect(port > 0)
        let url = socketURL(with: port)

        let websocket = WebSocket(configuration: .init(url: url))

        let messagesTask = Task {
            var messages: [WebSocketMessage] = []
            for await message in await websocket.messages.prefix(3) {
                messages.append(message)
            }

            return messages
        }

        await websocket.connect()

        try await Task.sleep(for: .seconds(5))

        #expect(await websocket.state == .connected)

        try await server.send(text: "Message 1")
        try await server.send(text: "Message 2")
        try await server.send(text: "Message 3")

        try await Task.sleep(for: .seconds(5))

        let messages = await messagesTask.value

        #expect(messages.count == 3)

        await websocket.disconnect()
    }

    @Test(.timeLimit(.minutes(1)))
    func clientWebSocketSendTextMessage() async throws {
        try await confirmation(expectedCount: 1) { confirm in
            let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)

            let server = WebSocketTestServer(group: group) { opcode, buffer in
                #expect(opcode == .text)

                let text = String(buffer: buffer)

                #expect(text == "Message")

                confirm.confirm()
            }

            defer {
                do {
                    try await server.stop()
                    try await group.shutdownGracefully()
                } catch {
                    Issue.record("Unexpected error: \(error)")
                }
            }

            let port = try await server.start()
            #expect(port > 0)
            let url = socketURL(with: port)

            let websocket = WebSocket(configuration: .init(url: url))

            await websocket.connect()

            try await Task.sleep(for: .seconds(1))

            #expect(await websocket.state == .connected)

            try await websocket.send("Message")

            try await Task.sleep(for: .seconds(1))
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func clientWebSocketSendDataMessage() async throws {
        try await confirmation(expectedCount: 1) { confirm in
            let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)

            let server = WebSocketTestServer(group: group) { opcode, buffer in
                #expect(opcode == .binary)

                let data = Data(buffer: buffer)

                #expect(data == Data([1, 2, 3, 4, 5]))

                confirm.confirm()
            }

            defer {
                do {
                    try await server.stop()
                    try await group.shutdownGracefully()
                } catch {
                    Issue.record("Unexpected error: \(error)")
                }
            }

            let port = try await server.start()
            #expect(port > 0)
            let url = socketURL(with: port)

            let websocket = WebSocket(configuration: .init(url: url))

            await websocket.connect()

            try await Task.sleep(for: .seconds(1))

            #expect(await websocket.state == .connected)

            let data = Data([1, 2, 3, 4, 5])

            try await websocket.send(data)

            try await Task.sleep(for: .seconds(1))
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func clientWebSocketSendPing() async throws {
        try await confirmation(expectedCount: 1) { confirm in
            let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)

            let server = WebSocketTestServer(group: group) { opcode, _ in
                #expect(opcode == .ping)

                confirm.confirm()
            }

            defer {
                do {
                    try await server.stop()
                    try await group.shutdownGracefully()
                } catch {
                    Issue.record("Unexpected error: \(error)")
                }
            }

            let port = try await server.start()
            #expect(port > 0)
            let url = socketURL(with: port)

            let websocket = WebSocket(configuration: .init(url: url))

            await websocket.connect()

            try await Task.sleep(for: .seconds(1))

            #expect(await websocket.state == .connected)

            try await websocket.sendPing()

            try await Task.sleep(for: .seconds(1))
        }
    }
}

extension WebSocketIntegrationTests {
    @Test(.timeLimit(.minutes(1)))
    func clientWebSocketShedulePing() async throws {
        try await confirmation(expectedCount: 3) { confirm in
            let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)

            let server = WebSocketTestServer(group: group) { opcode, _ in
                #expect(opcode == .ping)

                confirm.confirm()
            }

            defer {
                do {
                    try await server.stop()
                    try await group.shutdownGracefully()
                } catch {
                    Issue.record("Unexpected error: \(error)")
                }
            }

            let port = try await server.start()
            #expect(port > 0)
            let url = socketURL(with: port)

            let websocket = WebSocket(
                configuration: .init(url: url, pingInterval: .seconds(6))
            )

            await websocket.connect()

            try await Task.sleep(for: .seconds(1))

            #expect(await websocket.state == .connected)

            try await Task.sleep(for: .seconds(18))
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func clientWebSocketCustomPing() async throws {
        struct TestCustomPing: WebSocketPing {
            func ping(_ socket: WebSocket) async throws {
                try await socket.send("CustomPing")
            }
        }

        try await confirmation(expectedCount: 3) { confirm in
            let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)

            let server = WebSocketTestServer(group: group) { opcode, buffer in
                #expect(opcode == .text)

                let text = String(buffer: buffer)

                #expect(text == "CustomPing")

                confirm.confirm()
            }

            defer {
                do {
                    try await server.stop()
                    try await group.shutdownGracefully()
                } catch {
                    Issue.record("Unexpected error: \(error)")
                }
            }

            let port = try await server.start()
            #expect(port > 0)
            let url = socketURL(with: port)

            let websocket = WebSocket(
                configuration: .init(url: url, pingInterval: .seconds(3)),
                pingUpdater: TestCustomPing()
            )

            try await Task.sleep(for: .seconds(4))

            await websocket.connect()

            try await Task.sleep(for: .seconds(1))

            #expect(await websocket.state == .connected)

            try await Task.sleep(for: .seconds(9))
            await websocket.disconnect()
        }
    }
}
