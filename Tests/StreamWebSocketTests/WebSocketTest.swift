
import Foundation
import NIOCore
import NIOFoundationCompat
import NIOPosix
import NIOWebSocket
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

/// Configuration
extension WebSocketIntegrationTests {
    @Test(.timeLimit(.minutes(1)))
    func clientWebSocketConfiguration() async throws {
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
        let url = socketURL(with: port)

        let websocket = WebSocket(configuration: .init(url: url))

        let task = Task {
            var states: [WebSocketState] = []
            for await state in await websocket.states.prefix(6) {
                states.append(state)
                print(state)
            }

            return states
        }

        await websocket.connect()

        #expect(try await waitForState(websocket) { $0 == .connected } == .connected)

        await websocket
            .update(configuration: .init(url: url, pingInterval: .seconds(4)))

        let states = await task.value

        #expect(states == [
            .disconnected,
            .connecting,
            .connected,
            .disconnected,
            .connecting,
            .connected,
        ])
    }
}

/// Failure
extension WebSocketIntegrationTests {
    @Test(.timeLimit(.minutes(1)))
    func clientWebSocketStatesServerRejecting() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let server = WebSocketTestServer(group: group, mode: .failure)

        defer {
            do {
                try await server.stop()
                try await group.shutdownGracefully()
            } catch {
                Issue.record("Unexpected error: \(error)")
            }
        }

        let port = try await server.start()
        let url = socketURL(with: port)

        let websocket = WebSocket(configuration: .init(url: url))

        let task = Task {
            var states: [WebSocketState] = []
            for await state in await websocket.states.prefix(4) {
                states.append(state)
            }

            return states
        }

        await websocket.connect()

        let states = await task.value

        #expect(states[0] == .disconnected)
        #expect(states[1] == .connecting)
        #expect(states[2] == .disconnected)

        switch states[3] {
        case let .reconnecting(attempt: attempt, delay: _):
            #expect(attempt == 1)
        default:
            Issue.record("Incorrect state:\(states[3])")
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func clientWebSocketStatesServerPolicyViolation() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let server = WebSocketTestServer(group: group, mode: .policyViolation)

        defer {
            do {
                try await server.stop()
                try await group.shutdownGracefully()
            } catch {
                Issue.record("Unexpected error: \(error)")
            }
        }

        let port = try await server.start()
        let websocket = WebSocket(configuration: .init(url: socketURL(with: port)))

        let task = Task {
            var states: [WebSocketState] = []
            for await state in await websocket.states.prefix(4) {
                states.append(state)
            }

            return states
        }

        await websocket.connect()

        #expect(try await waitForState(websocket) { $0 == .connected } == .connected)

        try await websocket.send("message")

        let states = await task.value

        #expect(
            states == [.disconnected, .connecting, .connected, .disconnected]
        )
    }

    @Test(.timeLimit(.minutes(1)))
    func clientWebSocketStatesServerNetworkUnavailable() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let server = WebSocketTestServer(group: group, mode: .failure)

        defer {
            do {
                try await server.stop()
                try await group.shutdownGracefully()
            } catch {
                Issue.record("Unexpected error: \(error)")
            }
        }

        let port = try await server.start()
        let url = socketURL(with: port)

        // Inject into the session your networking layer uses
        let sessionConfiguration = URLSessionConfiguration.ephemeral
        sessionConfiguration.protocolClasses = [NotConnectedToInternetURLErrorProtocol.self]

        let websocket = WebSocket(
            configuration: .init(url: url),
            sessionConfiguration: sessionConfiguration
        )

        let task = Task {
            var states: [WebSocketState] = []
            for await state in await websocket.states.prefix(4) {
                states.append(state)
            }

            return states
        }

        await websocket.connect()

        let states = await task.value

        #expect(states[0] == .disconnected)
        #expect(states[1] == .connecting)
        #expect(states[2] == .disconnected)

        switch states[3] {
        case let .reconnecting(attempt: attempt, delay: _):
            #expect(attempt == 1)
        default:
            Issue.record("Incorrect state:\(states[3])")
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

        #expect(try await waitForState(websocket) { $0 == .connected } == .connected)

        do {
            try await server.stop()
            try await group.shutdownGracefully()
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        #expect(await disconnectTask.value == [.disconnected, .connecting, .connected, .disconnected])
    }
}

/// Server Send Messages
extension WebSocketIntegrationTests {
    @Test(.timeLimit(.minutes(1)))
    func clientWebSocketServerSendTextMessages() async throws {
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

        #expect(try await waitForState(websocket) { $0 == .connected } == .connected)

        try await server.send(text: "Message 1")
        try await server.send(text: "Message 2")
        try await server.send(text: "Message 3")

        let messages = await messagesTask.value

        #expect(messages == [WebSocketMessage.text("Message 1"), .text("Message 2"), .text("Message 3")])
    }

    @Test(.timeLimit(.minutes(1)))
    func clientWebSocketServerSendDataMessages() async throws {
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

        #expect(try await waitForState(websocket) { $0 == .connected } == .connected)

        try await server.send(data: Data([1, 2, 3]))
        try await server.send(data: Data([4, 5, 6]))
        try await server.send(data: Data([7, 8, 9]))

        let messages = await messagesTask.value

        #expect(messages == [WebSocketMessage.data(Data([1, 2, 3])), .data(Data([4, 5, 6])), .data(Data([7, 8, 9]))])
    }
}

/// Client Send Messages
extension WebSocketIntegrationTests {
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

            #expect(try await waitForState(websocket) { $0 == .connected } == .connected)

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

            #expect(try await waitForState(websocket) { $0 == .connected } == .connected)

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

            #expect(try await waitForState(websocket) { $0 == .connected } == .connected)

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
                configuration: .init(url: url, pingInterval: .seconds(4))
            )

            await websocket.connect()

            #expect(try await waitForState(websocket) { $0 == .connected } == .connected)

            try await Task.sleep(for: .seconds(14))
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

            await websocket.connect()

            #expect(try await waitForState(websocket) { $0 == .connected } == .connected)

            try await Task.sleep(for: .seconds(10))
            await websocket.disconnect()
        }
    }
}

/// Server close
extension WebSocketIntegrationTests {
    func waitForState(
        _ websocket: WebSocket,
        timeout: Duration = .seconds(10),
        where predicate: (WebSocketState) -> Bool
    ) async throws -> WebSocketState {
        let deadline = ContinuousClock.now + timeout
        while ContinuousClock.now < deadline {
            let state = await websocket.state
            if predicate(state) {
                return state
            }
            try await Task.sleep(for: .milliseconds(50))
        }
        return await websocket.state
    }

    func serverClose(with code: WebSocketErrorCode) async throws -> WebSocketState {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let server = WebSocketTestServer(group: group)

        defer {
            try? await server.stop()
            try? await group.shutdownGracefully()
        }

        let port = try await server.start()
        let websocket = WebSocket(configuration: .init(url: socketURL(with: port)))

        await websocket.connect()

        #expect(try await waitForState(websocket) { $0 == .connected } == .connected)

        try await server.close(code: code)

        let state = try await waitForState(websocket) { $0 != .connected && $0 != .disconnected }

        await websocket.disconnect()

        return state
    }

    @Test(.timeLimit(.minutes(1)))
    func serverClosePolicyViolationFails() async throws {
        let state = try await serverClose(with: .policyViolation)

        #expect(state == .failed(.policyViolationError))
    }

    @Test(.timeLimit(.minutes(1)))
    func serverCloseNormalClosureFails() async throws {
        let state = try await serverClose(with: .normalClosure)

        guard case .failed(.closedByServer(code: .normalClosure, reason: _)) = state else {
            Issue.record("Unexpected state: \(state)")
            return
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func serverCloseGoingAwayReconnects() async throws {
        let state = try await serverClose(with: .goingAway)

        guard case .reconnecting = state else {
            Issue.record("Unexpected state: \(state)")
            return
        }
    }
}
