//
//  Created by Kurlovich Vitali on 9/26/26.
//

import Foundation
import Network

#if WebsocketLogging
    import Logging

    private let logger: Logger = .init(label: String(describing: WebSocket.self))
#endif

public actor WebSocket {
    public typealias Configuration = WebSocketConfiguration
    public typealias State = WebSocketState
    public typealias Event = WebSocketEvent

    deinit {
        continuation.finish()
        webSocketEventsTask?.cancel()
    }

    public init(configuration: Configuration, sessionConfiguration: URLSessionConfiguration = .default, pingUpdater: (any WebSocketPing)? = nil) {
        self.configuration = configuration
        session = URLSession(configuration: sessionConfiguration, delegate: webSocketDelegate, delegateQueue: nil)

        let (stream, continuation) = AsyncStream<WebSocketMessage>.makeStream()
        self.stream = stream
        self.continuation = continuation

        let (eventsStream, eventsContinuation) = AsyncStream<Event>.makeStream()

        self.eventsStream = eventsStream
        self.eventsContinuation = eventsContinuation

        let (stateStream, stateContinuation) = AsyncStream<State>.makeStream()

        self.stateStream = stateStream
        self.stateContinuation = stateContinuation
        self.pingUpdater = pingUpdater
    }

    private let session: URLSession

    private var wantsConnection = false
    private var attempt = 0

    private var task: URLSessionWebSocketTask?
    private var reconnectTask: Task<Void, Never>?
    private var receiveTask: Task<Void, Never>?
    private var pingTask: Task<Void, Never>?
    private var networkAvailable = true

    private var webSocketEventsTask: Task<Void, Never>?

    private let pathMonitor = NWPathMonitor()

    private let webSocketDelegate = WebSocketDelegate()

    private let stream: AsyncStream<WebSocketMessage>
    private let continuation: AsyncStream<WebSocketMessage>.Continuation

    private let eventsStream: AsyncStream<Event>
    private let eventsContinuation: AsyncStream<Event>.Continuation

    private let stateStream: AsyncStream<State>
    private let stateContinuation: AsyncStream<State>.Continuation

    private let pingUpdater: (any WebSocketPing)?

    public private(set) var state: State = .disconnected {
        didSet {
            invalidateState()
        }
    }

    public var configuration: Configuration {
        didSet {
            if oldValue != configuration {
                invalidateConfiguration()
            }
        }
    }
}

public extension WebSocket {
    func update(configuration: Configuration) {
        self.configuration = configuration
    }
}

public extension WebSocket {
    var states: AsyncStream<State> {
        return AsyncStream<State> { continuation in
            let task = Task {
                continuation.yield(state)

                for await state in stateStream {
                    continuation.yield(state)
                }
                continuation.finish()
            }

            continuation.onTermination = { _ in
                task.cancel()
            }
        }
    }

    var messages: AsyncStream<WebSocketMessage> {
        return AsyncStream<WebSocketMessage> { continuation in
            let task = Task {
                for await message in stream {
                    continuation.yield(message)
                }
                continuation.finish()
            }

            continuation.onTermination = { _ in
                task.cancel()
            }
        }
    }

    var events: AsyncStream<Event> {
        AsyncStream<Event> { continuation in
            let task = Task {
                for await event in eventsStream {
                    continuation.yield(event)
                }
                continuation.finish()
            }

            continuation.onTermination = { _ in
                task.cancel()
            }
        }
    }
}

public extension WebSocket {
    func connect() {
        #if WebsocketLogging
            logger.info("Connect")
        #endif

        subscribeEventsIfNeeds()

        wantsConnection = true
        attempt = 0
        reconnectTask?.cancel()
        open()

        startPathMonitor()
    }

    func disconnect() {
        #if WebsocketLogging
            logger.info("Disconnect")
        #endif

        wantsConnection = false
        reconnectTask?.cancel()
        teardown(code: .normalClosure)
        state = .disconnected

        stopPathMonitor()
    }

    /// Call when the app goes to background. Keeps the "wants connection" intent.
    func suspend() {
        #if WebsocketLogging
            logger.info("Suspend")
        #endif

        guard wantsConnection else { return }
        reconnectTask?.cancel()
        teardown(code: .goingAway)
        state = .disconnected

        stopPathMonitor()
    }

    /// Call when the app becomes active again.
    func resume() {
        #if WebsocketLogging
            logger.info("Resume")
        #endif

        guard wantsConnection, task == nil else { return }
        attempt = 0
        open()

        startPathMonitor()
    }
}

public extension WebSocket {
    func send(_ text: String) async throws {
        guard let task, state == .connected else { throw URLError(.notConnectedToInternet) }
        try await task.send(.string(text))

        #if WebsocketLogging
            logger.debug("Send text: \(text)")
        #endif
    }

    func send(_ data: Data) async throws {
        guard let task, state == .connected else { throw URLError(.notConnectedToInternet) }

        try await task.send(.data(data))

        #if WebsocketLogging
            logger.debug("Send data: \(data)")
        #endif
    }

    func sendPing() async throws {
        guard let task = task else { return }

        try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
            task.sendPing { error in
                if let error {
                    cont.resume(throwing: error)
                } else {
                    cont.resume()
                }
            }
        }

        #if WebsocketLogging
            logger.debug("Send ping")
        #endif
    }
}

private extension WebSocket {
    // MARK: - Connection lifecycle

    func open() {
        teardown(code: .goingAway)
        if attempt == 0 {
            state = .connecting
        }

        var request = URLRequest(url: configuration.url)
        request.timeoutInterval = 15
        for (field, value) in configuration.headers {
            request.setValue(value, forHTTPHeaderField: field)
        }

        let task = session.webSocketTask(with: request)
        task.maximumMessageSize = 4 * 1024 * 1024
        self.task = task
        task.resume()

        receiveTask = Task { [weak self] in
            await self?.receiveLoop(task)
        }

        #if WebsocketLogging
            logger.debug("Start receive loop")
        #endif
    }

    func teardown(code: URLSessionWebSocketTask.CloseCode) {
        #if WebsocketLogging
            logger.debug("Teardown with code: \(code)")
        #endif

        receiveTask?.cancel(); receiveTask = nil
        pingTask?.cancel(); pingTask = nil
        task?.cancel(with: code, reason: nil)
        task = nil
    }

    func receiveLoop(_ task: URLSessionWebSocketTask) async {
        while !Task.isCancelled {
            do {
                let message = try await task.receive()
                switch message {
                case let .string(text):
                    continuation.yield(.text(text))

                    #if WebsocketLogging
                        logger.debug("Receive text: \(text)")
                    #endif

                case let .data(data):
                    continuation.yield(.data(data))

                    #if WebsocketLogging
                        logger.debug("Receive data: \(data)")
                    #endif

                @unknown default: return
                }
            } catch {
                if Task.isCancelled == false {
                    handleFailure(task, error: .receiveError)
                }
                return
            }
        }
    }

    func scheduleReconnect() {
        #if WebsocketLogging
            logger.debug("Schedule reconnect")
        #endif

        reconnectTask?.cancel()
        guard networkAvailable else {
            // The path monitor will call open() when the network returns.
            state = .reconnecting(attempt: attempt, delay: 0)
            return
        }
        attempt += 1
        let base = min(maxBackoff, pow(2, Double(attempt - 1)))
        let delay = base * Double.random(in: 0.8 ... 1.2)
        state = .reconnecting(attempt: attempt, delay: delay)

        reconnectTask = Task { [weak self] in
            try? await Task.sleep(for: .seconds(delay))
            guard !Task.isCancelled else { return }
            await self?.open()
        }
    }
}

private extension WebSocket {
    // MARK: - Connection state change

    func recieve(event: WebSocketDelegate.Events) {
        switch event {
        case let .didOpen(task, protocolName):
            didOpenConnection(task, protocolName: protocolName)
            eventsContinuation.yield(.onConnect(self, task, protocolName))

        case let .didClose(task, closeCode, data):
            didCloseConnection(task, code: closeCode)
            eventsContinuation.yield(.onDisconnet(self, task, closeCode, data))
        }
    }

    func didOpenConnection(
        _ task: URLSessionWebSocketTask,
        protocolName _: String?
    ) {
        #if WebsocketLogging
            logger.info("Did open connection")
        #endif

        guard task === self.task else { return }
        attempt = 0
        state = .connected
        startPing(task)
    }

    func didCloseConnection(_ task: URLSessionWebSocketTask, code: URLSessionWebSocketTask.CloseCode) {
        #if WebsocketLogging
            logger.info("Did close connection with code:\(code)")
        #endif

        guard task === self.task else { return }

        handleFailure(task, error: .urlError(.networkConnectionLost), closeCode: code)
    }
}

private extension WebSocket {
    // MARK: - Error handlening

    func handleFailure(_ task: URLSessionWebSocketTask,
                       error: WebSocketError,
                       closeCode: URLSessionWebSocketTask.CloseCode? = nil)
    {
        #if WebsocketLogging
            logger.error("Handle failure: \(error)")
        #endif
        guard task === self.task else { return } // ignore stale tasks
        teardown(code: .goingAway)

        // Server said "go away" for policy/auth reasons: don't hammer it.
        if closeCode == .policyViolation {
            wantsConnection = false
            state = .failed(.policyViolationError)
            return
        }

        guard wantsConnection else {
            state = .failed(error)
            return
        }
        scheduleReconnect()
    }
}

private extension WebSocket {
    var pingInterval: Duration {
        configuration.pingInterval
    }

    var flushInterval: Duration {
        configuration.flushInterval
    }

    var maxBackoff: TimeInterval {
        configuration.maxBackoff
    }
}

private extension WebSocket {
    func invalidateConfiguration() {
        #if WebsocketLogging
            logger.debug("Update configuration")
        #endif

        guard wantsConnection else { return }
        disconnect()
        connect()
    }

    func invalidateState() {
        stateContinuation.yield(state)

        #if WebsocketLogging
            logger.debug("Update state: \(state)")
        #endif
    }
}

private extension WebSocket {
    func subscribeEventsIfNeeds() {
        guard webSocketEventsTask == nil else { return }

        let events = webSocketDelegate.events

        webSocketEventsTask = Task { [weak self] in
            for await event in events {
                await self?.recieve(event: event)
            }
        }
    }
}

private extension WebSocket {
    // MARK: - Network reachability

    func startPathMonitor() {
        #if WebsocketLogging

            defer {
                logger.debug("Start path monitor")
            }

        #endif
        pathMonitor.pathUpdateHandler = { [weak self] path in
            let available = path.status == .satisfied

            #if WebsocketLogging
                logger.debug("Network availablity changed: \(path.status)")
            #endif

            Task {
                await self?.networkChanged(available: available)
            }
        }
        pathMonitor.start(queue: DispatchQueue(label: "ws.path-monitor"))
    }

    func stopPathMonitor() {
        pathMonitor.cancel()

        #if WebsocketLogging
            logger.debug("Cancel path monitor")
        #endif
    }

    func networkChanged(available: Bool) {
        let wasAvailable = networkAvailable
        networkAvailable = available
        if available, !wasAvailable, wantsConnection, state != .connected {
            reconnectTask?.cancel()
            attempt = 0
            open()
        }
    }
}

private extension WebSocket {
    func startPing(_ task: URLSessionWebSocketTask) {
        #if WebsocketLogging
            logger.debug("Scedule ping")
        #endif

        pingTask?.cancel()
        pingTask = Task { [weak self, pingInterval] in
            while !Task.isCancelled {
                try? await Task.sleep(for: pingInterval)
                guard !Task.isCancelled else { return }
                do {
                    if let self, let pingUpdater = self.pingUpdater {
                        pingUpdater.ping(self)
                        #if WebsocketLogging
                            logger.debug("Send custom ping")
                        #endif

                        return
                    }

                    try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
                        task.sendPing { error in
                            if let error {
                                cont.resume(throwing: error)
                                #if WebsocketLogging
                                    logger.error("Send ping error: \(error.localizedDescription)")
                                #endif
                            } else {
                                cont.resume()
                                #if WebsocketLogging
                                    logger.debug("Send ping")
                                #endif
                            }
                        }
                    }
                } catch {
                    await self?.handleFailure(task, error: .sendPingError)
                    return
                }
            }
        }
    }
}
