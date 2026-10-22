//
//  Created by Kurlovich Vitali on 9/26/26.
//

import AsyncAlgorithms
import Foundation
import Network

#if WebsocketLogging
    import Logging

    private let logger: Logger = .init(label: String(describing: WebSocket.self))
#endif

public actor WebSocket {
    public typealias Configuration = WebSocketConfiguration
    public typealias State = WebSocketState

    deinit {
        pathMonitor?.pathUpdateHandler = nil
        pathMonitor?.cancel()

        messagesContinuation.finish()
        stateContinuation.finish()
    }

    public init(configuration: Configuration, sessionConfiguration: URLSessionConfiguration = .default, pingUpdater: (any WebSocketPing)? = nil) {
        self.configuration = configuration
        session = URLSession(configuration: sessionConfiguration, delegate: nil, delegateQueue: nil)

        let (messagesStream, messagesContinuation) = AsyncStream<WebSocketMessage>.makeStream()
        self.messagesContinuation = messagesContinuation
        messagesSharedStream = messagesStream.share()

        let (stateStream, stateContinuation) = AsyncStream<State>.makeStream(bufferingPolicy: .bufferingNewest(2))
        self.stateContinuation = stateContinuation
        stateSharedStream = stateStream.share()

        self.pingUpdater = pingUpdater

        stateContinuation.yield(.disconnected)
    }

    private let session: URLSession

    private var wantsConnection = false
    private var attempt = 0

    private var socketTask: URLSessionWebSocketTask?
    private var reconnectTask: Task<Void, Never>?
    private var receiveTask: Task<Void, Never>?
    private var pingTask: Task<Void, Never>?
    private var networkAvailable = true

    private var pathMonitor: NWPathMonitor?

    private lazy var webSocketDelegate: WebSocketDelegate = .init(webSocket: self)

    private let messagesContinuation: AsyncStream<WebSocketMessage>.Continuation
    private let messagesSharedStream: any AsyncSequence<WebSocketMessage, Never>

    private let stateContinuation: AsyncStream<State>.Continuation
    private let stateSharedStream: any AsyncSequence<State, Never>

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
        return AsyncStream<State>(bufferingPolicy: .bufferingNewest(1)) { continuation in
            let task = Task {

                for await state in stateSharedStream {
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
                for await message in messagesSharedStream {
                    continuation.yield(message)
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

        stopPathMonitor()
    }

    /// Call when the app becomes active again.
    func resume() {
        #if WebsocketLogging
            logger.info("Resume")
        #endif

        guard wantsConnection, socketTask == nil else { return }
        attempt = 0
        open()

        startPathMonitor()
    }
}

public extension WebSocket {
    func send(_ text: String) async throws {
        guard let socketTask, state == .connected else { throw URLError(.notConnectedToInternet) }

        try await socketTask.send(.string(text))

        #if WebsocketLogging
            logger.debug("Send text: \(text)")
        #endif
    }

    func send(_ data: Data) async throws {
        guard let socketTask, state == .connected else { throw URLError(.notConnectedToInternet) }

        try await socketTask.send(.data(data))

        #if WebsocketLogging
            logger.debug("Send data: \(data)")
        #endif
    }

    func sendPing() async throws {
        guard let task = socketTask else { return }

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
        task.delegate = webSocketDelegate
        task.maximumMessageSize = 4 * 1024 * 1024
        socketTask = task
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

        socketTask?.cancel(with: code, reason: nil)

        socketTask = nil
        state = .disconnected
    }

    func receiveLoop(_ task: URLSessionWebSocketTask) async {
        while !Task.isCancelled {
            do {
                let message = try await task.receive()
                switch message {
                case let .string(text):
                    messagesContinuation.yield(.text(text))

                    #if WebsocketLogging
                        logger.debug("Receive text: \(text)")
                    #endif

                case let .data(data):
                    messagesContinuation.yield(.data(data))

                    #if WebsocketLogging
                        logger.debug("Receive data: \(data)")
                    #endif

                @unknown default: return
                }
            } catch {
                if Task.isCancelled == false {
                    #if WebsocketLogging
                        logger.error("receiveLoop failure: \(error)")
                    #endif
                    handleFailure(task, error: failure(for: task, error: error))
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

extension WebSocket {
    // MARK: - Connection state change

    func didOpenConnection(
        _ task: URLSessionWebSocketTask
    ) {
        #if WebsocketLogging
            logger.info("Did open connection")
        #endif

        guard task === socketTask else { return } // ignore stale tasks

        attempt = 0
        state = .connected
        startPing(task)
    }

    func didCloseConnection(_ task: URLSessionWebSocketTask, code: URLSessionWebSocketTask.CloseCode) {
        #if WebsocketLogging
            logger.info("Did close connection with code:\(code)")
        #endif

        handleFailure(task, error: failure(for: task, error: URLError(.networkConnectionLost)))
    }

    func didCompleteWithError(task: URLSessionTask, error: (any Error)?) {
        // Usually arrives after receiveLoop has already handled the failure; then it's ignored as stale.
        guard let task = task as? URLSessionWebSocketTask else { return }

        handleFailure(task, error: failure(for: task, error: error ?? URLError(.networkConnectionLost)))
    }
}

private extension WebSocket {
    // MARK: - Error handlening

    func handleFailure(_ task: URLSessionWebSocketTask, error: WebSocketError) {
        guard task === socketTask else { return } // ignore stale tasks

        #if WebsocketLogging
            logger.error("Handle failure: \(error)")
        #endif

        teardown(code: .goingAway)

        // Server rejected us or ended the session on purpose: don't hammer it.
        if error.isPermanent {
            wantsConnection = false
        }

        guard wantsConnection else {
            state = .failed(error)
            return
        }
        scheduleReconnect()
    }

    func failure(for task: URLSessionWebSocketTask, error: any Error) -> WebSocketError {
        if let response = task.response as? HTTPURLResponse, response.statusCode != 101 {
            return .handshakeFailed(statusCode: response.statusCode)
        }

        switch task.closeCode {
        case .policyViolation:
            return .policyViolationError
        case .invalid:
            break
        default:
            return .closedByServer(code: task.closeCode, reason: task.closeReason)
        }

        if let urlError = error as? URLError {
            return .urlError(urlError.code)
        }
        return .receiveError
    }
}

private extension WebSocketError {
    var isPermanent: Bool {
        switch self {
        case .policyViolationError,
             .handshakeFailed(statusCode: 400 ..< 500),
             .closedByServer(code: .normalClosure, reason: _):
            true
        default:
            false
        }
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
    // MARK: - Network reachability

    func startPathMonitor() {
        #if WebsocketLogging
            defer {
                logger.debug("Start path monitor")
            }
        #endif
        pathMonitor?.pathUpdateHandler = nil
        pathMonitor?.cancel()
        pathMonitor = NWPathMonitor()

        pathMonitor?.pathUpdateHandler = { [weak self] path in
            let available = path.status == .satisfied

            #if WebsocketLogging
                logger.debug("Network availablity changed: \(path.status)")
            #endif

            Task {
                await self?.networkChanged(available: available)
            }
        }
        pathMonitor?.start(queue: DispatchQueue(label: "ws.path-monitor"))
    }

    func stopPathMonitor() {
        pathMonitor?.cancel()
        pathMonitor = nil
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
        pingTask?.cancel()
        pingTask = Task { [weak self, pingInterval] in
            while !Task.isCancelled {
                try? await Task.sleep(for: pingInterval)

                guard let self else { return }

                guard !Task.isCancelled else { return }
                do {
                    if let pingUpdater = self.pingUpdater {
                        do {
                            try await pingUpdater.ping(self)

                            #if WebsocketLogging
                                logger.debug("Send custom ping")
                            #endif

                        } catch {
                            #if WebsocketLogging
                                logger.error("\(error.localizedDescription)")
                            #endif
                            await handleFailure(task, error: .sendPingError)
                            return
                        }

                        continue
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
                    await self.handleFailure(task, error: .sendPingError)
                    return
                }
            }
        }
    }
}
