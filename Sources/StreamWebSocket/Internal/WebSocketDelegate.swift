//
//  Created by Kurlovich Vitali on 9/29/26.
//

import Foundation

final class WebSocketDelegate: NSObject, URLSessionWebSocketDelegate, Sendable {
    enum Events {
        case didOpen(URLSessionWebSocketTask, String?)
        case didClose(URLSessionWebSocketTask, URLSessionWebSocketTask.CloseCode, Data?)
        case didCompleteWithError(URLSessionTask, error: (any Error)?)
    }

    var events: AsyncStream<Events> {
        stream
    }

    private let stream: AsyncStream<Events>
    private let continuation: AsyncStream<Events>.Continuation

    deinit {
        continuation.finish()
    }

    override init() {
        (stream, continuation) = AsyncStream<Events>.makeStream(
            bufferingPolicy: .bufferingNewest(1)
        )
        super.init()
    }

    func urlSession(_: URLSession,
                    webSocketTask: URLSessionWebSocketTask,
                    didOpenWithProtocol protocolName: String?)
    {
        continuation.yield(.didOpen(webSocketTask, protocolName))
    }

    func urlSession(_: URLSession,
                    webSocketTask: URLSessionWebSocketTask,
                    didCloseWith closeCode: URLSessionWebSocketTask.CloseCode,
                    reason: Data?)
    {
        continuation.yield(.didClose(webSocketTask, closeCode, reason))
    }

    func urlSession(
        _: URLSession,
        task: URLSessionTask,
        didCompleteWithError error: (any Error)?
    ) {
        #if WebsocketLogging
            if let error {
                logger
                    .error(
                        "didCompleteWithError: \(error.localizedDescription)"
                    )
            }
        #endif

        continuation.yield(.didCompleteWithError(task, error: error))
    }
}
