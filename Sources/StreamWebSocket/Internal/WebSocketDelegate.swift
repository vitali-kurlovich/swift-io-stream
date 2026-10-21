//
//  Created by Kurlovich Vitali on 9/29/26.
//

import Foundation

#if WebsocketLogging
    import Logging

    private let logger: Logger = .init(label: String(describing: WebSocketDelegate.self))
#endif

final class WebSocketDelegate: NSObject, URLSessionWebSocketDelegate, Sendable {
    weak let webSocket: WebSocket?

    init(webSocket: WebSocket!) {
        self.webSocket = webSocket
    }

    func urlSession(_: URLSession,
                    webSocketTask: URLSessionWebSocketTask,
                    didOpenWithProtocol protocolName: String?)
    {
        guard let webSocket else { return }

        Task {
            await webSocket
                .didOpenConnection(webSocketTask, protocolName: protocolName)
        }
    }

    func urlSession(_: URLSession,
                    webSocketTask: URLSessionWebSocketTask,
                    didCloseWith closeCode: URLSessionWebSocketTask.CloseCode,
                    reason: Data?)
    {
        guard let webSocket else { return }
        Task {
            await webSocket
                .didCloseConnection(webSocketTask, code: closeCode, reason: reason)
        }
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

        guard let webSocket else { return }

        Task {
            await webSocket.didCompleteWithError(task: task, error: error)
            // .didCloseConnection(webSocketTask, code: closeCode, reason: reason)
        }
    }
}
