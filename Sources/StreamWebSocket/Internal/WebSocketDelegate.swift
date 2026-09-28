//
//  Created by Kurlovich Vitali on 9/29/26.
//

import Foundation

final class WebSocketDelegate: NSObject, URLSessionWebSocketDelegate, Sendable {
    enum Events {
        case didOpen(URLSessionWebSocketTask, String?)
        case didClose(URLSessionWebSocketTask, URLSessionWebSocketTask.CloseCode, Data?)
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
        let (stream, continuation) = AsyncStream<Events>.makeStream()
        self.stream = stream
        self.continuation = continuation
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
}
