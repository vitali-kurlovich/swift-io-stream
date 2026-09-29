//
//  Created by Kurlovich Vitali on 9/29/26.
//

import Foundation

public struct WebSocketConfiguration: Equatable, Sendable {
    public var url: URL
    public var headers: [String: String]
    public var pingInterval: Duration
    public var flushInterval: Duration
    public var maxBackoff: TimeInterval

    public init(
        url: URL,
        headers: [String: String] = [:],
        pingInterval: Duration = .seconds(20),
        flushInterval: Duration = .milliseconds(100),
        maxBackoff: TimeInterval = 30
    ) {
        self.url = url
        self.headers = headers
        self.pingInterval = pingInterval
        self.flushInterval = flushInterval
        self.maxBackoff = maxBackoff
    }
}
