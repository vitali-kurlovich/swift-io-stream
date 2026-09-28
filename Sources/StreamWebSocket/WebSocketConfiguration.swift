//
//  Created by Kurlovich Vitali on 9/29/26.
//

import Foundation

public struct WebSocketConfiguration: Equatable, Sendable {
    var url: URL
    var headers: [String: String] = [:]
    var pingInterval: Duration = .seconds(20)
    var flushInterval: Duration = .milliseconds(100)
    var maxBackoff: TimeInterval = 30
}
