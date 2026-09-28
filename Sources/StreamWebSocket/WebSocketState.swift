//
//  Created by Kurlovich Vitali on 9/29/26.
//

import Foundation

public enum WebSocketState: Equatable, Sendable {
    case disconnected
    case connecting
    case connected
    case reconnecting(attempt: Int, delay: TimeInterval)
    case failed(WebSocketError)
}
