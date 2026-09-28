//
//  Created by Kurlovich Vitali on 9/29/26.
//

import Foundation

public enum WebSocketError: Error, Equatable, Sendable {
    case sendPingError
    case receiveError
    case urlError(URLError.Code)
    case policyViolationError
}
