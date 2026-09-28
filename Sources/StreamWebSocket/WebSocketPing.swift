//
//  Created by Kurlovich Vitali on 9/29/26.
//

public protocol WebSocketPing: Sendable {
    func ping(_ socket: WebSocket)
}
