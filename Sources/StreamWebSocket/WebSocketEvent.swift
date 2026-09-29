//
//  Created by Kurlovich Vitali on 9/29/26.
//

import Foundation

public enum WebSocketEvent: Sendable {
    case onConnect(WebSocket, URLSessionWebSocketTask, String?)
    case onDisconnet(WebSocket, URLSessionWebSocketTask, URLSessionWebSocketTask.CloseCode, Data?)
}
