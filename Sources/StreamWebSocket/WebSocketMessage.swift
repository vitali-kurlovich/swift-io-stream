//
//  Created by Kurlovich Vitali on 9/29/26.
//

import Foundation

public enum WebSocketMessage: Sendable {
    case text(String)
    case data(Data)
}
