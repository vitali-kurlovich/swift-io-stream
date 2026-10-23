//
//  Created by Kurlovich Vitali on 10/23/26.
//

import NIOCore
import NIOHTTP1
import NIOPosix
import NIOWebSocket

final class PolicyViolationHandler: ChannelInboundHandler, Sendable {
    typealias InboundIn = WebSocketFrame
    typealias OutboundOut = WebSocketFrame

    func channelRead(
        context: ChannelHandlerContext,
        data: NIOAny
    ) {
        let frame = unwrapInboundIn(data)

        switch frame.opcode {
        case .text, .binary:
            let code = WebSocketErrorCode.policyViolation

            var data = context.channel.allocator.buffer(capacity: 2)
            data.write(webSocketErrorCode: code)

            let frame = WebSocketFrame(fin: true, opcode: .connectionClose, data: data)

            context.writeAndFlush(wrapOutboundOut(frame)).whenComplete { _ in
                context.close(mode: .output, promise: nil)
            }

        default:
            break
        }
    }
}
