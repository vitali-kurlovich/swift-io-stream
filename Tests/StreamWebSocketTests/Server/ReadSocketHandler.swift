//
//  Created by Kurlovich Vitali on 10/21/26.
//

import Foundation
import NIOCore
import NIOHTTP1
import NIOPosix
import NIOWebSocket

final class ReadSocketHandler: ChannelInboundHandler, Sendable {
    typealias InboundIn = WebSocketFrame
    typealias OutboundOut = WebSocketFrame

    let recieveHandler: @Sendable (WebSocketOpcode, ByteBuffer) -> Void

    init(recieveHandler: @escaping @Sendable (WebSocketOpcode, ByteBuffer) -> Void) {
        self.recieveHandler = recieveHandler
    }

    func channelRead(
        context: ChannelHandlerContext,
        data: NIOAny
    ) {
        let frame = unwrapInboundIn(data)

        switch frame.opcode {
        case .text:
            let payload = frame.unmaskedData
            recieveHandler(frame.opcode, payload)

        case .binary:
            recieveHandler(frame.opcode, frame.unmaskedData)

        case .ping:
            let payload = frame.unmaskedData

            recieveHandler(frame.opcode, payload)

            let pong = WebSocketFrame(
                fin: true,
                opcode: .pong,
                data: payload
            )

            context.writeAndFlush(
                wrapOutboundOut(pong),
                promise: nil
            )

        case .connectionClose:
            let close = WebSocketFrame(
                fin: true,
                opcode: .connectionClose,
                data: frame.data
            )

            context.writeAndFlush(
                wrapOutboundOut(close)
            ).flatMap {
                context.close()
            }.whenFailure { error in
                context.close(promise: nil)
                print("Failed to write close frame:", error)
            }

        default:
            break
        }
    }

    func errorCaught(
        context: ChannelHandlerContext,
        error _: Error
    ) {
        context.close(promise: nil)
    }
}
