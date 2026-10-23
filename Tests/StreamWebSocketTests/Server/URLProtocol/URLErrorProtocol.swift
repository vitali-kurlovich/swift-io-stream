//
//  Created by Kurlovich Vitali on 10/23/26.
//

import Foundation

class URLErrorProtocol: URLProtocol {
    var error: URLError {
        fatalError("Must be overrading")
    } // .Code = .notConnectedToInternet

    override class func canInit(with _: URLRequest) -> Bool {
        true
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        request
    }

    override func startLoading() {
        client?.urlProtocol(self, didFailWithError: error)
    }

    override func stopLoading() {}
}

final class NotConnectedToInternetURLErrorProtocol: URLErrorProtocol {
    override var error: URLError {
        URLError(.notConnectedToInternet)
    }
}

final class SecureConnectionFailedURLErrorProtocol: URLErrorProtocol {
    override var error: URLError {
        URLError(.secureConnectionFailed)
    }
}
