// swift-tools-version:6.4
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "swift-io-stream",
    platforms: [
        .macOS(.v14),
        .iOS(.v16),
        .watchOS(.v10),
        .tvOS(.v17),
    ],
    products: [
        .library(
            name: "StreamWebSocket",
            targets: ["StreamWebSocket"]
        ),
    ],
    dependencies: [],
    targets: [
        .target(
            name: "StreamWebSocket",
            dependencies: [
            ]
        ),
    ],
    swiftLanguageModes: [.v6]
)
