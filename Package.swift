// swift-tools-version:6.4
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "swift-io-stream",

    platforms: [
        .macOS(.v15),
        .iOS(.v18),
        .watchOS(.v11),
        .tvOS(.v18),
    ],

    products: [
        .library(
            name: "StreamWebSocket",
            targets: ["StreamWebSocket"]
        ),
    ],
    traits: [
        .trait(name: "WebsocketLogging", description: "Enables websocket logging features"),
        // .default(enabledTraits: ["WebsocketLogging"]),
    ],

    dependencies: [
        .package(url: "https://github.com/apple/swift-log", from: "1.15.1"),
        .package(url: "https://github.com/apple/swift-async-algorithms", from: "1.0.0"),
        .package(url: "https://github.com/swiftlang/swift-docc-plugin", from: "1.1.0"),
    ],
    targets: [
        .target(
            name: "StreamWebSocket",
            dependencies: [
                .product(name: "AsyncAlgorithms", package: "swift-async-algorithms"),
                .product(
                    name: "Logging",
                    package: "swift-log",
                    condition: .when(traits: ["WebsocketLogging"])
                ),
            ]

        ),
    ],
    swiftLanguageModes: [.v6]
)
