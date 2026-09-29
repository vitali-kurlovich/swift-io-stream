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
    traits: [
        .trait(name: "WebsocketLogging", description: "Enables websocket logging features"),
        //.default(enabledTraits: ["WebsocketLogging"]),
    ],

    dependencies: [
        .package(url: "https://github.com/apple/swift-log", from: "1.15.1"),
    ],
    targets: [
        .target(
            name: "StreamWebSocket",
            dependencies: [
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
