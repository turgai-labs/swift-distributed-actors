//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Distributed Actors open source project
//
// Copyright (c) 2026 Apple Inc. and the Swift Distributed Actors project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Distributed Actors project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Distributed
import DistributedActorsTestKit
import XCTest

@testable import DistributedCluster

private distributed actor Counter {
    typealias ActorSystem = ClusterSystem

    private var count = 0

    distributed func increment() -> Int {
        self.count += 1
        return self.count
    }

    distributed func value() -> Int {
        self.count
    }
}

private struct Refused: Error, Codable {}

/// The calls a filter was asked about: each one's actor and target.
private actor Asked {
    private(set) var calls: [(ActorID, String)] = []

    func record(_ actor: any DistributedActor, _ target: RemoteCallTarget) {
        if let id = actor.id as? ActorID {
            self.calls.append((id, target.identifier))
        }
    }
}

/// Lets every call through but `increment`, recording each one it's asked about.
private func refusingIncrement(asked: Asked) -> @Sendable (any DistributedActor, RemoteCallTarget) async throws -> Void {
    { actor, target in
        await asked.record(actor, target)
        if target.identifier.contains("increment") { throw Refused() }
    }
}

/// Passes each call on to `counter` as a local call, as a proxy to a local actor does.
private struct ToCounter: RemoteCallInterceptor {
    let counter: Counter

    func interceptRemoteCall<Act, Err, Res>(
        on actor: Act,
        target: RemoteCallTarget,
        invocation: inout ClusterSystem.InvocationEncoder,
        throwing: Err.Type,
        returning: Res.Type
    ) async throws -> Res
    where
        Act: DistributedActor,
        Act.ID == ActorID,
        Err: Error,
        Res: Codable
    {
        try await self.counter.actorSystem.localCall(
            on: self.counter,
            target: target,
            invocation: &invocation,
            throwing: throwing,
            returning: returning
        )
    }

    func interceptRemoteCallVoid<Act, Err>(
        on actor: Act,
        target: RemoteCallTarget,
        invocation: inout ClusterSystem.InvocationEncoder,
        throwing: Err.Type
    ) async throws
    where
        Act: DistributedActor,
        Act.ID == ActorID,
        Err: Error
    {
        try await self.counter.actorSystem.localCallVoid(
            on: self.counter,
            target: target,
            invocation: &invocation,
            throwing: throwing
        )
    }
}

final class IncomingCallFilterTests: ClusteredActorSystemsXCTestCase {
    func test_remoteCall_refusedByTheFilter_doesNotRun_andThrowsItsError() async throws {
        let asked = Asked()
        let owner = await setUpNode("owner") { settings in
            settings.remoteCall.incomingCallFilter = refusingIncrement(asked: asked)
            settings.serialization.registerInbound(Refused.self)
        }
        let caller = await setUpNode("caller") { settings in
            settings.serialization.registerInbound(Refused.self)
        }
        caller.cluster.join(endpoint: owner.cluster.endpoint)

        let counter = Counter(actorSystem: owner)
        let remote = try Counter.resolve(id: counter.id, using: caller)

        let error = try await shouldThrow {
            _ = try await remote.increment()
        }
        guard error is Refused else {
            throw TestError("Expected the filter's error, got: \(error)")
        }
        // Let through, and the refused call never ran.
        try await remote.value().shouldEqual(0)

        // It's asked about every call an actor of this node takes, the cluster's own too, such as
        // SWIM's: these two are the counter's.
        let calls = await asked.calls
        calls.filter { $0.0 == counter.id }.count.shouldEqual(2)
    }

    func test_localCallThroughAProxy_refusedByTheFilter_doesNotRun_andThrowsItsError() async throws {
        let asked = Asked()
        let system = await setUpNode("owner") { settings in
            settings.remoteCall.incomingCallFilter = refusingIncrement(asked: asked)
        }
        let counter = Counter(actorSystem: system)
        let proxy: Counter = try system.interceptCalls(
            to: Counter.self,
            metadata: ActorMetadata(),
            interceptor: ToCounter(counter: counter)
        )

        let error = try await shouldThrow {
            _ = try await proxy.increment()
        }
        guard error is Refused else {
            throw TestError("Expected the filter's error, got: \(error)")
        }
        try await proxy.value().shouldEqual(0)

        let calls = await asked.calls
        calls.map(\.0).shouldEqual([counter.id, counter.id])
    }

    func test_withoutAFilter_everyCallRuns() async throws {
        let owner = await setUpNode("owner")
        let caller = await setUpNode("caller")
        caller.cluster.join(endpoint: owner.cluster.endpoint)

        let counter = Counter(actorSystem: owner)
        let remote = try Counter.resolve(id: counter.id, using: caller)

        try await remote.increment().shouldEqual(1)
        try await remote.value().shouldEqual(1)
    }
}
