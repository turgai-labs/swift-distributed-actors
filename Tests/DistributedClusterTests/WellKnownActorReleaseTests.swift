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

import Atomics
import Distributed
import DistributedActorsTestKit
import XCTest

@testable import DistributedCluster

/// A well-known actor that says when it has ended.
distributed actor WellKnownUntilReleased {
    typealias ActorSystem = ClusterSystem

    @ActorID.Metadata(\.wellKnown)
    var wellKnownName: String

    private let ended: ManagedAtomic<Bool>

    init(name: String, ended: ManagedAtomic<Bool>, actorSystem: ActorSystem) {
        self.actorSystem = actorSystem
        self.ended = ended
        self.wellKnownName = name
    }

    deinit {
        self.ended.store(true, ordering: .relaxed)
    }
}

final class WellKnownActorReleaseTests: SingleClusterSystemXCTestCase {
    /// Releasing the ID of a well-known actor only the system still holds lets it end: its
    /// `deinit` resigns its ID, which takes the system's naming lock, so the release mustn't
    /// still hold that lock when it lets the actor go.
    func test_releaseWellKnownActorID_endsAnActorOnlyTheSystemHolds() throws {
        let ended = ManagedAtomic(false)
        let id = WellKnownUntilReleased(name: "until-released", ended: ended, actorSystem: self.system).id

        self.system.releaseWellKnownActorID(id)

        ended.load(ordering: .relaxed).shouldBeTrue()
    }
}
