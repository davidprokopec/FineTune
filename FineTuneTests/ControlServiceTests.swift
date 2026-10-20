import XCTest
import Observation
@testable import FineTune

@Observable
@MainActor
private final class FakeControlEngine: ControlEngine {
    var state: ControlJSON = .object(["value": .number(0)])
    var commands: [ControlCommand] = []
    var failure: ControlFailure?
    func controlState() -> ControlJSON { state }
    func performControlCommand(_ command: ControlCommand) throws {
        if let failure { throw failure }
        commands.append(command)
        state = .object(["value": .number(Double(commands.count))])
    }
}

@MainActor
private final class ControlServiceFixture {
    let engine = FakeControlEngine()
    var messages: [UUID: [ControlJSON]] = [:]
    var closed: [UUID] = []
    var scheduled: [@MainActor () -> Void] = []
    lazy var service = ControlService(
        engine: engine,
        send: { [weak self] data, id in
            do { self?.messages[id, default: []].append(try JSONDecoder().decode(ControlJSON.self, from: data)) }
            catch { XCTFail("Invalid outbound JSON: \(error)") }
        },
        disconnect: { [weak self] in self?.closed.append($0) },
        schedule: { [weak self] action in
            self?.scheduled.append(action)
            return Task {}
        }
    )

    func connect() -> UUID {
        if service.sessionID.isEmpty { service.start(listen: false) }
        let id = UUID()
        service.connected(id, session: service.sessionID)
        return id
    }

    func request(_ method: String, id requestID: String, client: UUID, params: [String: ControlJSON] = [:], session: String? = nil) throws {
        let value = ControlJSON.object([
            "type": .string("request"), "id": .string(requestID),
            "sessionId": .string(session ?? service.sessionID), "method": .string(method), "params": .object(params)
        ])
        service.received(try JSONEncoder().encode(value), from: client, session: service.sessionID)
    }

    func runPublication() {
        let actions = scheduled
        scheduled.removeAll()
        for action in actions { action() }
    }
}

nonisolated final class ControlServiceTests: XCTestCase {
    @MainActor
    func testHelloSubscribeDedupFlushAndSessionReset() throws {
        let f = ControlServiceFixture()
        let id = f.connect()
        defer { f.service.stop() }
        XCTAssertEqual(f.messages[id]?.first?.object?["protocol"], .string("finetune-control"))
        XCTAssertEqual(f.messages[id]?.first?.object?["version"], .number(1))
        try f.request("subscribe", id: "1", client: id)
        XCTAssertEqual(f.messages[id]?.last?.object?["result"]?.object?["revision"], .number(0))
        f.engine.state = .object(["value": .number(2)])
        try f.request("getState", id: "2", client: id)
        XCTAssertEqual(f.service.revision, 1)
        XCTAssertEqual(f.messages[id]?.last?.object?["result"]?.object?["state"], f.engine.state)
        f.service.flush()
        XCTAssertEqual(f.service.revision, 1)
        let session = f.service.sessionID
        f.service.stop()
        f.service.start(listen: false)
        XCTAssertNotEqual(f.service.sessionID, session)
        XCTAssertEqual(f.service.revision, 0)
    }

    @MainActor
    func testSubscribeResponsePrecedesEventsAndCommandResponse() throws {
        let f = ControlServiceFixture()
        let id = f.connect()
        defer { f.service.stop() }
        f.engine.state = .object(["value": .number(7)])
        try f.request("subscribe", id: "1", client: id)
        XCTAssertEqual(f.messages[id]?.count, 2)
        XCTAssertEqual(f.messages[id]?.last?.object?["type"], .string("response"))
        try f.request("setAppMute", id: "2", client: id, params: ["appId": .string("a"), "muted": .bool(true)])
        XCTAssertEqual(f.engine.commands, [.mute("a", nil, true)])
        f.service.flush()
        let messages = try XCTUnwrap(f.messages[id])
        XCTAssertEqual(messages[messages.count - 2].object?["result"], .object(["accepted": .bool(true)]))
        XCTAssertEqual(messages.last?.object?["type"], .string("event"))
        f.engine.state = .object(["value": .number(9)])
        try f.request("subscribe", id: "3", client: id)
        XCTAssertEqual(f.messages[id]?.count, messages.count + 1)
    }

    @MainActor
    func testObservationRearmsWithoutRestartingPublicationWindow() async throws {
        let f = ControlServiceFixture()
        let id = f.connect()
        defer { f.service.stop() }
        try f.request("subscribe", id: "1", client: id)
        f.engine.state = .object(["value": .number(1)])
        await nextMainTurn()
        XCTAssertEqual(f.scheduled.count, 1)
        f.engine.state = .object(["value": .number(2)])
        await nextMainTurn()
        XCTAssertEqual(f.scheduled.count, 1)
        f.runPublication()
        XCTAssertEqual(f.service.revision, 1)
        XCTAssertEqual(f.messages[id]?.last?.object?["state"], f.engine.state)
        f.engine.state = .object(["value": .number(3)])
        await nextMainTurn()
        XCTAssertEqual(f.scheduled.count, 1)
        f.runPublication()
        XCTAssertEqual(f.service.revision, 2)
    }

    @MainActor
    func testErrorsAndDuplicateIDDisconnect() throws {
        let f = ControlServiceFixture()
        let id = f.connect()
        defer { f.service.stop() }
        try f.request("getState", id: "1", client: id, session: "old")
        XCTAssertEqual(f.messages[id]?.last?.object?["error"]?.object?["code"], .string("staleSession"))
        for (index, code) in ["appNotFound", "ambiguousApp", "staleApp", "deviceNotFound", "unsupportedRouting", "permissionDenied", "commandFailed", "internalError"].enumerated() {
            f.engine.failure = ControlFailure(code: code, message: "Test failure")
            try f.request("setPreferredInput", id: "error\(index)", client: id, params: ["deviceUID": .string("mic")])
            XCTAssertEqual(f.messages[id]?.last?.object?["error"]?.object?["code"], .string(code))
        }
        try f.request("unknown", id: "method", client: id)
        XCTAssertEqual(f.messages[id]?.last?.object?["error"]?.object?["code"], .string("unknownMethod"))
        try f.request("setAppMute", id: "params", client: id)
        XCTAssertEqual(f.messages[id]?.last?.object?["error"]?.object?["code"], .string("invalidParams"))
        try f.request("getState", id: "1", client: id)
        XCTAssertEqual(f.closed, [id])
    }

    @MainActor
    func testMalformedInputAndUnencodableStateDisconnect() {
        let f = ControlServiceFixture()
        let id = f.connect()
        defer { f.service.stop() }
        f.service.received(Data("not json".utf8), from: id, session: f.service.sessionID)
        XCTAssertEqual(f.closed, [id])
        let other = f.connect()
        f.engine.state = .number(.nan)
        f.service.flush()
        XCTAssertNoThrow(try f.request("getState", id: "state", client: other))
        XCTAssertTrue(f.closed.contains(other))
    }

    @MainActor
    func testSettingObservationDefaultOffEnableDisableAndReenable() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("ft-" + UUID().uuidString.prefix(8))
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let settings = SettingsManager(directory: root)
        let directory = root.appendingPathComponent("FineTune/Control")
        let server = UnixSocketServer(directory: directory)
        let service = ControlService(engine: FakeControlEngine(), settings: settings, server: server)
        service.observeSetting()
        XCTAssertEqual(service.sessionID, "")
        XCTAssertFalse(FileManager.default.fileExists(atPath: directory.appendingPathComponent("control.sock").path))
        settings.appSettings.externalControlEnabled = true
        await nextMainTurn()
        let firstSession = service.sessionID
        XCTAssertFalse(firstSession.isEmpty)
        settings.appSettings.externalControlEnabled = false
        await nextMainTurn()
        await withCheckedContinuation { continuation in server.stop { continuation.resume() } }
        XCTAssertFalse(FileManager.default.fileExists(atPath: directory.appendingPathComponent("control.sock").path))
        settings.appSettings.externalControlEnabled = true
        await nextMainTurn()
        XCTAssertNotEqual(service.sessionID, firstSession)
        await withCheckedContinuation { continuation in service.stop { continuation.resume() } }
        XCTAssertFalse(FileManager.default.fileExists(atPath: directory.appendingPathComponent("control.sock").path))
        settings.flushSync()
        try FileManager.default.removeItem(at: root)
    }

    @MainActor
    private func nextMainTurn() async {
        await withCheckedContinuation { continuation in
            DispatchQueue.main.async { continuation.resume() }
        }
    }
}
