import Foundation
import Darwin
import os

/// All descriptors and buffers are confined to `queue`; callbacks run on that queue.
final class UnixSocketServer: @unchecked Sendable {
    static let maximumFrameBytes = 1_048_576
    static let maximumOutputBytes = 4 * 1_048_576
    static let maximumOutputFrames = 8

    private final class Client {
        let fd: Int32
        let reader: DispatchSourceRead
        let writer: DispatchSourceWrite
        var input = Data()
        var output: [Data] = []
        var offset = 0
        var partialSince: TimeInterval?
        var writeProgress: TimeInterval?
        var writerResumed = false
        var readerSuspended = false

        init(fd: Int32, queue: DispatchQueue) {
            self.fd = fd
            reader = DispatchSource.makeReadSource(fileDescriptor: fd, queue: queue)
            writer = DispatchSource.makeWriteSource(fileDescriptor: fd, queue: queue)
        }
    }

    private let queue = DispatchQueue(label: "com.finetuneapp.control")
    private let directory: URL
    private let peerUID: @Sendable (Int32) -> uid_t?
    private let now: @Sendable () -> TimeInterval
    private let fileStatus: @Sendable (Int32, String?, UnsafeMutablePointer<stat>) -> Int32
    private let logger = Logger(subsystem: "com.finetuneapp.FineTune", category: "ControlAPI")
    private var listener: Int32 = -1
    private var lockFD: Int32 = -1
    private var directoryFD: Int32 = -1
    private var socketIdentity: (dev_t, ino_t)?
    private var acceptSource: DispatchSourceRead?
    private var timer: DispatchSourceTimer?
    private var clients: [UUID: Client] = [:]
    private var onConnect: (@Sendable (UUID) -> Void)?
    private var onFrame: (@Sendable (UUID, Data, @escaping @Sendable () -> Void) -> Void)?
    private var onDisconnect: (@Sendable (UUID) -> Void)?

    init(
        directory: URL? = nil,
        peerUID: @escaping @Sendable (Int32) -> uid_t? = { fd in
            var uid: uid_t = 0
            var gid: gid_t = 0
            return getpeereid(fd, &uid, &gid) == 0 ? uid : nil
        },
        now: @escaping @Sendable () -> TimeInterval = { ProcessInfo.processInfo.systemUptime },
        fileStatus: @escaping @Sendable (Int32, String?, UnsafeMutablePointer<stat>) -> Int32 = UnixSocketServer.readFileStatus
    ) {
        self.directory = directory ?? FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent("Library/Application Support/FineTune/Control", isDirectory: true)
        self.peerUID = peerUID
        self.now = now
        self.fileStatus = fileStatus
    }

    static func readFileStatus(_ fd: Int32, _ name: String?, _ info: UnsafeMutablePointer<stat>) -> Int32 {
        if let name { return fstatat(fd, name, info, AT_SYMLINK_NOFOLLOW) }
        return fstat(fd, info)
    }

    func start(
        onConnect: @escaping @Sendable (UUID) -> Void,
        onFrame: @escaping @Sendable (UUID, Data, @escaping @Sendable () -> Void) -> Void,
        onDisconnect: @escaping @Sendable (UUID) -> Void,
        onFailure: @escaping @Sendable (String) -> Void,
        onReady: @escaping @Sendable () -> Void = {}
    ) {
        queue.async {
            guard self.listener == -1 else { return }
            self.onConnect = onConnect
            self.onFrame = onFrame
            self.onDisconnect = onDisconnect
            do {
                try self.openListener()
                onReady()
            }
            catch {
                self.logger.error("External controller listener failed: \(error.localizedDescription)")
                self.stopOnQueue()
                onFailure(error.localizedDescription)
            }
        }
    }

    func send(_ payload: Data, to id: UUID) {
        queue.async {
            guard let client = self.clients[id] else { return }
            let buffered = client.output.reduce(0) { $0 + $1.count } - client.offset
            guard payload.count <= Self.maximumFrameBytes,
                  client.output.count < Self.maximumOutputFrames,
                  buffered + payload.count + 1 <= Self.maximumOutputBytes else {
                self.logger.error("Disconnecting external controller: outbound limit exceeded")
                self.closeClient(id)
                return
            }
            client.output.append(payload + Data([10]))
            if client.writeProgress == nil { client.writeProgress = self.now() }
            self.writeClient(id)
        }
    }

    func disconnect(_ id: UUID) { queue.async { self.closeClient(id) } }
    func stop(completion: @escaping @Sendable () -> Void = {}) {
        queue.async { self.stopOnQueue(); completion() }
    }

    private func failure(_ message: String) -> ControlFailure {
        ControlFailure(code: "listenerFailed", message: "\(message) (errno \(errno))")
    }

    private func openListener() throws {
        // Open each API-owned path component without following symlinks. Never chmod parents.
        let parent = directory.deletingLastPathComponent()
        let support = parent.deletingLastPathComponent()
        let supportFD = open(support.path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
        guard supportFD >= 0 else { throw failure("Cannot open Application Support") }
        defer { close(supportFD) }
        if mkdirat(supportFD, parent.lastPathComponent, 0o700) != 0 && errno != EEXIST {
            throw failure("Cannot create FineTune directory")
        }
        let parentFD = openat(supportFD, parent.lastPathComponent, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
        guard parentFD >= 0 else { throw failure("Unsafe FineTune directory") }
        defer { close(parentFD) }
        var info = stat()
        guard fileStatus(parentFD, nil, &info) == 0, info.st_uid == geteuid(), info.st_mode & 0o022 == 0 else {
            throw failure("FineTune directory must belong to this user and not be writable by others")
        }
        if mkdirat(parentFD, directory.lastPathComponent, 0o700) != 0 && errno != EEXIST {
            throw failure("Cannot create Control directory")
        }
        directoryFD = openat(parentFD, directory.lastPathComponent, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
        guard directoryFD >= 0, fileStatus(directoryFD, nil, &info) == 0, info.st_uid == geteuid(),
              fchmod(directoryFD, 0o700) == 0 else { throw failure("Unsafe Control directory") }
        lockFD = openat(directoryFD, "server.lock", O_RDWR | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0o600)
        guard lockFD >= 0, fileStatus(lockFD, nil, &info) == 0, info.st_uid == geteuid(),
              info.st_mode & S_IFMT == S_IFREG, flock(lockFD, LOCK_EX | LOCK_NB) == 0 else {
            throw failure("Cannot acquire external controller lock")
        }
        if fileStatus(directoryFD, "control.sock", &info) == 0 {
            guard info.st_uid == geteuid(), info.st_mode & S_IFMT == S_IFSOCK else {
                throw failure("Refusing unsafe existing control.sock")
            }
            guard unlinkat(directoryFD, "control.sock", 0) == 0 else { throw failure("Cannot remove stale socket") }
        } else if errno != ENOENT { throw failure("Cannot inspect control.sock") }

        var address = sockaddr_un()
        address.sun_family = sa_family_t(AF_UNIX)
        let path = Array(directory.appendingPathComponent("control.sock").path.utf8) + [0]
        guard path.count <= MemoryLayout.size(ofValue: address.sun_path) else { throw failure("Socket path is too long") }
        withUnsafeMutableBytes(of: &address.sun_path) { $0.copyBytes(from: path) }
        address.sun_len = UInt8(MemoryLayout<sockaddr_un>.size)
        listener = socket(AF_UNIX, SOCK_STREAM, 0)
        guard listener >= 0 else { throw failure("Cannot create socket") }
        guard fcntl(listener, F_SETFL, O_NONBLOCK) == 0,
              fcntl(listener, F_SETFD, FD_CLOEXEC) == 0 else { throw failure("Cannot configure socket") }
        let result = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.bind(listener, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
            }
        }
        guard result == 0 else { throw failure("Cannot bind socket") }
        guard fileStatus(directoryFD, "control.sock", &info) == 0 else {
            throw failure("Cannot inspect bound socket")
        }
        socketIdentity = (info.st_dev, info.st_ino)
        guard fchmodat(directoryFD, "control.sock", 0o600, AT_SYMLINK_NOFOLLOW) == 0,
              listen(listener, 8) == 0 else { throw failure("Cannot secure/listen on socket") }
        let source = DispatchSource.makeReadSource(fileDescriptor: listener, queue: queue)
        source.setEventHandler { [weak self] in self?.acceptClients() }
        acceptSource = source
        source.resume()
        let timer = DispatchSource.makeTimerSource(queue: queue)
        timer.schedule(deadline: .now() + 1, repeating: 1)
        timer.setEventHandler { [weak self] in self?.expireClients() }
        self.timer = timer
        timer.resume()
    }

    private func acceptClients() {
        while true {
            let fd = accept(listener, nil, nil)
            if fd < 0 {
                if errno == EINTR { continue }
                return
            }
            // Authenticate before installing a reader or examining a byte.
            guard clients.count < 8, peerUID(fd) == geteuid(),
                  fcntl(fd, F_SETFL, O_NONBLOCK) == 0,
                  fcntl(fd, F_SETFD, FD_CLOEXEC) == 0 else { close(fd); continue }
            var one: Int32 = 1
            guard setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, socklen_t(MemoryLayout.size(ofValue: one))) == 0 else {
                close(fd); continue
            }
            let id = UUID()
            let client = Client(fd: fd, queue: queue)
            clients[id] = client
            client.reader.setEventHandler { [weak self] in self?.readClient(id) }
            client.writer.setEventHandler { [weak self] in self?.writeClient(id) }
            // Closing from the read cancellation handler avoids a descriptor reuse race.
            client.reader.setCancelHandler { close(fd) }
            client.reader.resume()
            onConnect?(id)
        }
    }

    private func readClient(_ id: UUID) {
        guard let client = clients[id], !client.readerSuspended else { return }
        var bytes = [UInt8](repeating: 0, count: 16_384)
        while true {
            let count = Darwin.read(client.fd, &bytes, bytes.count)
            if count == 0 { closeClient(id); return }
            if count < 0 {
                if errno == EINTR { continue }
                if errno != EAGAIN && errno != EWOULDBLOCK { closeClient(id) }
                return
            }
            client.partialSince = now()
            client.input.append(contentsOf: bytes.prefix(count))
            _ = deliverBufferedFrame(id)
            return
        }
    }

    /// One request in flight per client prevents unbounded work/data queued to MainActor.
    private func deliverBufferedFrame(_ id: UUID) -> Bool {
        guard let client = clients[id] else { return false }
        guard let newline = client.input.firstIndex(of: 10) else {
            if client.input.count > Self.maximumFrameBytes { closeClient(id) }
            return false
        }
        guard client.input.distance(from: client.input.startIndex, to: newline) <= Self.maximumFrameBytes else {
            closeClient(id)
            return false
        }
        var frame = Data(client.input[..<newline])
        client.input.removeSubrange(...newline)
        client.partialSince = client.input.isEmpty || client.input.last == 10 ? nil : now()
        if frame.last == 13 { frame.removeLast() }
        guard !frame.isEmpty, String(data: frame, encoding: .utf8) != nil else { closeClient(id); return false }
        if !client.readerSuspended { client.reader.suspend(); client.readerSuspended = true }
        onFrame?(id, frame, { [weak self] in
            guard let self else { return }
            self.queue.async {
                guard let current = self.clients[id] else { return }
                if !self.deliverBufferedFrame(id), self.clients[id] != nil {
                    current.readerSuspended = false
                    current.reader.resume()
                }
            }
        })
        return true
    }

    private func writeClient(_ id: UUID) {
        guard let client = clients[id] else { return }
        while let first = client.output.first {
            let count = first.withUnsafeBytes { bytes in
                Darwin.write(client.fd, bytes.baseAddress!.advanced(by: client.offset), first.count - client.offset)
            }
            if count < 0 {
                if errno == EINTR { continue }
                if errno != EAGAIN && errno != EWOULDBLOCK { closeClient(id); return }
                if !client.writerResumed { client.writerResumed = true; client.writer.resume() }
                return
            }
            guard count > 0 else { closeClient(id); return }
            client.offset += count
            client.writeProgress = now()
            if client.offset == first.count { client.output.removeFirst(); client.offset = 0 }
        }
        client.writeProgress = nil
        if client.writerResumed { client.writer.suspend(); client.writerResumed = false }
    }

    private func expireClients() {
        let time = now()
        for (id, client) in clients {
            if client.partialSince.map({ time - $0 >= 10 }) == true ||
                client.writeProgress.map({ time - $0 >= 10 }) == true { closeClient(id) }
        }
    }

    private func closeClient(_ id: UUID) {
        guard let client = clients.removeValue(forKey: id) else { return }
        client.reader.cancel()
        if client.readerSuspended { client.reader.resume() }
        client.writer.cancel()
        if !client.writerResumed { client.writer.resume() }
        onDisconnect?(id)
    }

    private func stopOnQueue() {
        acceptSource?.cancel()
        acceptSource = nil
        timer?.cancel()
        timer = nil
        for id in Array(clients.keys) { closeClient(id) }
        if listener >= 0 { close(listener); listener = -1 }
        if let identity = socketIdentity {
            var info = stat()
            if fileStatus(directoryFD, "control.sock", &info) == 0,
               info.st_uid == geteuid(), info.st_mode & S_IFMT == S_IFSOCK,
               info.st_dev == identity.0, info.st_ino == identity.1 {
                unlinkat(directoryFD, "control.sock", 0)
            }
        }
        socketIdentity = nil
        if directoryFD >= 0 { close(directoryFD); directoryFD = -1 }
        if lockFD >= 0 { close(lockFD); lockFD = -1 }
        onConnect = nil
        onFrame = nil
        onDisconnect = nil
    }
}
