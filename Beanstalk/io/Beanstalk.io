Beanstalk := Object clone do(
//metadoc Beanstalk Aleksey Yeschenko, 2009
//metadoc Beanstalk license BSD revised
//metadoc Beanstalk category Messaging

	Connection := Object clone do(

		with := method(addr,
			self clone open(addr)
		)

		to := method(addr, with(addr))

		# Producer Commands
		put := method(body, pri, delay, ttr,
			body = body asString
			if(pri == nil, pri = 65536)
			if(delay == nil, delay = 0)
			if(ttr == nil, ttr = 120)

			cmd := "put #{pri} #{delay} #{ttr} #{body size}\r\n#{body}" interpolate
			command(cmd, list("INSERTED", "BURIED")) at(1) asNumber
		)

		use := method(tube,
			command("use #{tube}" interpolate, "USING") at(1)
		)

		# Worker Commands
		reserve := method(timeout,
			cmd := if(timeout == nil, "reserve", "reserve-with-timeout #{timeout}" interpolate)
			readJob(command(cmd, "RESERVED"))
		)

		reserveWithTimeout := method(timeout,
			reserve(timeout)
		)

		delete := method(id,
			command("delete #{id}" interpolate, "DELETED")
			self
		)

		release := method(id, pri, delay,
			if(pri == nil, pri = 65536)
			if(delay == nil, delay = 0)
			command("release #{id} #{pri} #{delay}" interpolate, "RELEASED")
			self
		)

		bury := method(id, pri,
			if(pri == nil, pri = 65536)
			command("bury #{id} #{pri}" interpolate, "BURIED")
			self
		)

		touch := method(id,
			command("touch #{id}" interpolate, "TOUCHED")
			self
		)

		watch := method(tube,
			command("watch #{tube}" interpolate, "WATCHING") at(1) asNumber
		)

		ignore := method(tube,
			command("ignore #{tube}" interpolate, "WATCHING") at(1) asNumber
		)

		# Other Commands
		peek := method(id,
			peekGeneric("peek #{id}" interpolate)
		)

		peekReady := method(
			peekGeneric("peek-ready")
		)

		peekDelayed := method(
			peekGeneric("peek-delayed")
		)

		peekBuried := method(
			peekGeneric("peek-buried")
		)

		kick := method(bound,
			command("kick #{bound}" interpolate, "KICKED") at(1) asNumber
		)

		statsJob := method(id,
			readYAML("stats-job #{id}" interpolate)
		)

		statsTube := method(tube,
			readYAML("stats-tube #{tube}" interpolate)
		)

		stats := method(
			readYAML("stats")
		)

		listTubes := method(
			readYAML("list-tubes")
		)

		listTubeUsed := method(
			command("list-tube-used", "USING") at(1)
		)

		listTubesWatched := method(
			readYAML("list-tubes-watched")
		)

		# Non-API methods
		open := method(addr,
			host := addr split(":") first
			port := addr split(":") second asNumber
			self socket := Socket clone setHost(host) setPort(port) connect
			self
		)

		close := method(
			socket close,
			self
		)

		# Internals
		command := method(cmd, expected,
			socket streamWrite(cmd .. "\r\n")
			expected = list(expected) flatten
			response := socket readUntilSeq("\r\n") split
			if(response containsAny(expected),
				response,
				Exception raise(Beanstalk errorWithMessage(response at(0)))
			)
		)

		peekGeneric := method(cmd,
			readJob(command(cmd, "FOUND"))
		)

		readJob := method(response,
			id   := response at(1) asNumber
			size := response at(2) asNumber # excluding \r\n
			body := socket readBytes(size + 2) inclusiveSlice(0, size - 1)
			Beanstalk Job with(id, body, self)
		)

		readYAML := method(cmd,
			size := command(cmd, "OK") at(1) asNumber
			data := socket readBytes(size + 2) inclusiveSlice(0, size - 1)
			YAML load(data)
		)

	)

	Job := Object clone do(

		with := method(id, body, connection,
			job := self clone
			job id := id
			job body := body
			job connection := connection
			job reserved := true
			job
		)

		delete := method(
			if(reserved, connection delete(id))
			reserved = false
			self
		)

		release := method(pri, delay,
			if(reserved, connection release(id, pri, delay))
			reserved = false
			self
		)

		bury := method(pri,
			if(reserved, connection bury(id, pri))
			reserved = false
			self
		)

		touch := method(
			if(reserved, connection touch(id))
			self
		)

		stats := method(
			connection statsJob(id)
		)

	)

	# Common Errors
	OutOfMemoryError    := Error clone with("OUT_OF_MEMORY")
	InternalError       := Error clone with("INTERNAL_ERROR")
	DrainingError       := Error clone with("DRAINING")
	BadFormatError      := Error clone with("BAD_FORMAT")
	UnknownCommandError := Error clone with("UNKNOWN_COMMAND")

	# put Errors
	ExpectedCRLFError   := Error clone with("EXPECTED_CRLF")
	JobTooBigError      := Error clone with("JOB_TOO_BIG")

	# reserve and reserve-with-timeout Errors
	DeadlineSoonError   := Error clone with("DEADLINE_SOON")
	TimedOutError       := Error clone with("TIMED_OUT")

	# delete, release, bury and touch Errors
	NotFoundError       := Error clone with("NOT_FOUND")

	# ignore Errors
	NotIgnoredError     := Error clone with("NOT_IGNORED")

	# peek, peek-ready, peek-delayed and peek-buried can respond
	# with NOT_FOUND, which is defined already
	# same with stats-job and stats-tube

	allErrors := method(
		self slotNames select(endsWithSeq("Error")) map(name, self getSlot(name))
	)

	errorWithMessage := method(msg, allErrors detect(message == msg))

)

# Am I missing an existing method for this?
Socket readBytes := method(n,
	while(readBuffer size < n, self read)
	bytes := readBuffer inclusiveSlice(0, n - 1)
	readBuffer removeSlice(0, n - 1)
	bytes
)
