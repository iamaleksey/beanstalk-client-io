Beanstalk := Object clone do(

	Connection := Object clone do(

		with := method(addr,
			host := addr split(":") first
			port := addr split(":") second asNumber

			conn := self clone
			conn socket := Socket clone setHost(host) setPort(port) connect
			conn
		)

		# Producer Commands
		put := method(body, pri, delay, ttr,
			body = body asString
			if(pri == nil, pri = 65536)
			if(delay == nil, delay = 0)
			if(ttr == nil, ttr = 120)

			cmd := "put #{pri} #{delay} #{ttr} #{body size}\r\n#{body}" interpolate
			command(cmd, list("INSERTED", "BURIED")) split last asNumber
		)

		use := method(tube,
			command("use #{tube}" interpolate, list("USING")) split last
		)

		# Worker Commands
		reserve := method(timeout,
			cmd := if(timeout == nil, "reserve", "reserve-with-timeout #{timeout}" interpolate)
			readJob(command(cmd, list("RESERVED")))
		)

		reserveWithTimeout := method(timeout,
			reserve(timeout)
		)

		delete := method(id,
			command("delete #{id}" interpolate, list("DELETED"))
			self
		)

		release := method(id, pri, delay,
			if(pri == nil, pri = 65536)
			if(delay == nil, delay = 0)
			command("release #{id} #{pri} #{delay}" interpolate, list("RELEASED"))
			self
		)

		bury := method(id, pri,
			if(pri == nil, pri = 65536)
			command("bury #{id} #{pri}" interpolate, list("BURIED"))
			self
		)

		touch := method(id,
			command("touch #{id}" interpolate, list("TOUCHED"))
			self
		)

		watch := method(tube,
			command("watch #{tube}" interpolate, list("WATCHING")) split last asNumber
		)

		ignore := method(tube,
			command("ignore #{tube}" interpolate, list("WATCHING")) split last asNumber
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
			command("kick #{bound}" interpolate, list("KICKED")) split last asNumber
		)

		statsJob := method()
		statsTube := method()
		stats := method()

		listTubes := method()

		listTubeUsed := method(
			command("list-tube-used", list("USING")) split last
		)

		listTubesWatched := method()

		# Non-API methods
		close := method(
			socket close,
			self
		)

		# Internals
		command := method(cmd, expected,
			socket streamWrite(cmd .. "\r\n")
			response := socket readUntilSeq("\r\n")
			if(response split containsAny(expected),
				response,
				Exception raise(Beanstalk errorWithMessage(response))
			)
		)

		peekGeneric := method(cmd,
			readJob(command(cmd, list("FOUND")))
		)

		readJob := method(response,
			id   := response split second asNumber
			size := response split third  asNumber # excluding \r\n

			while(socket readBuffer size < size + 2, socket read)
			body := socket readBuffer inclusiveSlice(0, size - 1)
			socket readBuffer removeSlice(0, size + 1)

			Beanstalk Job with(id, body, self)
		)

	)

	Job := Object clone do(

		with := method(id, body, connection,
			job := self clone
			job id := id
			job body := body
			job connection := connection
			job
		)

		delete := method(
			connection delete(id)
			self
		)

		release := method(pri, delay,
			connection release(id, pri, delay)
			self
		)

		bury := method(pri,
			connection bury(id, pri)
			self
		)

		touch := method(
			connection touch(id)
			self
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
