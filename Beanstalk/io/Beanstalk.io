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

			cmd := "put #{pri} #{delay} #{ttr} #{body size}\r\n#{body}\r\n" interpolate
			command(cmd, list("INSERTED", "BURIED")) split last asNumber
		)

		use := method(tube,
			command("use #{tube}\r\n" interpolate, list("USING")) split last
		)

		# Other non-api methods
		close := method(
			socket close,
			self
		)

		# Internals
		command := method(cmd, expected,
			socket streamWrite(cmd)
			response := socket readUntilSeq("\r\n")
			if(response split containsAny(expected),
				response,
				Exception raise(Beanstalk errorWithMessage(response))
			)
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
	
	allErrors := method(
		self slotNames select(endsWithSeq("Error")) map(name, self getSlot(name))
	)

	errorWithMessage := method(msg, allErrors detect(message == msg))

)
