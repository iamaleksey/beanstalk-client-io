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
				error(response)
			)
		)

		error := method(msg,
			err:= list(
				Beanstalk OutOfMemoryError,
				Beanstalk InternalError,
				Beanstalk DrainingError,
				Beanstalk BadFormatError,
				Beanstalk UnknownCommandError
			) detect(e, e message == msg)
			Exception raise(err)
		)

	)

	# Errors
	OutOfMemoryError    := Error clone with("OUT_OF_MEMORY")
	InternalError       := Error clone with("INTERNAL_ERROR")
	DrainingError       := Error clone with("DRAINING")
	BadFormatError      := Error clone with("BAD_FORMAT")
	UnknownCommandError := Error clone with("UNKNOWN_COMMAND")

)
