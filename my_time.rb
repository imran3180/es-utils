class MyTime
	def initialize
		@start_time = Time.now
	end

	def remaining_time finished, total
		@current_time = Time.now
		@remaining_time = ((@current_time-@start_time).to_f/finished)*(total-finished)
		redable_time = time_in_words(@remaining_time)
		puts "#{redable_time} remaining..."
	end

	def time_taken
		@end_time = Time.now
		redable_time = time_gap(@start_time, @end_time)
		puts "#{redable_time} taken."
	end


	private
	def time_gap(time1, time2)
		diff = (time2 - time1)
		time_in_words(diff)
	end

	def time_in_words(time_in_seconds)
		time_in_seconds = time_in_seconds.to_i
		
		days = (time_in_seconds/86400).floor
		time_in_seconds = time_in_seconds-(days*86400)

		hours = (time_in_seconds/3600).floor
		time_in_seconds = time_in_seconds-(hours*3600)
		
		minutes = (time_in_seconds/60).floor
		time_in_seconds = time_in_seconds-(minutes*60)

		seconds = time_in_seconds

		response = ""
		response += "#{days} days, " unless days == 0
		response += "#{hours} hours, " unless hours == 0
		response += "#{minutes} minutes, " unless minutes == 0
		response += "#{seconds} seconds" unless seconds == 0

		response
	end
end