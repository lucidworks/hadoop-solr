require 'r'

class Main
	def init
		@instance = TestDir.new()
		@instance.init
	end

	def match
		if @instance != nil
			@instance.match("1.1.1.1 abc")
		else
			puts "initial config not loaded"
		end
	end

end

puts "push to load read and load configuration"
char = STDIN.gets

instance = Main.new()
instance.init

puts "init process complete"

while true do
	puts "push to perform match"
	char = STDIN.getc
	instance.match
end
