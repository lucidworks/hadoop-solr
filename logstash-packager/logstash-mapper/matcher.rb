require 'grok-pure'
require 'logstash-mapper/lw_wrapped'

class LWMatcher
	def match(log, filters)
		hash = Hash.new
		hash["message"] = log
		event = LWWrapped::LWEvent.new(hash)
		if filters != nil && filters.length >0
			filters.each do |cFilter|
				cFilter.filter(event)
			end
			return event.get_data
		else
			return nil
		end
	end
end

instance = LWMatcher.new
instance.match($log , $filters)
