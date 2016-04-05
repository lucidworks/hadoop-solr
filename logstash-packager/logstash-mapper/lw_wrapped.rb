require 'delegate'
require 'logstash/pipeline'
require 'logstash/filters/grok'

# List of wrapper functions that overrides those from logstash
module LWWrapped
  class LWPipelineGrok < LogStash::Pipeline
    def get_filters
      return @filters
    end

    def get_grok_object
      @filters.each do |cFilter|
        return cFilter if cFilter.class.to_s == 'LogStash::Filters::Grok'
      end
      return nil
    end

    def plugin(plugin_type, name, *args)
      # skipping input and output types
      if (plugin_type == "input" or plugin_type == "output")
        return
      end

      args << {} if args.empty?
      klass = LogStash::Plugin.lookup(plugin_type, name)
      if( !klass.nil?)
        return klass.new(*args)
      else
        puts "skipping " + plugin_type + " - " + name
      end
    end
  end

   class LWEvent < LogStash::Event
     def get_data
       return @data
     end
   end

  class LWWrappedGrok < DelegateClass(LogStash::Filters::Grok)
    def initialize(wrapped_grok)
      super
    end

    def add_additional_patterns(additional_patterns)
      @additional_patterns = additional_patterns
      # adding additional patterns
      if(@additional_patterns.length > 0)
        array = @additional_patterns.split("\n")
        array.each do |line|
          name, pattern = line.chomp.split(/\s+/, 2)
          @patterns[field].add_pattern(name, pattern)
        end
      end
    end

    def get_config
      return @config
    end
  end
end
