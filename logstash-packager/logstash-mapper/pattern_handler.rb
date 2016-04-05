require 'logstash-mapper/lw_wrapped'

class LWPatternHandler
  def get_patterns_dir(config_string)
    pipeline = LWWrapped::LWPipelineGrok.new(config_string)
    grok_object = LWWrapped::LWWrappedGrok.new(pipeline.get_grok_object)

    @patterns_dir_array = grok_object.get_config
    return @patterns_dir_array
  end
end

handler = LWPatternHandler.new
handler.get_patterns_dir($config_string_param)


