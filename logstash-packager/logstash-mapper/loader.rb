require 'logstash-mapper/lw_wrapped'

require 'grok-pure'

class LWLoader
  # this function read, loads and prepare all necessary things to perform the match process
  def init(config_string_param, additional_patterns)

    pipeline_grok = LWWrapped::LWPipelineGrok.new(config_string_param)

    grok_object = LWWrapped::LWWrappedGrok.new(pipeline_grok.get_grok_object)
    grok_object.register

    if !additional_patterns.blank?
      # FIXME: from hdfs?
      grok_object.add_additional_patterns(additional_patterns)
    end

    filters = pipeline_grok.get_filters
    filters.each do |cFilter|
      cFilter.register if cFilter.class.to_s != "LogStash::Filters::Grok"
    end
    return filters
  end
end

instance = LWLoader.new()
instance.init($config_string_param, $additional_patterns)
