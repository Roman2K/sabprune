module Incomplete

class SABnzbd
  def initialize(uri, log:)
    uri = Utils.merge_uri uri, output: "json"
    @http = Utils::SimpleHTTP.new uri, log: log
    @http.type_config.update json_in: false, json_out: true
  end

  def history
    @http.get([mode: "history"])
  end
end

end # Incomplete
