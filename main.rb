require 'fiber'
require_relative 'imports'
require_relative 'incomplete'

class App
  def initialize(ng, mnt, sab:, pvrs: [], log:)
    @sab = sab
    @pvrs = pvrs
    @pruners = [
      -> { Incomplete::Pruner.new(ng, mnt, sab: sab, log: log) },
      -> { Imports::Pruner.new(ng, mnt, log: log) },
    ]
  end

  def cmd_prune
    pruners = @pruners.map &:call
    pruners.dup.tap do |handlers|
      each_ev do |pvr, ev|
        handlers.select! do |pr|
          pr.add_ev pvr, ev
          pr.need_evs?
        end
        break if handlers.empty?
      end
    end
    pruners.each &:prune
  end

  private def each_ev
    hists = @pvrs.map do |pvr|
      evs = pvr.history_events
      Fiber.new { evs.each { |ev| Fiber.yield pvr, ev } }
    end
    hists.select! do |h|
      item = h.resume
      h.alive? or next false
      yield item
      true
    end until hists.empty?
  end
end

if $0 == __FILE__
  require 'metacli'
  config = Utils::Conf.new "config.yml"
  log = Utils::Log.new $stderr, level: :info
  log.level = :debug if ENV["DEBUG"] == "1"
  sab = Utils::SABnzbd.new URI(config[:sab]), log: log["sab"]
  pvrs = config[:pvrs].to_hash.map do |name, url|
    Utils::PVR.const_get(name).new(URI(url), log: log[name])
  end
  app = App.new config[:ng], config[:mnt],
    pvrs: pvrs,
    sab: sab,
    log: log
  MetaCLI.new(ARGV).run app
end
